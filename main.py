"""
النظام الكامل — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها:
  - يكتشف مينتات اليوم على Robinhood + Ethereum
  - يشتري لجميع المحافظ المعرفة بالتوازي (Parallel Execution)
  - يرسل إشعار الشراء أو التحديث لكل محفظة على بوت التيليجرام الخاص بها
  
🔄 محسّن للسرعة مع:
  - تخزين مؤقت ذكي
  - معالجة متوازية
  - اكتشاف سريع عبر Mempool
  - الحفاظ على فحص تويتر كشرط أساسي للشراء
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Set, List

import requests
import websockets
from dotenv import load_dotenv
from web3 import Web3

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_wallet_lock,
    SEADROP_ADDRESS,
)
from twitter_checker import get_twitter_username_from_opensea, get_twitter_username_cached

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
ALCHEMY_API_KEY = os.environ.get("ALCHEMY_API_KEY_ETHEREUM", os.environ.get("ALCHEMY_API_KEY"))
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

# تفكيك المحافظ والمفاتيح وإعدادات التيليجرام
PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]
TELEGRAM_BOT_TOKENS = [t.strip() for t in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if t.strip()]
TELEGRAM_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

if not (len(PRIVATE_KEYS) == len(WALLETS) == len(TELEGRAM_BOT_TOKENS) == len(TELEGRAM_CHAT_IDS)):
    raise ValueError("أعداد المفاتيح، المحافظ، توكنات البوتات، و Chat IDs غير متطابقة في ملف .env!")

# إنشاء هيكلية المحافظ
WALLETS_DATA = []
for i in range(len(WALLETS)):
    WALLETS_DATA.append({
        "wallet": WALLETS[i],
        "private_key": PRIVATE_KEYS[i],
        "bot_token": TELEGRAM_BOT_TOKENS[i],
        "chat_id": TELEGRAM_CHAT_IDS[i],
    })

ALCHEMY_API_KEY_ROBINHOOD = os.environ["ALCHEMY_API_KEY"]
ALCHEMY_API_KEY_ETHEREUM = os.environ["ALCHEMY_API_KEY_ETHEREUM"]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

CHAIN_CONFIGS = {
    "robinhood": {
        "stream_chain_name": "robinhood",
        "rpc_url": f"https://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ROBINHOOD}",
        "ws_rpc_url": f"wss://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ROBINHOOD}",
        "max_gas_fee_usd": 0.05,
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ETHEREUM}",
        "ws_rpc_url": f"wss://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ETHEREUM}",
        "max_gas_fee_usd": 0.50,
    },
}

W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# تتبع المحافظ التي اشترت بنجاح: slug -> set(wallet_address)
successful_mints: dict[str, set[str]] = {}
watchlist: dict[str, dict] = {}
in_flight: set[str] = set()

# تبريد مؤقت للمجموعات التي رُفضت
REJECTION_COOLDOWN_SECONDS = 120
rejected_cooldown: dict[str, float] = {}

# 🔥 تحسين: تخزين مؤقت سريع للبيانات
class FastCache:
    """تخزين مؤقت سريع مع انتهاء صلاحية"""
    def __init__(self, default_ttl: int = 60):
        self._cache: Dict[str, tuple[float, Any]] = {}
        self.default_ttl = default_ttl
    
    def get(self, key: str) -> Optional[Any]:
        if key in self._cache:
            timestamp, value = self._cache[key]
            if time.time() - timestamp < self.default_ttl:
                return value
            del self._cache[key]
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        self._cache[key] = (time.time(), value)
    
    def clear(self):
        self._cache.clear()

# كاشات سريعة
collection_cache = FastCache(60)  # دقيقة واحدة فقط - لتجنب البيانات القديمة
slug_from_contract_cache = FastCache(3600)  # ساعة كاملة للـ mapping

_eth_price_cache = {"value": None, "ts": 0}

# 🔥 تتبع slugs التي تم رفضها بسبب تويتر لتجنب إعادة الفحص
twitter_rejected_cache = FastCache(600)  # 10 دقائق

def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < 30):  # 🔥 30 ثانية فقط
        return _eth_price_cache["value"]
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=5,
        )
        price = resp.json()["ethereum"]["usd"]
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except Exception as e:
        log.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0


def is_in_cooldown(slug: str) -> bool:
    ts = rejected_cooldown.get(slug)
    if ts is None:
        return False
    if time.time() - ts >= REJECTION_COOLDOWN_SECONDS:
        rejected_cooldown.pop(slug, None)
        return False
    return True


def mark_rejected(slug: str):
    rejected_cooldown[slug] = time.time()


# 🔥 تحسين: تجميع طلبات تفاصيل المينتات
pending_drop_requests: Dict[str, asyncio.Future] = {}

async def fetch_drop_detail_fast(slug: str) -> tuple[bool, dict | None]:
    """جلب تفاصيل المينت مع تجميع الطلبات المتكررة"""
    # التحقق من الكاش أولاً
    cached = collection_cache.get(slug)
    if cached is not None:
        return cached
    
    # التحقق من الطلبات المعلقة لنفس الـ slug
    if slug in pending_drop_requests:
        try:
            return await pending_drop_requests[slug]
        except Exception:
            pass
    
    # إنشاء طلب جديد
    future = asyncio.Future()
    pending_drop_requests[slug] = future
    
    try:
        result = await asyncio.to_thread(fetch_drop_detail_sync, slug)
        collection_cache.set(slug, result)
        future.set_result(result)
        return result
    except Exception as e:
        future.set_exception(e)
        return False, None
    finally:
        pending_drop_requests.pop(slug, None)


def fetch_drop_detail_sync(slug: str) -> tuple[bool, dict | None]:
    """الدالة المتزامنة لجلب التفاصيل"""
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=5,
        )
        if resp.status_code == 200:
            return True, resp.json()
        if resp.status_code == 404:
            return False, None
        return None, None
    except Exception as e:
        log.warning(f"[Drops API] خطأ: {e}")
        return None, None


def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def started_today_local(stage: dict) -> bool:
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()


def stage_has_ended(stage: dict) -> bool:
    end = parse_iso(stage.get("end_time", ""))
    if not end:
        return False
    return datetime.now(timezone.utc) > end


def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD


# ---------------------------------------------------------------------------
# 🚀 اكتشاف المينتات عبر Mempool (اكتشاف أسرع)
# ---------------------------------------------------------------------------

MINT_PUBLIC_SIGNATURE = "0x8c7a63ae"  # أول 4 بايت من mintPublic

async def listen_mempool(chain_key: str):
    """الاستماع إلى الـ mempool لاكتشاف المينتات قبل وصولها للـ blockchain"""
    ws_url = CHAIN_CONFIGS[chain_key]["ws_rpc_url"]
    
    while True:
        try:
            async with websockets.connect(ws_url) as ws:
                # الاشتراك في المعاملات المعلقة
                subscribe_msg = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["alchemy_pendingTransactions"]
                }
                await ws.send(json.dumps(subscribe_msg))
                log.info(f"✅ بدء الاستماع إلى Mempool لـ {chain_key}")
                
                while True:
                    try:
                        response = await ws.recv()
                        data = json.loads(response)
                        
                        if "params" not in data:
                            continue
                        
                        tx = data["params"]["result"]
                        
                        # فحص إذا كانت المعاملة تستهدف SeaDrop
                        tx_to = tx.get("to", "").lower()
                        if tx_to != SEADROP_ADDRESS.lower():
                            continue
                        
                        # استخراج input data
                        input_data = tx.get("input", "")
                        if not input_data.startswith(MINT_PUBLIC_SIGNATURE):
                            continue
                        
                        # استخراج عنوان العقد من input
                        # format: 0x + 8 (signature) + 32 (nftContract) + ...
                        if len(input_data) >= 74:
                            contract_address = "0x" + input_data[34:74]
                            
                            # محاولة العثور على الـ slug من عنوان العقد
                            slug = await get_slug_from_contract_fast(contract_address, chain_key)
                            if slug:
                                log.info(f"⚡ اكتشف مينت في Mempool: {slug}")
                                # 🔥 تنفيذ الشراء فوراً (مع الحفاظ على فحص تويتر)
                                asyncio.create_task(evaluate_new_mint(slug, chain_key))
                                
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        log.error(f"خطأ في معالجة رسالة Mempool: {e}")
                        
        except Exception as e:
            log.warning(f"انقطع اتصال Mempool ({e}). إعادة المحاولة...")
            await asyncio.sleep(2)


# 🔥 تحويل عنوان العقد إلى slug (مع تخزين مؤقت)
async def get_slug_from_contract_fast(contract_address: str, chain_key: str) -> Optional[str]:
    """الحصول على slug من عنوان العقد مع تخزين مؤقت"""
    cache_key = f"{chain_key}:{contract_address.lower()}"
    cached = slug_from_contract_cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        # محاولة العثور على المجموعة عن طريق OpenSea API
        url = f"https://api.opensea.io/api/v2/collections"
        params = {
            "asset_contract_address": contract_address,
            "limit": 1
        }
        headers = {"x-api-key": OPENSEA_API_KEY}
        
        resp = await asyncio.to_thread(
            requests.get, url, params=params, headers=headers, timeout=5
        )
        
        if resp.status_code == 200:
            data = resp.json()
            collections = data.get("collections", [])
            if collections:
                slug = collections[0].get("slug")
                if slug:
                    slug_from_contract_cache.set(cache_key, slug)
                    return slug
    except Exception as e:
        log.debug(f"خطأ في جلب slug من contract: {e}")
    
    return None


# ---------------------------------------------------------------------------
# إدارة رسائل التيليجرام
# ---------------------------------------------------------------------------

send_queue: "asyncio.Queue[dict]" = asyncio.Queue()


def enqueue_message(bot_token: str, chat_id: str, text: str):
    """إضافة إشعار جديد مع تحديد البوت والمستلم"""
    send_queue.put_nowait({
        "bot_token": bot_token,
        "chat_id": chat_id,
        "text": text
    })


def broadcast_message(text: str):
    """إرسال إشعار عام لجميع البوتات"""
    for w in WALLETS_DATA:
        enqueue_message(w["bot_token"], w["chat_id"], text)


async def telegram_sender():
    while True:
        msg = await send_queue.get()
        try:
            telegram_api = f"https://api.telegram.org/bot{msg['bot_token']}"
            await asyncio.to_thread(
                requests.post,
                f"{telegram_api}/sendMessage",
                data={"chat_id": msg["chat_id"], "text": msg["text"], "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception as e:
            log.error(f"خطأ إرسال تليجرام: {e}")
        send_queue.task_done()
        await asyncio.sleep(0.1)


def build_single_wallet_success_msg(detail: dict, result: dict, chain_key: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    chain_label = "Robinhood Chain" if chain_key == "robinhood" else "Ethereum Mainnet"
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    return (
        f"✅ <b>تم الشراء بنجاح لمحافظتك!</b> ({chain_label})\n\n"
        f"المحفظة: <code>{w_short}</code>\n"
        f"المجموعة: <b>{name}</b>\n"
        f"الكمية: {result['quantity']}\n"
        f"رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
        f"المعاملة: {result['tx_hash']}\n"
        f"🔗 {url}"
    )


def build_watching_message(detail: dict, reason: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"👀 <b>تحت المراقبة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}"


def build_gaveup_message(detail: dict, reason: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"❌ <b>انتهت الفرصة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}"


# ---------------------------------------------------------------------------
# الشراء المتوازي
# ---------------------------------------------------------------------------

async def purchase_task_for_wallet(
    w3, item, slug, contract_address, price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd
):
    wallet_addr = item["wallet"]
    pk = item["private_key"]
    bot_token = item["bot_token"]
    chat_id = item["chat_id"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        if wallet_addr in successful_mints.get(slug, set()):
            return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

        res = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            w3, pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd,
        )

        if res.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            
            msg = build_single_wallet_success_msg(item.get("current_detail", {}), res, item.get("chain_key", ""))
            enqueue_message(bot_token, chat_id, msg)

        return res


async def try_buy_now_multi_wallet(slug: str, chain_key: str, detail: dict) -> list[dict] | None:
    stage = detail.get("active_stage")
    if not stage:
        return None

    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    if remaining <= 0:
        return [{"success": False, "reason": "sold_out"}]

    contract_address = detail.get("contract_address")
    if not contract_address:
        return [{"success": False, "reason": "no_contract_address"}]

    w3 = W3_INSTANCES[chain_key]
    eth_price_usd = get_eth_price_usd()

    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

    if not is_free_or_negligible(price_wei, eth_price_usd):
        return None

    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]

    already_bought_wallets = successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in already_bought_wallets]

    if not pending_items:
        return [{"success": False, "reason": "all_wallets_completed"}]

    for item in pending_items:
        item["current_detail"] = detail
        item["chain_key"] = chain_key

    tasks = [
        purchase_task_for_wallet(
            w3, item, slug, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd
        )
        for item in pending_items
    ]

    results = await asyncio.gather(*tasks)
    return list(results)


# ---------------------------------------------------------------------------
# 🔥 تقييم المينت الجديد - محسّن مع الحفاظ على فحص تويتر
# ---------------------------------------------------------------------------

async def evaluate_new_mint(slug: str, chain_key: str):
    """تقييم المينت الجديد مع الحفاظ على فحص تويتر كشرط أساسي"""
    if (
        len(successful_mints.get(slug, set())) >= len(WALLETS_DATA)
        or slug in watchlist
        or slug in in_flight
        or is_in_cooldown(slug)
    ):
        return

    # 🔥 التحقق السريع من تويتر (إذا تم رفضه مؤخراً)
    if twitter_rejected_cache.get(slug) is not None:
        log.debug(f"⏭️ {slug} مرفوض من تويتر مؤخراً، تخطي")
        return

    in_flight.add(slug)
    try:
        # 1. جلب تفاصيل المينت مع التخزين المؤقت
        found, detail = await fetch_drop_detail_fast(slug)
        if not found or not detail or not detail.get("is_minting"):
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return

        contract_address = detail.get("contract_address")
        if not contract_address:
            return

        w3 = W3_INSTANCES[chain_key]
        eth_price_usd = get_eth_price_usd()

        # 🔥 2. تنفيذ الطلبات بالتوازي: السعر + تويتر (كلاهما ضروري)
        price_task = asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
        twitter_task = asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
        
        # تنفيذ المهمتين بالتوازي
        onchain_price_result, twitter_username_result = await asyncio.gather(
            price_task, 
            twitter_task,
            return_exceptions=True
        )
        
        onchain_price = onchain_price_result if not isinstance(onchain_price_result, Exception) else None
        twitter_username = twitter_username_result if not isinstance(twitter_username_result, Exception) else None
        
        # 3. استخدام السعر المخبأ إذا فشل
        price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
        
        # 4. 🔥 فحص السعر أولاً (سريع)
        if not is_free_or_negligible(price_wei, eth_price_usd):
            # نضيف للمراقبة
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            broadcast_message(build_watching_message(detail, "السعر الحالي مدفوع — تحت المراقبة."))
            return

        # 5. 🔥 فحص تويتر (شرط أساسي للشراء)
        if not twitter_username:
            log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X مربوط.")
            twitter_rejected_cache.set(slug, True, ttl=600)  # تخزين الرفض لمدة 10 دقائق
            mark_rejected(slug)
            return

        log.info(f"✅ '{slug}': يوجد حساب X مربوط (@{twitter_username}) — المتابعة للشراء.")

        # 6. تنفيذ الشراء
        results = await try_buy_now_multi_wallet(slug, chain_key, detail)

        if results is None:
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            broadcast_message(build_watching_message(detail, "السعر الحالي مدفوع — تحت المراقبة."))
            return

        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}

    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)


async def watch_loop():
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        if not watchlist:
            continue

        for slug in list(watchlist.keys()):
            if slug in in_flight or len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                watchlist.pop(slug, None)
                continue

            entry = watchlist.get(slug)
            if not entry:
                continue

            in_flight.add(slug)
            try:
                chain_key = entry["chain_key"]
                found, fresh_detail = await fetch_drop_detail_fast(slug)

                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    watchlist.pop(slug, None)
                    broadcast_message(build_gaveup_message(entry["detail"], "المينت لم يعد نشطًا."))
                    continue

                stage = fresh_detail.get("active_stage")
                if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
                    watchlist.pop(slug, None)
                    broadcast_message(build_gaveup_message(fresh_detail, "انتهت المرحلة."))
                    continue

                results = await try_buy_now_multi_wallet(slug, chain_key, fresh_detail)

                if results is None:
                    watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}
                    continue

                if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                    watchlist.pop(slug, None)
                else:
                    watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}

            except Exception as e:
                log.error(f"خطأ بدورة مراقبة '{slug}': {e}")
            finally:
                in_flight.discard(slug)


# ---------------------------------------------------------------------------
# 🔥 OpenSea Stream - محسن للاستجابة السريعة
# ---------------------------------------------------------------------------

async def listen_opensea():
    msg_ref = 0
    while True:
        try:
            async with websockets.connect(
                STREAM_URL, 
                ping_interval=10,
                ping_timeout=5,
                open_timeout=5,
                close_timeout=5,
                max_size=2**23,
            ) as ws:
                log.info(f"🚀 متصل بـ OpenSea Stream — يراقب لـ {len(WALLETS_DATA)} محافظ.")
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()

                while True:
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()

                    # استقبال سريع بدون timeout
                    raw = await ws.recv()
                    
                    try:
                        parsed = json.loads(raw)
                        if not (isinstance(parsed, list) and len(parsed) == 5):
                            continue
                        
                        _, _, _, event_name, payload_wrapper = parsed
                        if event_name != "item_transferred":
                            continue
                        
                        payload = (payload_wrapper or {}).get("payload") or {}
                        item = payload.get("item", {}) or {}
                        stream_chain_name = (item.get("chain", {}) or {}).get("name", "")
                        
                        chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
                        if chain_key is None:
                            continue
                        
                        from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                        if from_address != ZERO_ADDRESS:
                            continue
                        
                        slug = (payload.get("collection", {}) or {}).get("slug", "")
                        if not slug:
                            continue
                        
                        # إنشاء المهمة فوراً
                        asyncio.create_task(evaluate_new_mint(slug, chain_key))
                        
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        log.error(f"خطأ في معالجة الرسالة: {e}")
                        continue

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"⚠️ انقطع الاتصال ({e}). إعادة الاتصال...")
            await asyncio.sleep(1)
        except Exception as e:
            log.error(f"خطأ غير متوقع: {e}.")
            await asyncio.sleep(2)


# ---------------------------------------------------------------------------
# تشغيل النظام
# ---------------------------------------------------------------------------

async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        broadcast_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false).")
        await telegram_sender()
        return

    broadcast_message(f"✅ تم تشغيل المحفظة الخاصة بك بنجاح وربطها بهذا البوت!")
    
    # 🔥 تشغيل جميع المصادر بالتوازي
    tasks = [
        listen_opensea(),
        watch_loop(),
        telegram_sender(),
    ]
    
    # 🔥 إضافة Mempool Listener لكل شبكة
    for chain_key in CHAIN_CONFIGS.keys():
        tasks.append(listen_mempool(chain_key))
    
    await asyncio.gather(*tasks)


def main():
    backoff = 2
    max_retries = 10
    retries = 0
    while retries < max_retries:
        try:
            asyncio.run(run())
            break
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدويًا.")
            break
        except Exception as e:
            retries += 1
            log.critical(f"توقف غير متوقع (محاولة {retries}/{max_retries}): {e}.")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        else:
            break
    else:
        log.critical("❌ فشل النظام بعد عدة محاولات. إيقاف.")


if __name__ == "__main__":
    main()
