"""
🚀 بوت شراء NFT تلقائي - نسخة نهائية مستقرة
- يدعم 10 محافظ مع بوت تليجرام لكل محفظة
- اكتشاف سريع للمينتات عبر WebSocket + Mempool + Polling
- فحص تويتر كشرط أساسي للشراء
- تخزين مؤقت لتسريع العمليات
- إعادة محاولة ذكية للتعامل مع الأخطاء
- تحسينات لاستقرار الاتصال
"""

import asyncio
import json
import logging
import os
import time
from typing import Dict, Set, Optional, List
from datetime import datetime, timezone

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_wallet_lock,
    SEADROP_ADDRESS,
)
from twitter_checker import (
    get_twitter_username_from_opensea,
    is_twitter_rejected,
    mark_twitter_rejected,
)
from price_fetcher import get_eth_price_sync
from utils import FastCache, started_today_local, stage_has_ended

load_dotenv()

# ============================================
# 🔥 إعدادات التسجيل (Logging) - تقليل المزعج
# ============================================

# تقليل Logs المكتبات الخارجية
logging.getLogger("web3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

# ============================================
# 🔥 قراءة الإعدادات من .env
# ============================================

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]
TELEGRAM_BOT_TOKENS = [t.strip() for t in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if t.strip()]
TELEGRAM_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

if not (len(PRIVATE_KEYS) == len(WALLETS) == len(TELEGRAM_BOT_TOKENS) == len(TELEGRAM_CHAT_IDS)):
    raise ValueError("اعداد المفاتيح والمحافظ غير متطابقة!")

WALLETS_DATA = [
    {
        "wallet": WALLETS[i],
        "private_key": PRIVATE_KEYS[i],
        "bot_token": TELEGRAM_BOT_TOKENS[i],
        "chat_id": TELEGRAM_CHAT_IDS[i],
    }
    for i in range(len(WALLETS))
]

# ============================================
# 🔥 الإعدادات
# ============================================

ALCHEMY_API_KEY_ROBINHOOD = os.environ["ALCHEMY_API_KEY"]
ALCHEMY_API_KEY_ETHEREUM = os.environ["ALCHEMY_API_KEY_ETHEREUM"]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
FREE_PRICE_THRESHOLD_USD = 0.01

# 🔥 تحسينات WebSocket
WEBSOCKET_PING_INTERVAL = 30  # زيادة من 25
WEBSOCKET_PING_TIMEOUT = 20   # زيادة من 15
WEBSOCKET_OPEN_TIMEOUT = 15
WEBSOCKET_CLOSE_TIMEOUT = 15

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

# ============================================
# 🔥 الحالة العامة
# ============================================

successful_mints: Dict[str, Set[str]] = {}
watchlist: Dict[str, dict] = {}
in_flight: Set[str] = set()
rejected_cooldown: Dict[str, float] = {}
REJECTION_COOLDOWN = 120

# تخزين مؤقت مع TTL أطول
collection_cache = FastCache(120)  # دقيقتين
mempool_slug_cache = FastCache(3600)  # ساعة

# قائمة الانتظار للرسائل
send_queue: asyncio.Queue = asyncio.Queue()

# تتبع معدل الطلبات
_api_request_times = []
API_RATE_LIMIT = 10
API_RATE_WINDOW = 2

# مجموعة slugs التي تم رؤيتها (لـ Polling)
seen_slugs: Set[str] = set()

# ============================================
# 🔥 دوال مساعدة مع إعادة محاولة
# ============================================

def is_in_cooldown(slug: str) -> bool:
    ts = rejected_cooldown.get(slug)
    if ts is None:
        return False
    if time.time() - ts >= REJECTION_COOLDOWN:
        rejected_cooldown.pop(slug, None)
        return False
    return True

def mark_rejected(slug: str):
    rejected_cooldown[slug] = time.time()

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

async def rate_limit():
    """تقييد معدل الطلبات لتجنب الـ Rate Limiting"""
    global _api_request_times
    now = time.time()
    _api_request_times = [t for t in _api_request_times if now - t < API_RATE_WINDOW]
    if len(_api_request_times) >= API_RATE_LIMIT:
        wait_time = API_RATE_WINDOW - (now - _api_request_times[0])
        if wait_time > 0:
            await asyncio.sleep(wait_time + 0.1)
    _api_request_times.append(now)

async def fetch_drop_detail_with_retry(slug: str, max_retries: int = 3) -> tuple[bool, dict | None]:
    """جلب تفاصيل المينت مع إعادة محاولة وتأخير ذكي"""
    
    # التحقق من الكاش أولاً
    cached = collection_cache.get(slug)
    if cached is not None:
        return cached
    
    for attempt in range(max_retries):
        try:
            await rate_limit()
            
            resp = await asyncio.to_thread(
                requests.get,
                f"{DROPS_API_BASE}/{slug}",
                headers={"x-api-key": OPENSEA_API_KEY},
                timeout=10,
            )
            
            if resp.status_code == 200:
                result = (True, resp.json())
                collection_cache.set(slug, result)
                return result
            elif resp.status_code == 404:
                return (False, None)
            else:
                log.warning(f"[Drops API] HTTP {resp.status_code} لـ '{slug}'")
                
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 1.5
                log.debug(f"[Drops API] محاولة {attempt + 1} فشلت، إعادة بعد {wait_time}s")
                await asyncio.sleep(wait_time)
            else:
                log.debug(f"[Drops API] فشل بعد {max_retries} محاولات: {e}")
    
    return (False, None)

async def get_slug_from_contract(contract_address: str, chain_key: str) -> Optional[str]:
    """تحويل عنوان العقد إلى slug مع تخزين مؤقت"""
    cache_key = f"{chain_key}:{contract_address.lower()}"
    cached = mempool_slug_cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        await rate_limit()
        
        url = "https://api.opensea.io/api/v2/collections"
        params = {"asset_contract_address": contract_address, "limit": 1}
        headers = {"x-api-key": OPENSEA_API_KEY}
        
        resp = await asyncio.to_thread(requests.get, url, params=params, headers=headers, timeout=10)
        if resp.status_code == 200:
            collections = resp.json().get("collections", [])
            if collections:
                slug = collections[0].get("slug")
                if slug:
                    mempool_slug_cache.set(cache_key, slug)
                    return slug
    except Exception:
        pass
    return None

# ============================================
# 🔥 دوال التليجرام
# ============================================

def enqueue_message(bot_token: str, chat_id: str, text: str):
    send_queue.put_nowait({"bot_token": bot_token, "chat_id": chat_id, "text": text})

def broadcast_message(text: str):
    for w in WALLETS_DATA:
        enqueue_message(w["bot_token"], w["chat_id"], text)

async def telegram_sender():
    while True:
        msg = await send_queue.get()
        try:
            api = f"https://api.telegram.org/bot{msg['bot_token']}"
            await asyncio.to_thread(
                requests.post,
                f"{api}/sendMessage",
                data={"chat_id": msg["chat_id"], "text": msg["text"], "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception as e:
            log.error(f"خطأ تليجرام: {e}")
        send_queue.task_done()
        await asyncio.sleep(0.1)

def build_success_msg(detail: dict, result: dict, chain_key: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    chain_label = "Robinhood" if chain_key == "robinhood" else "Ethereum"
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    return (
        f"✅ <b>تم الشراء!</b> ({chain_label})\n"
        f"المحفظة: <code>{w_short}</code>\n"
        f"المجموعة: <b>{name}</b>\n"
        f"الكمية: {result['quantity']}\n"
        f"رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
        f"المعاملة: {result['tx_hash'][:16]}...\n"
        f"🔗 {url}"
    )

# ============================================
# 🔥 محرك الشراء
# ============================================

async def purchase_task(w3, item, slug, contract_address, price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd):
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
            
            msg = build_success_msg(item.get("detail", {}), res, item.get("chain_key", ""))
            enqueue_message(bot_token, chat_id, msg)

        return res

async def try_buy_now(slug: str, chain_key: str, detail: dict) -> list[dict] | None:
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
        return [{"success": False, "reason": "no_contract"}]

    w3 = W3_INSTANCES[chain_key]
    eth_price_usd = get_eth_price_sync()

    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

    if not is_free_or_negligible(price_wei, eth_price_usd):
        return None

    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]

    already_bought = successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in already_bought]

    if not pending_items:
        return [{"success": False, "reason": "all_done"}]

    for item in pending_items:
        item["detail"] = detail
        item["chain_key"] = chain_key

    tasks = [
        purchase_task(
            w3, item, slug, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd
        )
        for item in pending_items
    ]

    return await asyncio.gather(*tasks)

# ============================================
# 🔥 تقييم المينت
# ============================================

async def evaluate_new_mint(slug: str, chain_key: str):
    """تقييم المينت الجديد - مع فحص تويتر"""
    
    if (
        len(successful_mints.get(slug, set())) >= len(WALLETS_DATA)
        or slug in watchlist
        or slug in in_flight
        or is_in_cooldown(slug)
        or is_twitter_rejected(slug)
    ):
        return

    in_flight.add(slug)
    try:
        found, detail = await fetch_drop_detail_with_retry(slug)
        if not found or not detail or not detail.get("is_minting"):
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return

        contract_address = detail.get("contract_address")
        if not contract_address:
            return

        w3 = W3_INSTANCES[chain_key]
        eth_price_usd = get_eth_price_sync()

        # جلب السعر وتويتر بالتوازي
        price_task = asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
        twitter_task = asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
        
        onchain_price, twitter_username = await asyncio.gather(
            price_task, twitter_task, return_exceptions=True
        )
        
        onchain_price = onchain_price if not isinstance(onchain_price, Exception) else None
        twitter_username = twitter_username if not isinstance(twitter_username, Exception) else None
        
        price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
        
        # فحص السعر
        if not is_free_or_negligible(price_wei, eth_price_usd):
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            return

        # 🔥 فحص تويتر (شرط أساسي)
        if not twitter_username:
            log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X مربوط.")
            mark_twitter_rejected(slug)
            mark_rejected(slug)
            return

        log.info(f"✅ '{slug}': يوجد تويتر (@{twitter_username}) - شراء!")

        results = await try_buy_now(slug, chain_key, detail)

        if results is None:
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            return

        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}

    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)

# ============================================
# 🔥 مراقبة المينتات
# ============================================

async def watch_loop():
    while True:
        await asyncio.sleep(15)
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
                found, fresh_detail = await fetch_drop_detail_with_retry(slug)

                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    watchlist.pop(slug, None)
                    continue

                stage = fresh_detail.get("active_stage")
                if not stage or stage_has_ended(stage):
                    watchlist.pop(slug, None)
                    continue

                results = await try_buy_now(slug, chain_key, fresh_detail)

                if results is None:
                    watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}
                    continue

                if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                    watchlist.pop(slug, None)
                else:
                    watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}

            except Exception as e:
                log.error(f"خطأ بمراقبة '{slug}': {e}")
            finally:
                in_flight.discard(slug)

# ============================================
# 🔥 اكتشاف Mempool
# ============================================

MINT_PUBLIC_SIGNATURE = "0x8c7a63ae"

async def listen_mempool(chain_key: str):
    """الاستماع إلى Mempool لاكتشاف أسرع"""
    ws_url = CHAIN_CONFIGS[chain_key]["ws_rpc_url"]
    reconnect_delay = 2
    
    while True:
        try:
            async with websockets.connect(
                ws_url,
                ping_interval=WEBSOCKET_PING_INTERVAL,
                ping_timeout=WEBSOCKET_PING_TIMEOUT,
                close_timeout=WEBSOCKET_CLOSE_TIMEOUT,
            ) as ws:
                subscribe = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["alchemy_pendingTransactions"]
                }
                await ws.send(json.dumps(subscribe))
                log.info(f"✅ بدء Mempool لـ {chain_key}")
                reconnect_delay = 2
                
                while True:
                    try:
                        response = await ws.recv()
                        data = json.loads(response)
                        
                        if "params" not in data:
                            continue
                        
                        tx = data["params"]["result"]
                        tx_to = tx.get("to", "").lower()
                        if tx_to != SEADROP_ADDRESS.lower():
                            continue
                        
                        input_data = tx.get("input", "")
                        if not input_data.startswith(MINT_PUBLIC_SIGNATURE):
                            continue
                        
                        if len(input_data) >= 74:
                            contract_address = "0x" + input_data[34:74]
                            slug = await get_slug_from_contract(contract_address, chain_key)
                            if slug:
                                log.info(f"⚡ Mempool: {slug}")
                                asyncio.create_task(evaluate_new_mint(slug, chain_key))
                                
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        log.debug(f"خطأ Mempool: {e}")
                        
        except Exception as e:
            log.warning(f"انقطع Mempool ({e}). إعادة بعد {reconnect_delay}s")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)

# ============================================
# 🔥 OpenSea Polling - بديل احتياطي
# ============================================

async def poll_opensea():
    """جلب المينتات الجديدة عبر API كبديل للـ WebSocket"""
    global seen_slugs
    
    while True:
        try:
            url = "https://api.opensea.io/api/v2/collections"
            params = {
                "limit": 20,
                "order_by": "created_date",
                "order_direction": "desc"
            }
            headers = {"x-api-key": OPENSEA_API_KEY}
            
            resp = await asyncio.to_thread(
                requests.get, url, params=params, headers=headers, timeout=10
            )
            
            if resp.status_code == 200:
                data = resp.json()
                collections = data.get("collections", [])
                
                for collection in collections:
                    slug = collection.get("slug")
                    if not slug or slug in seen_slugs:
                        continue
                    
                    # التحقق إذا كانت مجموعة جديدة (منذ آخر دقيقتين)
                    created_date = collection.get("created_date")
                    if created_date:
                        try:
                            created = datetime.fromisoformat(created_date.replace("Z", "+00:00"))
                            if (datetime.now(timezone.utc) - created).seconds < 120:
                                seen_slugs.add(slug)
                                log.info(f"🔄 Polling: {slug}")
                                asyncio.create_task(evaluate_new_mint(slug, "ethereum"))
                        except:
                            pass
                
                # تنظيف القائمة (احتفاظ بآخر 500 فقط)
                if len(seen_slugs) > 500:
                    seen_slugs = set(list(seen_slugs)[-250:])
                    
        except Exception as e:
            log.debug(f"خطأ في Polling: {e}")
        
        await asyncio.sleep(10)  # كل 10 ثواني

# ============================================
# 🔥 OpenSea Stream - نسخة محسّنة ومستقرة
# ============================================

async def listen_opensea():
    """الاستماع إلى OpenSea Stream مع إعادة اتصال تلقائي"""
    msg_ref = 0
    reconnect_delay = 2
    
    while True:
        try:
            async with websockets.connect(
                STREAM_URL,
                ping_interval=WEBSOCKET_PING_INTERVAL,
                ping_timeout=WEBSOCKET_PING_TIMEOUT,
                open_timeout=WEBSOCKET_OPEN_TIMEOUT,
                close_timeout=WEBSOCKET_CLOSE_TIMEOUT,
                max_size=2**23,
                user_agent_header="Mozilla/5.0",
                extra_headers={
                    "Origin": "https://opensea.io",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            ) as ws:
                log.info(f"🚀 متصل بـ OpenSea Stream - {len(WALLETS_DATA)} محافظ")
                reconnect_delay = 2
                
                # إرسال طلب الانضمام
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()
                heartbeat_interval = 25

                while True:
                    try:
                        # إرسال heartbeat إذا لزم الأمر
                        if time.time() - last_heartbeat > heartbeat_interval:
                            try:
                                hb_ref = str(msg_ref)
                                await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                                msg_ref += 1
                                last_heartbeat = time.time()
                            except Exception:
                                break

                        # استقبال الرسائل
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=35)
                        except asyncio.TimeoutError:
                            continue
                        
                        # معالجة الرسالة
                        try:
                            parsed = json.loads(raw)
                            if not (isinstance(parsed, list) and len(parsed) == 5):
                                continue
                            
                            _, _, _, event_name, payload_wrapper = parsed
                            if event_name != "item_transferred":
                                continue
                            
                            payload = payload_wrapper.get("payload", {})
                            item = payload.get("item", {})
                            chain_name = item.get("chain", {}).get("name", "")
                            chain_key = STREAM_NAME_TO_CHAIN_KEY.get(chain_name)
                            
                            if not chain_key:
                                continue
                            
                            from_addr = payload.get("from_account", {}).get("address", "").lower()
                            if from_addr != ZERO_ADDRESS:
                                continue
                            
                            slug = payload.get("collection", {}).get("slug", "")
                            if slug:
                                asyncio.create_task(evaluate_new_mint(slug, chain_key))
                                
                        except json.JSONDecodeError:
                            continue
                        except Exception:
                            continue

                    except websockets.ConnectionClosed:
                        break
                    except Exception:
                        break

        except Exception as e:
            log.warning(f"انقطع Stream. إعادة بعد {reconnect_delay}s")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)

# ============================================
# 🔥 تشغيل البوت
# ============================================

async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        broadcast_message("🔴 البوت في وضع الإيقاف")
        await telegram_sender()
        return

    broadcast_message("✅ تم تشغيل البوت بنجاح!")
    
    tasks = [
        listen_opensea(),      # المصدر الرئيسي
        poll_opensea(),        # 🔥 بديل احتياطي
        watch_loop(),
        telegram_sender(),
    ]
    
    # إضافة Mempool لكل شبكة
    for chain_key in CHAIN_CONFIGS:
        tasks.append(listen_mempool(chain_key))
    
    await asyncio.gather(*tasks)

def main():
    max_retries = 10
    retries = 0
    backoff = 3
    
    while retries < max_retries:
        try:
            asyncio.run(run())
            break
        except KeyboardInterrupt:
            log.info("تم الإيقاف")
            break
        except Exception as e:
            retries += 1
            log.critical(f"توقف (محاولة {retries}/{max_retries}): {e}")
            time.sleep(backoff)
            backoff = min(backoff * 1.5, 60)
    else:
        log.critical("❌ فشل البوت بعد عدة محاولات")

if __name__ == "__main__":
    main()
