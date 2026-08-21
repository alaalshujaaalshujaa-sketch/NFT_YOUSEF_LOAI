"""
النظام الكامل — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها:
  - يكتشف مينتات اليوم على Robinhood + Ethereum
  - يشتري لجميع المحافظ المعرفة بالتوازي (Parallel Execution)
  - يرسل إشعار الشراء فقط
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_wallet_lock,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
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
RECV_TIMEOUT = 5
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
        "max_gas_fee_usd": 0.05,
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ETHEREUM}",
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

# ==================== تتبع المينتات المدفوعة ====================

paid_mints_tracking: dict[str, dict] = {}

# ==================== التخزين المؤقت ====================

_twitter_cache = {}
TWITTER_CACHE_DURATION = 300

_drop_cache = {}
DROP_CACHE_DURATION = 30

event_buffer = []
last_flush = time.time()
BUFFER_FLUSH_INTERVAL = 3
MAX_BUFFER_SIZE = 5

# ==================== دوال التخزين المؤقت ====================

def get_cached_twitter(slug: str):
    if slug in _twitter_cache:
        username, timestamp = _twitter_cache[slug]
        if time.time() - timestamp < TWITTER_CACHE_DURATION:
            return username
    return None

def set_cached_twitter(slug: str, username):
    _twitter_cache[slug] = (username, time.time())

def get_cached_drop(slug: str):
    if slug in _drop_cache:
        detail, timestamp = _drop_cache[slug]
        if time.time() - timestamp < DROP_CACHE_DURATION:
            return detail
    return None

def set_cached_drop(slug: str, detail):
    _drop_cache[slug] = (detail, time.time())

# ==================== الوظائف الأساسية ====================

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

_eth_price_cache = {"value": None, "ts": 0}

def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < 300):
        return _eth_price_cache["value"]
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8,
        )
        price = resp.json()["ethereum"]["usd"]
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except Exception as e:
        log.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0

def fetch_drop_detail(slug: str):
    cached = get_cached_drop(slug)
    if cached is not None:
        return True, cached
    
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            detail = resp.json()
            set_cached_drop(slug, detail)
            return True, detail
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
# إدارة رسائل التيليجرام
# ---------------------------------------------------------------------------

send_queue: "asyncio.Queue[dict]" = asyncio.Queue()

def enqueue_message(bot_token: str, chat_id: str, text: str):
    send_queue.put_nowait({
        "bot_token": bot_token,
        "chat_id": chat_id,
        "text": text
    })

def broadcast_message(text: str):
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
            log.error(f"خطأ إرسال تليجرام للبوت ({msg['bot_token'][:10]}...): {e}")
        send_queue.task_done()
        await asyncio.sleep(0.1)

# ==================== رسائل الإشعارات فقط ====================

def build_startup_message() -> str:
    """رسالة بداية التشغيل"""
    wallet_count = len(WALLETS_DATA)
    return (
        f"🚀 <b>تم تشغيل البوت بنجاح!</b>\n\n"
        f"📊 عدد المحافظ: {wallet_count}\n"
        f"🔗 الشبكات: Robinhood + Ethereum\n"
        f"💰 الحد الأقصى للغاز: $0.05 (Robinhood) / $0.50 (Ethereum)\n"
        f"🔄 جارٍ مراقبة المينتات المجانية..."
    )

def build_purchase_message(detail: dict, result: dict, chain_key: str) -> str:
    """رسالة تم الشراء بنجاح"""
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    chain_label = "Robinhood" if chain_key == "robinhood" else "Ethereum"
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    
    return (
        f"✅ <b>تم الشراء بنجاح!</b>\n\n"
        f"📦 المجموعة: <b>{name}</b>\n"
        f"🔗 الشبكة: {chain_label}\n"
        f"👛 المحفظة: <code>{w_short}</code>\n"
        f"🔢 الكمية: {result['quantity']}\n"
        f"⛽ رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
        f"🔗 المعاملة: <code>{result['tx_hash'][:10]}...</code>\n"
        f"🌐 <a href='{url}'>عرض على OpenSea</a>"
    )

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
            
            # إرسال إشعار الشراء فقط
            msg = build_purchase_message(item.get("current_detail", {}), res, item.get("chain_key", ""))
            enqueue_message(bot_token, chat_id, msg)

        return res

async def try_buy_now_multi_wallet(slug: str, chain_key: str, detail: dict):
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
# تقييم المينتات (مع تتبع المدفوعة)
# ---------------------------------------------------------------------------

async def evaluate_new_mint(slug: str, chain_key: str):
    """تقييم المينت الجديد مع تتبع المينتات المدفوعة"""
    
    if slug in successful_mints and len(successful_mints[slug]) >= len(WALLETS_DATA):
        return
    
    if slug in in_flight:
        return
    
    if is_in_cooldown(slug):
        return

    in_flight.add(slug)
    try:
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not found or not detail or not detail.get("is_minting"):
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return

        w3 = W3_INSTANCES[chain_key]
        eth_price_usd = get_eth_price_usd()
        contract_address = detail.get("contract_address")
        
        if contract_address:
            onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
            price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
            
            if not is_free_or_negligible(price_wei, eth_price_usd):
                # تخزين للتتبع بدون إشعار
                paid_mints_tracking[slug] = {
                    "chain_key": chain_key,
                    "detail": detail,
                    "first_seen": time.time(),
                    "last_check": time.time(),
                    "check_count": 0
                }
                return

        twitter_username = get_cached_twitter(slug)
        if twitter_username is None:
            twitter_username = await asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
            set_cached_twitter(slug, twitter_username)
        
        if not twitter_username:
            mark_rejected(slug)
            return

        results = await try_buy_now_multi_wallet(slug, chain_key, detail)

        if results is None:
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            return

        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}

    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)

# ---------------------------------------------------------------------------
# فحص ذكي متكيف للمينتات المدفوعة
# ---------------------------------------------------------------------------

def calculate_adaptive_interval() -> int:
    paid_count = len(paid_mints_tracking)
    
    if paid_count == 0:
        return 60
    elif paid_count <= 3:
        return 10
    elif paid_count <= 10:
        return 15
    elif paid_count <= 20:
        return 20
    else:
        return 30

def get_priority_mints(limit: int = 15) -> list:
    if not paid_mints_tracking:
        return []
    
    sorted_mints = sorted(
        paid_mints_tracking.items(),
        key=lambda x: (
            x[1].get('first_seen', 0),
            x[1].get('check_count', 0)
        )
    )
    return sorted_mints[:limit]

async def scan_paid_mints():
    while True:
        try:
            check_interval = calculate_adaptive_interval()
            await asyncio.sleep(check_interval)
            
            priority_mints = get_priority_mints(limit=15)
            
            for slug, data in priority_mints:
                data['check_count'] = data.get('check_count', 0) + 1
                data['last_check'] = time.time()
                
                if slug in successful_mints and len(successful_mints[slug]) >= len(WALLETS_DATA):
                    paid_mints_tracking.pop(slug, None)
                    continue
                
                found, fresh_detail = await asyncio.to_thread(fetch_drop_detail, slug)
                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    paid_mints_tracking.pop(slug, None)
                    continue
                
                stage = fresh_detail.get("active_stage")
                if not stage:
                    continue
                
                chain_key = data.get("chain_key", "ethereum")
                w3 = W3_INSTANCES[chain_key]
                eth_price_usd = get_eth_price_usd()
                contract_address = fresh_detail.get("contract_address")
                
                if contract_address:
                    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
                    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
                    
                    if is_free_or_negligible(price_wei, eth_price_usd):
                        log.info(f"🔄 '{slug}' أصبح مجانياً!")
                        paid_mints_tracking.pop(slug, None)
                        asyncio.create_task(evaluate_new_mint(slug, chain_key))
                    else:
                        paid_mints_tracking[slug] = {
                            "chain_key": chain_key,
                            "detail": fresh_detail,
                            "first_seen": data.get("first_seen", time.time()),
                            "last_check": time.time(),
                            "check_count": data.get("check_count", 0)
                        }
            
            # تنظيف المينتات القديمة
            now = time.time()
            expired = []
            for slug, data in paid_mints_tracking.items():
                if now - data.get('first_seen', now) > 3600:
                    expired.append(slug)
            
            for slug in expired:
                paid_mints_tracking.pop(slug, None)
                        
        except Exception as e:
            log.error(f"خطأ في فحص المينتات المدفوعة: {e}")
            await asyncio.sleep(5)

# ---------------------------------------------------------------------------
# watch_loop
# ---------------------------------------------------------------------------

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
                found, fresh_detail = await asyncio.to_thread(fetch_drop_detail, slug)

                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    watchlist.pop(slug, None)
                    continue

                stage = fresh_detail.get("active_stage")
                if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
                    watchlist.pop(slug, None)
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
# الاستماع إلى OpenSea
# ---------------------------------------------------------------------------

async def process_event_buffer():
    global event_buffer, last_flush
    
    if not event_buffer:
        return
    
    unique_events = {}
    for event in event_buffer:
        slug = event['slug']
        if slug not in unique_events or event['timestamp'] > unique_events[slug]['timestamp']:
            unique_events[slug] = event
    
    for event in unique_events.values():
        asyncio.create_task(evaluate_new_mint(event['slug'], event['chain_key']))
    
    event_buffer = []
    last_flush = time.time()

async def listen_opensea():
    global event_buffer, last_flush
    
    msg_ref = 0
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"متصل بـ OpenSea Stream — يراقب لـ {len(WALLETS_DATA)} محافظ.")
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

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        if event_buffer and time.time() - last_flush > BUFFER_FLUSH_INTERVAL:
                            await process_event_buffer()
                        continue

                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if isinstance(parsed, list) and len(parsed) == 5:
                        _jref, _ref, _topic, event_name, payload_wrapper = parsed
                    else:
                        continue

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

                    event_buffer.append({
                        'slug': slug,
                        'chain_key': chain_key,
                        'timestamp': time.time()
                    })

                    if len(event_buffer) >= MAX_BUFFER_SIZE:
                        await process_event_buffer()

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"خطأ غير متوقع: {e}.")
            await asyncio.sleep(5)

# ==================== التشغيل الرئيسي ====================

async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        # إرسال إشعار واحد فقط
        broadcast_message("🔴 البوت في وضع الإيقاف (BOT_ENABLED=false).")
        await telegram_sender()
        return

    # إرسال رسالة بداية التشغيل
    broadcast_message(build_startup_message())
    log.info("🚀 تم تشغيل البوت بنجاح!")
    
    await asyncio.gather(
        listen_opensea(),
        scan_paid_mints(),
        watch_loop(),
        telegram_sender()
    )

def main():
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدويًا.")
            break
        except Exception as e:
            log.critical(f"توقف غير متوقع: {e}.")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break

if __name__ == "__main__":
    main()
