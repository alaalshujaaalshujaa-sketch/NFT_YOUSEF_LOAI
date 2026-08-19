"""
النظام الكامل — 10 محافظ مع تحسينات السرعة
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, List, Tuple

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

# ============ الإعدادات ============
OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

# إعدادات السرعة
FAST_MODE = os.environ.get("FAST_MODE", "true").lower() == "true"
PARALLEL_WORKERS = int(os.environ.get("PARALLEL_WORKERS", "40"))
RPC_TIMEOUT = int(os.environ.get("RPC_TIMEOUT", "5"))

PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]
TELEGRAM_BOT_TOKENS = [t.strip() for t in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if t.strip()]
TELEGRAM_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

if not (len(PRIVATE_KEYS) == len(WALLETS) == len(TELEGRAM_BOT_TOKENS) == len(TELEGRAM_CHAT_IDS)):
    raise ValueError("أعداد المفاتيح، المحافظ، توكنات البوتات، و Chat IDs غير متطابقة!")

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

# إعدادات السرعة
HEARTBEAT_INTERVAL = 30
RECV_TIMEOUT = 2 if FAST_MODE else 5
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 2 if FAST_MODE else 15
REJECTION_COOLDOWN_SECONDS = 20 if FAST_MODE else 120

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer-fast")

# Executor للعمليات المتزامنة
EXECUTOR = ThreadPoolExecutor(max_workers=PARALLEL_WORKERS)

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

# ============ State ============
successful_mints: Dict[str, set] = {}
watchlist: Dict[str, Dict] = {}
in_flight: set = set()
rejected_cooldown: Dict[str, float] = {}
priority_queue: asyncio.Queue = asyncio.Queue()
send_queue: asyncio.Queue = asyncio.Queue()

# ============ Cache ============
_collection_cache: Dict[str, Tuple[dict, float]] = {}
_eth_price_cache = {"value": None, "ts": 0}

def get_cached_collection(slug: str) -> Optional[dict]:
    now = time.time()
    if slug in _collection_cache:
        data, timestamp = _collection_cache[slug]
        if now - timestamp < 5:
            return data
    return None

def set_cached_collection(slug: str, data: dict):
    _collection_cache[slug] = (data, time.time())

# ============ Cooldown ============
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

# ============ ETH Price ============
def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < 1):
        return _eth_price_cache["value"]
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=2,
        )
        price = resp.json()["ethereum"]["usd"]
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except Exception:
        return _eth_price_cache["value"] or 3000.0

# ============ OpenSea API ============
def fetch_drop_detail(slug: str) -> Tuple[bool, Optional[dict]]:
    cached = get_cached_collection(slug)
    if cached is not None:
        return True, cached
    
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=RPC_TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            set_cached_collection(slug, data)
            return True, data
        if resp.status_code == 404:
            return False, None
        return None, None
    except Exception as e:
        log.warning(f"[Drops API] خطأ: {e}")
        return None, None

# ============ Telegram ============
def enqueue_message(bot_token: str, chat_id: str, text: str):
    send_queue.put_nowait({"bot_token": bot_token, "chat_id": chat_id, "text": text})

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
                timeout=3,
            )
        except Exception as e:
            log.error(f"خطأ إرسال تليجرام: {e}")
        send_queue.task_done()

# ============ Purchase ============
def build_single_wallet_success_msg(detail: dict, result: dict, chain_key: str) -> str:
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
        f"المعاملة: {result['tx_hash']}\n"
        f"🔗 {url}"
    )

async def purchase_task_for_wallet(w3, item, slug, contract_address, price_wei, 
                                   max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd):
    wallet_addr = item["wallet"]
    pk = item["private_key"]
    bot_token = item["bot_token"]
    chat_id = item["chat_id"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        if wallet_addr in successful_mints.get(slug, set()):
            return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

        res = await asyncio.get_event_loop().run_in_executor(
            EXECUTOR,
            attempt_purchase_single_wallet,
            w3, pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd,
        )

        if res.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            
            msg = build_single_wallet_success_msg(
                item.get("current_detail", {}), res, item.get("chain_key", "")
            )
            enqueue_message(bot_token, chat_id, msg)

        return res

async def batch_purchase_mint(slug: str, chain_key: str, detail: dict) -> List[Dict]:
    stage = detail.get("active_stage")
    if not stage:
        return []

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
        return []

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

    results = await asyncio.gather(*tasks, return_exceptions=True)
    valid_results = [r for r in results if isinstance(r, dict)]
    
    successful_count = sum(1 for r in valid_results if r.get("success"))
    if successful_count > 0:
        log.info(f"✅ {successful_count} محفظة اشترت في {slug}")
    
    return valid_results

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    return (price_wei / 1e18) * eth_price_usd < FREE_PRICE_THRESHOLD_USD

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

def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

# ============ Mint Evaluation ============
async def evaluate_new_mint(slug: str, chain_key: str):
    if (len(successful_mints.get(slug, set())) >= len(WALLETS_DATA) or
        slug in watchlist or slug in in_flight or is_in_cooldown(slug)):
        return

    in_flight.add(slug)
    try:
        found, detail = await asyncio.get_event_loop().run_in_executor(
            EXECUTOR, fetch_drop_detail, slug
        )
        
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
                mark_rejected(slug)
                return

        twitter_username = await asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
        if not twitter_username:
            log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X.")
            mark_rejected(slug)
            return

        log.info(f"✅ '{slug}': يوجد حساب X (@{twitter_username}) — جاري الشراء.")
        await batch_purchase_mint(slug, chain_key, detail)

        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}

    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)

# ============ Priority Processor ============
async def priority_processor():
    while True:
        try:
            slug, chain_key = await priority_queue.get()
            await evaluate_new_mint(slug, chain_key)
            priority_queue.task_done()
        except Exception as e:
            log.error(f"خطأ في معالج الأولوية: {e}")
        await asyncio.sleep(0.05)

# ============ Watch Loop ============
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
                found, fresh_detail = await asyncio.get_event_loop().run_in_executor(
                    EXECUTOR, fetch_drop_detail, slug
                )

                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    watchlist.pop(slug, None)
                    broadcast_message(f"❌ انتهت الفرصة: {slug}")
                    continue

                stage = fresh_detail.get("active_stage")
                if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
                    watchlist.pop(slug, None)
                    continue

                await batch_purchase_mint(slug, chain_key, fresh_detail)

                if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                    watchlist.pop(slug, None)
                else:
                    watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}

            except Exception as e:
                log.error(f"خطأ بدورة مراقبة '{slug}': {e}")
            finally:
                in_flight.discard(slug)

# ============ Health Monitor ============
async def health_monitor():
    while True:
        await asyncio.sleep(10)
        qsize = priority_queue.qsize()
        if qsize > 50:
            log.warning(f"⚠️ قائمة الأولوية: {qsize}")

# ============ WebSocket ============
async def listen_opensea():
    msg_ref = 0
    while True:
        try:
            async with websockets.connect(
                STREAM_URL,
                ping_interval=None,
                open_timeout=3,
                close_timeout=3,
                max_size=2**20,
                compression=None if FAST_MODE else "deflate"
            ) as ws:
                log.info(f"🚀 متصل بـ OpenSea (سريع: {FAST_MODE})")
                await ws.send(json.dumps([str(msg_ref), str(msg_ref), "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()

                while True:
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        await ws.send(json.dumps([None, str(msg_ref), "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        continue

                    if not isinstance(raw, str) or 'item_transferred' not in raw:
                        continue

                    try:
                        parsed = json.loads(raw)
                    except:
                        continue

                    if not isinstance(parsed, list) or len(parsed) != 5:
                        continue

                    _, _, _, event_name, payload_wrapper = parsed
                    
                    if event_name not in {'item_transferred', 'item_minted'}:
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    item = payload.get("item", {}) or {}
                    
                    stream_chain_name = (item.get("chain", {}) or {}).get("name", "")
                    chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
                    if not chain_key:
                        continue

                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if slug:
                        await priority_queue.put((slug, chain_key))

        except Exception as e:
            log.warning(f"🔄 إعادة اتصال: {e}")
            await asyncio.sleep(3)

# ============ Main ============
async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        return

    broadcast_message(f"✅ تم تشغيل البوت (وضع سريع: {FAST_MODE})")
    log.info(f"🚀 {len(WALLETS_DATA)} محافظ, {PARALLEL_WORKERS} عامل")
    
    await asyncio.gather(
        listen_opensea(),
        watch_loop(),
        telegram_sender(),
        priority_processor(),
        health_monitor()
    )

def main():
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("🛑 تم الإيقاف")
            break
        except Exception as e:
            log.critical(f"💥 {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
