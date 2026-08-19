"""
النظام الكامل — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها:
  - يكتشف مينتات اليوم على Robinhood + Ethereum
  - يشتري لجميع المحافظ المعرفة بالتوازي (Parallel Execution)
  - يرسل إشعار الشراء أو التحديث لكل محفظة على بوت التيليجرام الخاص بها
  - محسّن للسرعة القصوى مع caching و batch processing
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, List, Tuple, Any

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_wallet_lock,
    get_cached_gas_price,
    get_cached_contract
)
from twitter_checker import get_twitter_username_from_opensea, get_cached_twitter

# محاولة استخدام uvloop للسرعة
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

load_dotenv()

# ============ الإعدادات الأساسية ============
OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

# إعدادات السرعة
FAST_MODE = os.environ.get("FAST_MODE", "true").lower() == "true"
PARALLEL_WORKERS = int(os.environ.get("PARALLEL_WORKERS", "20"))
RPC_TIMEOUT = int(os.environ.get("RPC_TIMEOUT", "5"))
CACHE_TTL = int(os.environ.get("CACHE_TTL", "1"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5"))

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

# ============ إعدادات السرعة المحسّنة ============
HEARTBEAT_INTERVAL = 30
RECV_TIMEOUT = 2 if FAST_MODE else 5
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 3 if FAST_MODE else 15
REJECTION_COOLDOWN_SECONDS = 30 if FAST_MODE else 120
PRIORITY_PROCESS_INTERVAL = 0.1 if FAST_MODE else 1

# ============ Logging ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S.%f"[:-3],  # عرض الميلي ثانية
)
log = logging.getLogger("auto-buyer-fast")

# ============ Executor للعمليات المتزامنة ============
EXECUTOR = ThreadPoolExecutor(max_workers=PARALLEL_WORKERS)

# ============ إعدادات السلاسل ============
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

# ============ Cache Systems ============
_collection_cache: Dict[str, Tuple[Any, float]] = {}
_price_cache: Dict[str, Tuple[float, float]] = {}
_eth_price_cache: Dict[str, Any] = {"value": None, "ts": 0}
_twitter_cache: Dict[str, Tuple[Optional[str], float]] = {}

# ============ State Management ============
successful_mints: Dict[str, set] = {}
watchlist: Dict[str, Dict] = {}
in_flight: set = set()
rejected_cooldown: Dict[str, float] = {}
priority_queue: asyncio.Queue = asyncio.Queue()
send_queue: asyncio.Queue = asyncio.Queue()

# ============ Performance Metrics ============
PERFORMANCE_LOG: List[Dict] = []
PERFORMANCE_LOG_MAX = 100

# ============ Cache Functions ============
def get_cached_collection(slug: str, force_refresh: bool = False) -> Optional[Any]:
    """الحصول على بيانات المجموعة من cache"""
    now = time.time()
    if not force_refresh and slug in _collection_cache:
        data, timestamp = _collection_cache[slug]
        if now - timestamp < CACHE_TTL:
            return data
    return None

def set_cached_collection(slug: str, data: Any):
    """تخزين بيانات المجموعة في cache"""
    _collection_cache[slug] = (data, time.time())

def get_cached_price(contract: str) -> Optional[float]:
    """الحصول على سعر العقد من cache"""
    now = time.time()
    if contract in _price_cache:
        price, timestamp = _price_cache[contract]
        if now - timestamp < CACHE_TTL:
            return price
    return None

def set_cached_price(contract: str, price: float):
    """تخزين سعر العقد في cache"""
    _price_cache[contract] = (price, time.time())

def get_cached_twitter_username(slug: str) -> Optional[str]:
    """الحصول على اسم تويتر من cache"""
    now = time.time()
    if slug in _twitter_cache:
        username, timestamp = _twitter_cache[slug]
        if now - timestamp < (CACHE_TTL * 60):  # 1 دقيقة للتويتر
            return username
    return None

def set_cached_twitter_username(slug: str, username: Optional[str]):
    """تخزين اسم تويتر في cache"""
    _twitter_cache[slug] = (username, time.time())

# ============ إدارة التبريد ============
def is_in_cooldown(slug: str) -> bool:
    """التحقق من وجود تبريد للمجموعة"""
    ts = rejected_cooldown.get(slug)
    if ts is None:
        return False
    now = time.time()
    if now - ts >= REJECTION_COOLDOWN_SECONDS:
        rejected_cooldown.pop(slug, None)
        return False
    return True

def mark_rejected(slug: str, reason: str = "general"):
    """تسجيل رفض المجموعة مع وقت تبريد حسب السبب"""
    cooldown_times = {
        "no_twitter": 60,
        "not_free": 30,
        "api_error": 10,
        "sold_out": 60,
        "general": 30
    }
    cooldown = cooldown_times.get(reason, REJECTION_COOLDOWN_SECONDS)
    rejected_cooldown[slug] = time.time() + (cooldown - REJECTION_COOLDOWN_SECONDS)

# ============ سعر ETH ============
def get_eth_price_usd() -> float:
    """جلب سعر ETH مع cache سريع"""
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
    except Exception as e:
        log.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0

# ============ OpenSea API Functions ============
def fetch_drop_detail(slug: str, use_cache: bool = True) -> Tuple[bool, Optional[Any]]:
    """جلب تفاصيل المينت مع cache"""
    if use_cache:
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

async def fetch_multiple_drop_details(slugs: List[str]) -> Dict[str, Any]:
    """جلب تفاصيل عدة مجموعات دفعة واحدة"""
    tasks = []
    for slug in slugs:
        if get_cached_collection(slug) is None:
            tasks.append(asyncio.to_thread(fetch_drop_detail, slug, True))
    
    if not tasks:
        return {}
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    result_dict = {}
    for i, slug in enumerate(slugs):
        if i < len(results) and not isinstance(results[i], Exception):
            found, detail = results[i]
            if found and detail:
                result_dict[slug] = detail
    
    return result_dict

# ============ Telegram Functions ============
class TelegramBatch:
    """إرسال رسائل تليجرام بشكل مجمع"""
    def __init__(self):
        self.messages = []
        self.last_send = 0
        self.batch_size = 10
        
    async def send_batch(self):
        """إرسال رسائل مجمعة"""
        if not self.messages:
            return
            
        # تجميع حسب البوت
        grouped = {}
        for msg in self.messages:
            key = (msg["bot_token"], msg["chat_id"])
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(msg["text"])
        
        # إرسال كل مجموعة
        for (bot_token, chat_id), texts in grouped.items():
            if len(texts) == 1:
                await self.send_single(bot_token, chat_id, texts[0])
            else:
                combined = "\n\n---\n\n".join(texts)
                await self.send_single(bot_token, chat_id, combined)
        
        self.messages = []
        self.last_send = time.time()
    
    async def send_single(self, bot_token: str, chat_id: str, text: str):
        """إرسال رسالة واحدة"""
        try:
            telegram_api = f"https://api.telegram.org/bot{bot_token}"
            await asyncio.to_thread(
                requests.post,
                f"{telegram_api}/sendMessage",
                data={"chat_id": chat_id, "text": text[:4000], "parse_mode": "HTML"},
                timeout=3,
            )
        except Exception as e:
            log.error(f"خطأ إرسال تليجرام: {e}")

telegram_batch = TelegramBatch()

def enqueue_message(bot_token: str, chat_id: str, text: str):
    """إضافة رسالة للقائمة"""
    send_queue.put_nowait({
        "bot_token": bot_token,
        "chat_id": chat_id,
        "text": text
    })

def broadcast_message(text: str):
    """إرسال لجميع المحافظ"""
    for w in WALLETS_DATA:
        enqueue_message(w["bot_token"], w["chat_id"], text)

async def telegram_sender():
    """معالج إرسال رسائل التليجرام"""
    while True:
        msg = await send_queue.get()
        telegram_batch.messages.append(msg)
        
        if len(telegram_batch.messages) >= telegram_batch.batch_size:
            await telegram_batch.send_batch()
        elif time.time() - telegram_batch.last_send > 0.5:
            await telegram_batch.send_batch()
        
        send_queue.task_done()

# ============ Purchase Functions ============
def build_single_wallet_success_msg(detail: dict, result: dict, chain_key: str) -> str:
    """بناء رسالة نجاح الشراء"""
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

async def purchase_task_for_wallet(
    w3, item, slug: str, contract_address: str, price_wei: int,
    max_per_wallet: Optional[int], remaining: int, eth_price_usd: float,
    max_gas_fee_usd: float
) -> Dict:
    """تنفيذ عملية شراء لمحفظة واحدة"""
    wallet_addr = item["wallet"]
    pk = item["private_key"]
    bot_token = item["bot_token"]
    chat_id = item["chat_id"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        if wallet_addr in successful_mints.get(slug, set()):
            return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

        # تنفيذ الشراء في thread منفصل
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
                item.get("current_detail", {}), 
                res, 
                item.get("chain_key", "")
            )
            enqueue_message(bot_token, chat_id, msg)

        return res

async def batch_purchase_mint(slug: str, chain_key: str, detail: dict) -> List[Dict]:
    """تنفيذ شراء متوازي لجميع المحافظ"""
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

    # جلب السعر من cache إن أمكن
    cached_price = get_cached_price(contract_address)
    if cached_price is not None:
        price_wei = int(cached_price)
    else:
        onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
        price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
        if price_wei > 0:
            set_cached_price(contract_address, float(price_wei))

    if not is_free_or_negligible(price_wei, eth_price_usd):
        return []

    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]

    already_bought_wallets = successful_mints.get(slug, set())
    pending_items = [
        item for item in WALLETS_DATA 
        if item["wallet"] not in already_bought_wallets
    ]

    if not pending_items:
        return [{"success": False, "reason": "all_wallets_completed"}]

    for item in pending_items:
        item["current_detail"] = detail
        item["chain_key"] = chain_key

    # تنفيذ جميع عمليات الشراء بالتوازي
    tasks = [
        purchase_task_for_wallet(
            w3, item, slug, contract_address,
            price_wei, max_per_wallet, remaining, 
            eth_price_usd, max_gas_fee_usd
        )
        for item in pending_items
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # معالجة النتائج
    valid_results = []
    successful_count = 0
    for result in results:
        if isinstance(result, dict):
            valid_results.append(result)
            if result.get("success"):
                successful_count += 1
        elif isinstance(result, Exception):
            log.error(f"خطأ في معاملة: {result}")
    
    if successful_count > 0:
        log.info(f"✅ {successful_count}/{len(pending_items)} محفظة اشترت بنجاح في {slug}")
    
    return valid_results

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    """التحقق من أن السعر مجاني أو لا يُذكر"""
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

def started_today_local(stage: dict) -> bool:
    """التحقق من أن المينت بدأ اليوم"""
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()

def stage_has_ended(stage: dict) -> bool:
    """التحقق من انتهاء المرحلة"""
    end = parse_iso(stage.get("end_time", ""))
    if not end:
        return False
    return datetime.now(timezone.utc) > end

def parse_iso(ts: str):
    """تحويل ISO string إلى datetime"""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

# ============ Performance Monitoring ============
async def measure_performance(slug: str, start_time: float):
    """قياس أداء الاكتشاف والشراء"""
    elapsed = time.time() - start_time
    PERFORMANCE_LOG.append({
        'slug': slug,
        'time': elapsed,
        'timestamp': datetime.now()
    })
    
    # الاحتفاظ بآخر 100 قياس فقط
    if len(PERFORMANCE_LOG) > PERFORMANCE_LOG_MAX:
        PERFORMANCE_LOG.pop(0)
    
    if elapsed > 0.5:
        log.warning(f"⚠️ اكتشاف بطيء لـ {slug}: {elapsed:.3f}s")
    else:
        log.info(f"⚡ اكتشاف سريع لـ {slug}: {elapsed:.3f}s")
    
    # عرض متوسط الأداء كل 10 اكتشافات
    if len(PERFORMANCE_LOG) % 10 == 0:
        avg = sum(x['time'] for x in PERFORMANCE_LOG[-10:]) / 10
        log.info(f"📊 متوسط وقت الاكتشاف (آخر 10): {avg:.3f}s")

# ============ Mint Evaluation ============
async def evaluate_new_mint(slug: str, chain_key: str):
    """تقييم المينت الجديد واتخاذ إجراء"""
    start_time = time.time()
    
    if (len(successful_mints.get(slug, set())) >= len(WALLETS_DATA) or
        slug in watchlist or slug in in_flight or is_in_cooldown(slug)):
        return

    in_flight.add(slug)
    try:
        # 1. جلب تفاصيل المينت مع cache
        found, detail = await asyncio.get_event_loop().run_in_executor(
            EXECUTOR, fetch_drop_detail, slug, True
        )
        
        if not found or not detail or not detail.get("is_minting"):
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return

        # 2. التحقق من السعر
        w3 = W3_INSTANCES[chain_key]
        eth_price_usd = get_eth_price_usd()
        contract_address = detail.get("contract_address")
        
        if contract_address:
            # استخدام cache للسعر
            cached_price = get_cached_price(contract_address)
            if cached_price is not None:
                price_wei = int(cached_price)
            else:
                onchain_price = await asyncio.to_thread(
                    get_onchain_public_price_wei, w3, contract_address
                )
                price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
                if price_wei > 0:
                    set_cached_price(contract_address, float(price_wei))
            
            if not is_free_or_negligible(price_wei, eth_price_usd):
                mark_rejected(slug, "not_free")
                return

        # 3. التحقق من تويتر مع cache
        twitter_username = await asyncio.get_event_loop().run_in_executor(
            EXECUTOR, get_twitter_username_from_opensea, slug, OPENSEA_API_KEY
        )
        
        if not twitter_username:
            log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X مربوط.")
            mark_rejected(slug, "no_twitter")
            return

        log.info(f"✅ '{slug}': يوجد حساب X مربوط (@{twitter_username}) — جاري الشراء.")

        # 4. تنفيذ الشراء
        results = await batch_purchase_mint(slug, chain_key, detail)

        # تحديث حالة المراقبة
        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            broadcast_message(f"👀 <b>تحت المراقبة</b>\n\nالمجموعة: <b>{slug}</b>")

        # تسجيل الأداء
        await measure_performance(slug, start_time)

    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
        mark_rejected(slug, "api_error")
    finally:
        in_flight.discard(slug)

# ============ Priority Queue Processor ============
async def priority_processor():
    """معالجة المينتات ذات الأولوية العالية"""
    while True:
        try:
            slug, chain_key = await priority_queue.get()
            await evaluate_new_mint(slug, chain_key)
            priority_queue.task_done()
        except Exception as e:
            log.error(f"خطأ في معالج الأولوية: {e}")
        await asyncio.sleep(PRIORITY_PROCESS_INTERVAL)

# ============ Watch Loop ============
async def watch_loop():
    """مراقبة المجموعات المضافة لقائمة المراقبة"""
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        if not watchlist:
            continue

        for slug in list(watchlist.keys()):
            if (slug in in_flight or 
                len(successful_mints.get(slug, set())) >= len(WALLETS_DATA)):
                watchlist.pop(slug, None)
                continue

            entry = watchlist.get(slug)
            if not entry:
                continue

            in_flight.add(slug)
            try:
                chain_key = entry["chain_key"]
                found, fresh_detail = await asyncio.get_event_loop().run_in_executor(
                    EXECUTOR, fetch_drop_detail, slug, False  # force refresh
                )

                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    watchlist.pop(slug, None)
                    broadcast_message(f"❌ <b>انتهت الفرصة</b>\n\nالمجموعة: <b>{slug}</b>")
                    continue

                stage = fresh_detail.get("active_stage")
                if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
                    watchlist.pop(slug, None)
                    broadcast_message(f"❌ <b>انتهت المرحلة</b>\n\nالمجموعة: <b>{slug}</b>")
                    continue

                # محاولة الشراء مرة أخرى
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
    """مراقبة صحة النظام"""
    while True:
        await asyncio.sleep(10)
        
        # مراقبة قائمة الأولوية
        qsize = priority_queue.qsize()
        if qsize > 50:
            log.warning(f"⚠️ قائمة الأولوية كبيرة: {qsize}")
            # معالجة سريعة للمتراكمة
            for _ in range(min(10, qsize)):
                try:
                    slug, chain_key = priority_queue.get_nowait()
                    asyncio.create_task(evaluate_new_mint(slug, chain_key))
                    priority_queue.task_done()
                except:
                    break
        
        # مراقبة المعاملات المعلقة
        from buyer import _nonce_cache
        for wallet in WALLETS_DATA:
            wallet_addr = wallet["wallet"]
            if wallet_addr in _nonce_cache:
                nonce, pending = _nonce_cache[wallet_addr]
                if pending > 5:
                    log.warning(f"⚠️ {wallet_addr[:8]}... لديه {pending} معاملة معلقة")

# ============ WebSocket Listener ============
async def listen_opensea():
    """الاستماع إلى WebSocket مع فلترة سريعة"""
    msg_ref = 0
    reconnect_attempts = 0
    max_reconnect_delay = 5
    
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
                log.info(f"🚀 متصل بـ OpenSea Stream (وضع سريع: {FAST_MODE})")
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()
                reconnect_attempts = 0

                while True:
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        continue

                    # فلترة سريعة جداً - التحقق من النص الخام
                    if not isinstance(raw, str) or len(raw) < 20:
                        continue
                        
                    # تحقق سريع من وجود أحداث المينت
                    if 'item_transferred' not in raw and 'item_minted' not in raw:
                        continue

                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if not isinstance(parsed, list) or len(parsed) != 5:
                        continue

                    _, _, _, event_name, payload_wrapper = parsed
                    
                    # تجاهل الأحداث غير المهمة
                    if event_name not in {'item_transferred', 'item_minted'}:
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    item = payload.get("item", {}) or {}
                    
                    # تحقق سريع من السلسلة
                    stream_chain_name = (item.get("chain", {}) or {}).get("name", "")
                    chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
                    if chain_key is None:
                        continue

                    # التحقق من أن المينت من الصفر
                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug:
                        continue

                    # إضافة لقائمة الأولوية فوراً
                    await priority_queue.put((slug, chain_key))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            reconnect_attempts += 1
            delay = min(reconnect_attempts, max_reconnect_delay)
            log.warning(f"🔄 انقطع الاتصال. إعادة الاتصال خلال {delay} ثانية...")
            await asyncio.sleep(delay)
        except Exception as e:
            log.error(f"❌ خطأ غير متوقع: {e}")
            await asyncio.sleep(2)

# ============ Main ============
async def run():
    """تشغيل النظام"""
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        broadcast_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false).")
        await telegram_sender()
        return

    broadcast_message(f"✅ تم تشغيل المحفظة الخاصة بك بنجاح وربطها بهذا البوت!")
    broadcast_message(f"⚡ وضع السرعة: {'مفعل' if FAST_MODE else 'عادي'}")
    
    log.info(f"🚀 بدء تشغيل النظام السريع - {len(WALLETS_DATA)} محافظ")
    log.info(f"⚡ FAST_MODE: {FAST_MODE}, WORKERS: {PARALLEL_WORKERS}")
    
    await asyncio.gather(
        listen_opensea(),
        watch_loop(),
        telegram_sender(),
        priority_processor(),
        health_monitor()
    )

def main():
    """الدالة الرئيسية مع إعادة اتصال ذكية"""
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("🛑 تم الإيقاف يدويًا.")
            break
        except Exception as e:
            log.critical(f"💥 توقف غير متوقع: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break

if __name__ == "__main__":
    main()
