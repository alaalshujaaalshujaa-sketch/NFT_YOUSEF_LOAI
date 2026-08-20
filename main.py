"""
النظام الكامل — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها:
  - يكتشف مينتات اليوم على Robinhood + Ethereum
  - يشتري فقط المينتات المجانية (السعر = 0)
  - يتحقق من وجود حساب X (تويتر) عبر OpenSea API فقط
  - يستخدم معالجة متوازية للـ APIs لزيادة السرعة
  - بدون استخدام RPC للحصول على السعر (يعتمد على OpenSea API)
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    attempt_purchase_single_wallet,
    get_wallet_lock,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

# ==================== الإعدادات الأساسية ====================
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
COLLECTIONS_API = "https://api.opensea.io/api/v2/collections"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

# ==================== إعدادات الأداء والسرعة ====================
HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 15
REJECTION_COOLDOWN_SECONDS = 120

# إعدادات التخزين المؤقت للسرعة
CACHE_TWITTER_TTL = 300  # 5 دقائق
CACHE_DETAIL_TTL = 60    # 1 دقيقة

# تجمع مؤشرات الترابط للـ APIs
executor = ThreadPoolExecutor(max_workers=10)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

# ==================== إعدادات السلاسل ====================
CHAIN_CONFIGS = {
    "robinhood": {
        "stream_chain_name": "robinhood",
        "max_gas_fee_usd": 0.05,
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "max_gas_fee_usd": 0.50,
    },
}

STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# ==================== التخزين المؤقت للسرعة ====================
cache = {
    "twitter": {},      # slug -> (username, timestamp)
    "detail": {},       # slug -> (detail, timestamp)
    "price": {},        # slug -> (price_wei, timestamp)
    "eth_price": {"value": None, "ts": 0}
}

# ==================== التخزين الدائم ====================
SUCCESS_FILE = Path("successful_mints.json")

# تتبع المحافظ التي اشترت بنجاح: slug -> set(wallet_address)
successful_mints: dict[str, set[str]] = {}
watchlist: dict[str, dict] = {}
in_flight: set[str] = set()
rejected_cooldown: dict[str, float] = {}

def load_successful_mints():
    """تحميل المينتات الناجحة من الملف"""
    global successful_mints
    if SUCCESS_FILE.exists():
        try:
            with open(SUCCESS_FILE, 'r') as f:
                data = json.load(f)
                successful_mints = {k: set(v) for k, v in data.items()}
                log.info(f"✅ تم تحميل {len(successful_mints)} مينت ناجح")
        except Exception as e:
            log.warning(f"⚠️ تعذر تحميل المينتات الناجحة: {e}")
            successful_mints = {}
    else:
        successful_mints = {}

def save_successful_mints():
    """حفظ المينتات الناجحة إلى الملف"""
    try:
        with open(SUCCESS_FILE, 'w') as f:
            json.dump({k: list(v) for k, v in successful_mints.items()}, f, indent=2)
    except Exception as e:
        log.error(f"❌ خطأ في حفظ البيانات: {e}")

# ==================== دوال مساعدة مع تخزين مؤقت ====================

def is_in_cooldown(slug: str) -> bool:
    """التحقق من وجود تبريد لمجموعة معينة"""
    ts = rejected_cooldown.get(slug)
    if ts is None:
        return False
    if time.time() - ts >= REJECTION_COOLDOWN_SECONDS:
        rejected_cooldown.pop(slug, None)
        return False
    return True

def mark_rejected(slug: str):
    """تسجيل وقت رفض المجموعة للتبريد"""
    rejected_cooldown[slug] = time.time()

def get_eth_price_usd() -> float:
    """جلب سعر ETH مع تخزين مؤقت"""
    now = time.time()
    if cache["eth_price"]["value"] and (now - cache["eth_price"]["ts"] < 300):
        return cache["eth_price"]["value"]
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=5,
        )
        price = resp.json()["ethereum"]["usd"]
        cache["eth_price"]["value"] = price
        cache["eth_price"]["ts"] = now
        return price
    except Exception as e:
        log.warning(f"⚠️ [السعر] تعذر جلب سعر ETH: {e}")
        return cache["eth_price"]["value"] or 3000.0

def get_cached_twitter(slug: str) -> str | None:
    """جلب حساب تويتر من التخزين المؤقت"""
    if slug in cache["twitter"]:
        username, ts = cache["twitter"][slug]
        if time.time() - ts < CACHE_TWITTER_TTL:
            return username
        del cache["twitter"][slug]
    return None

def set_cached_twitter(slug: str, username: str | None):
    """تخزين حساب تويتر في التخزين المؤقت"""
    cache["twitter"][slug] = (username, time.time())

def get_cached_detail(slug: str) -> dict | None:
    """جلب تفاصيل المينت من التخزين المؤقت"""
    if slug in cache["detail"]:
        detail, ts = cache["detail"][slug]
        if time.time() - ts < CACHE_DETAIL_TTL:
            return detail
        del cache["detail"][slug]
    return None

def set_cached_detail(slug: str, detail: dict):
    """تخزين تفاصيل المينت في التخزين المؤقت"""
    cache["detail"][slug] = (detail, time.time())

def get_cached_price(slug: str) -> int | None:
    """جلب السعر من التخزين المؤقت"""
    if slug in cache["price"]:
        price, ts = cache["price"][slug]
        if time.time() - ts < CACHE_DETAIL_TTL:
            return price
        del cache["price"][slug]
    return None

def set_cached_price(slug: str, price: int):
    """تخزين السعر في التخزين المؤقت"""
    cache["price"][slug] = (price, time.time())

# ==================== دوال API السريعة ====================

def fetch_drop_detail_fast(slug: str) -> tuple[bool, dict | None]:
    """جلب تفاصيل المينت من OpenSea API (سريع)"""
    try:
        # التحقق من التخزين المؤقت أولاً
        cached = get_cached_detail(slug)
        if cached:
            return True, cached
            
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=5,  # تقليل timeout للسرعة
        )
        if resp.status_code == 200:
            data = resp.json()
            set_cached_detail(slug, data)
            return True, data
        if resp.status_code == 404:
            return False, None
        return None, None
    except Exception as e:
        log.warning(f"⚠️ [Drops API] خطأ: {e}")
        return None, None

def fetch_price_fast(slug: str, detail: dict) -> int:
    """جلب السعر من تفاصيل المينت (بدون RPC)"""
    try:
        # التحقق من التخزين المؤقت
        cached = get_cached_price(slug)
        if cached is not None:
            return cached
            
        # جلب السعر من تفاصيل المينت (بدون RPC)
        stage = detail.get("active_stage", {})
        price_str = stage.get("price", "0")
        price_wei = int(price_str) if price_str else 0
        
        set_cached_price(slug, price_wei)
        return price_wei
    except Exception as e:
        log.warning(f"⚠️ [السعر] خطأ: {e}")
        return 0

def fetch_twitter_fast(slug: str) -> str | None:
    """جلب حساب تويتر من OpenSea API (سريع)"""
    try:
        # التحقق من التخزين المؤقت
        cached = get_cached_twitter(slug)
        if cached is not None:
            return cached if cached else None
            
        url = f"{COLLECTIONS_API}/{slug}"
        headers = {"x-api-key": OPENSEA_API_KEY}
        
        response = requests.get(url, headers=headers, timeout=5)  # تقليل timeout
        
        if response.status_code == 200:
            data = response.json()
            
            # البحث في social_links
            social_links = data.get('social_links', [])
            for link in social_links:
                if isinstance(link, dict):
                    url_lower = link.get('url', '').lower()
                    if 'twitter.com' in url_lower or 'x.com' in url_lower:
                        username = link.get('username')
                        if username:
                            set_cached_twitter(slug, username)
                            return username
            
            # البحث المباشر
            twitter_username = data.get('twitter_username')
            if twitter_username:
                set_cached_twitter(slug, twitter_username)
                return twitter_username
            
            # البحث في project_details
            project_details = data.get('project_details', {})
            if isinstance(project_details, dict):
                twitter_username = project_details.get('twitter_username')
                if twitter_username:
                    set_cached_twitter(slug, twitter_username)
                    return twitter_username
            
            set_cached_twitter(slug, None)
            return None
            
    except Exception as e:
        log.debug(f"⚠️ خطأ في جلب تويتر: {e}")
        return None

def fetch_all_data_parallel(slug: str) -> tuple[dict | None, int, str | None]:
    """جلب جميع البيانات بالتوازي لزيادة السرعة"""
    # جلب التفاصيل أولاً (لأنها تحتوي على السعر)
    found, detail = fetch_drop_detail_fast(slug)
    if not found or not detail:
        return None, 0, None
    
    # جلب السعر من التفاصيل (بدون RPC)
    price_wei = fetch_price_fast(slug, detail)
    
    # جلب تويتر (قد يكون في التخزين المؤقت)
    twitter_username = fetch_twitter_fast(slug)
    
    return detail, price_wei, twitter_username

# ==================== دوال مساعدة أخرى ====================

def parse_iso(ts: str):
    """تحويل التاريخ بصيغة ISO إلى datetime"""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def started_today_local(stage: dict) -> bool:
    """التحقق من أن المينت بدأ اليوم بالتوقيت المحلي"""
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

def is_free_mint(price_wei: int, eth_price_usd: float) -> bool:
    """التحقق من أن المينت مجاني"""
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

# ==================== إدارة رسائل التيليجرام ====================

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
    """معالج رسائل التيليجرام"""
    while True:
        msg = await send_queue.get()
        try:
            telegram_api = f"https://api.telegram.org/bot{msg['bot_token']}"
            await asyncio.to_thread(
                requests.post,
                f"{telegram_api}/sendMessage",
                data={"chat_id": msg["chat_id"], "text": msg["text"], "parse_mode": "HTML"},
                timeout=5,
            )
        except Exception as e:
            log.error(f"❌ خطأ إرسال تليجرام: {e}")
        send_queue.task_done()
        await asyncio.sleep(0.05)  # أسرع

# ==================== دوال بناء الرسائل ====================

def build_single_wallet_success_msg(detail: dict, result: dict, chain_key: str) -> str:
    """بناء رسالة نجاح الشراء لمحفظة واحدة"""
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    chain_label = "Robinhood Chain" if chain_key == "robinhood" else "Ethereum Mainnet"
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    return (
        f"✅ <b>تم الشراء بنجاح!</b> ({chain_label})\n\n"
        f"المحفظة: <code>{w_short}</code>\n"
        f"المجموعة: <b>{name}</b>\n"
        f"الكمية: {result['quantity']}\n"
        f"رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
        f"المعاملة: {result['tx_hash']}\n"
        f"🔗 {url}"
    )

def build_free_mint_detected_message(detail: dict, twitter_username: str) -> str:
    """بناء رسالة اكتشاف مينت مجاني"""
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    return (
        f"🎉 <b>تم اكتشاف مينت مجاني!</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"حساب X: @{twitter_username}\n"
        f"🔗 {url}\n\n"
        f"🔄 جاري الشراء لجميع المحافظ..."
    )

def build_watching_message(detail: dict, reason: str) -> str:
    """بناء رسالة وضع المراقبة"""
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"👀 <b>تحت المراقبة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}"

def build_gaveup_message(detail: dict, reason: str) -> str:
    """بناء رسالة انتهاء الفرصة"""
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"❌ <b>انتهت الفرصة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}"

# ==================== الشراء المتوازي ====================

async def purchase_task_for_wallet(
    item, slug, contract_address, price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd
):
    """مهمة الشراء لمحفظة واحدة"""
    wallet_addr = item["wallet"]
    pk = item["private_key"]
    bot_token = item["bot_token"]
    chat_id = item["chat_id"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        # التحقق من الشراء المسبق
        if wallet_addr in successful_mints.get(slug, set()):
            return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

        # محاولة الشراء (بدون RPC - نمرر السعر مباشرة)
        res = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd,
        )

        if res.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            
            # إرسال إشعار النجاح
            msg = build_single_wallet_success_msg(
                item.get("current_detail", {}), res, item.get("chain_key", "")
            )
            enqueue_message(bot_token, chat_id, msg)
            
            # حفظ البيانات
            save_successful_mints()

        return res

async def try_buy_now_multi_wallet(slug: str, chain_key: str, detail: dict) -> list[dict] | None:
    """محاولة الشراء لجميع المحافظ بالتوازي"""
    stage = detail.get("active_stage")
    if not stage:
        return None

    # التحقق من الكمية المتبقية
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    if remaining <= 0:
        return [{"success": False, "reason": "sold_out"}]

    # التحقق من عنوان العقد
    contract_address = detail.get("contract_address")
    if not contract_address:
        return [{"success": False, "reason": "no_contract_address"}]

    eth_price_usd = get_eth_price_usd()

    # جلب السعر من التفاصيل (بدون RPC)
    price_wei = fetch_price_fast(slug, detail)

    # ✅ التأكد من أن المينت مجاني
    if not is_free_mint(price_wei, eth_price_usd):
        price_usd = (price_wei / 1e18) * eth_price_usd
        log.info(f"⏭️ '{slug}' ليس مجانياً (${price_usd:.4f}) — يتم تجاهله")
        return None

    log.info(f"✅ '{slug}' مينت مجاني! جاري الشراء...")

    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]

    # المحافظ التي لم تشترِ بعد
    already_bought = successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in already_bought]

    if not pending_items:
        return [{"success": False, "reason": "all_wallets_completed"}]

    # إضافة تفاصيل السياق
    for item in pending_items:
        item["current_detail"] = detail
        item["chain_key"] = chain_key

    # تنفيذ الشراء بالتوازي
    tasks = [
        purchase_task_for_wallet(
            item, slug, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd
        )
        for item in pending_items
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # معالجة النتائج
    processed_results = []
    for r in results:
        if isinstance(r, Exception):
            processed_results.append({"success": False, "reason": f"exception: {str(r)}"})
        else:
            processed_results.append(r)
    
    return processed_results

# ==================== تقييم المينتات (محسن للسرعة) ====================

async def evaluate_new_mint(slug: str, chain_key: str):
    """تقييم المينت الجديد - سريع مع تخزين مؤقت"""
    
    # التحقق من الشروط المسبقة (سريع)
    if (len(successful_mints.get(slug, set())) >= len(WALLETS_DATA) or
        slug in watchlist or slug in in_flight or is_in_cooldown(slug)):
        return

    in_flight.add(slug)
    try:
        # ✅ جلب جميع البيانات بالتوازي (سريع)
        detail, price_wei, twitter_username = await asyncio.to_thread(
            fetch_all_data_parallel, slug
        )
        
        if not detail:
            return

        # التحقق من أن المينت نشط (سريع)
        if not detail.get("is_minting"):
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return

        # ✅ التحقق من السعر (بدون RPC)
        eth_price_usd = get_eth_price_usd()
        
        if not is_free_mint(price_wei, eth_price_usd):
            price_usd = (price_wei / 1e18) * eth_price_usd
            log.info(f"⏭️ '{slug}' مدفوع (${price_usd:.4f}) — يتم تجاهله (نشتري مجاني فقط)")
            mark_rejected(slug)
            return

        log.info(f"💰 '{slug}' مينت مجاني! جاري التحقق من تويتر...")

        # ✅ التحقق من تويتر (من التخزين المؤقت)
        if not twitter_username:
            log.info(f"⏭️ '{slug}': لا يوجد حساب X مربوط — يتم تجاهله")
            mark_rejected(slug)
            return

        log.info(f"✅ '{slug}': مينت مجاني مع حساب X (@{twitter_username}) — جاري الشراء!")

        # إرسال إشعار باكتشاف مينت مجاني
        broadcast_message(build_free_mint_detected_message(detail, twitter_username))

        # تنفيذ الشراء
        results = await try_buy_now_multi_wallet(slug, chain_key, detail)

        if results is None:
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            return

        # تحديث حالة المينت
        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}

    except Exception as e:
        log.error(f"❌ خطأ بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)

# ==================== حلقة المراقبة ====================

async def watch_loop():
    """حلقة مراقبة المينتات - يحاول الشراء مرة أخرى"""
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
                
                # جلب تفاصيل محدثة (من التخزين المؤقت)
                found, fresh_detail = await asyncio.to_thread(fetch_drop_detail_fast, slug)

                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    watchlist.pop(slug, None)
                    broadcast_message(build_gaveup_message(entry["detail"], "المينت لم يعد نشطًا."))
                    continue

                stage = fresh_detail.get("active_stage")
                if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
                    watchlist.pop(slug, None)
                    broadcast_message(build_gaveup_message(fresh_detail, "انتهت المرحلة."))
                    continue

                # محاولة الشراء مرة أخرى
                results = await try_buy_now_multi_wallet(slug, chain_key, fresh_detail)

                if results is None:
                    watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}
                    continue

                if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                    watchlist.pop(slug, None)
                else:
                    watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}

            except Exception as e:
                log.error(f"❌ خطأ بدورة مراقبة '{slug}': {e}")
            finally:
                in_flight.discard(slug)

# ==================== الاتصال بـ OpenSea Stream ====================

async def listen_opensea():
    """الاستماع لتدفق المينتات من OpenSea"""
    msg_ref = 0
    detected_count = 0
    
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=10) as ws:
                log.info(f"✅ متصل بـ OpenSea Stream — يراقب لـ {len(WALLETS_DATA)} محافظ.")
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()

                while True:
                    # إرسال نبضات القلب
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
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
                    
                    # التحقق من أن المصدر هو العقد الصفري (مينت جديد)
                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    item = payload.get("item", {}) or {}
                    stream_chain_name = (item.get("chain", {}) or {}).get("name", "")
                    chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
                    if chain_key is None:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug:
                        continue

                    detected_count += 1
                    log.info(f"🔔 تم اكتشاف مينت جديد #{detected_count}: {slug} على {chain_key}")

                    # تشغيل تقييم المينت (غير متزامن)
                    asyncio.create_task(evaluate_new_mint(slug, chain_key))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"⚠️ انقطع الاتصال ({e}). إعادة الاتصال...")
            await asyncio.sleep(2)
        except Exception as e:
            log.error(f"❌ خطأ غير متوقع: {e}.")
            await asyncio.sleep(3)

# ==================== التشغيل الرئيسي ====================

async def run():
    """تشغيل النظام الرئيسي"""
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        broadcast_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false).")
        await telegram_sender()
        return

    # تحميل البيانات المحفوظة
    load_successful_mints()
    
    # إرسال رسالة التشغيل
    broadcast_message(f"✅ تم تشغيل البوت بنجاح!")
    broadcast_message(f"💰 النظام يشتري فقط المينتات المجانية مع حساب X")
    broadcast_message(f"⚡ وضع السرعة: بدون RPC، مع تخزين مؤقت")
    
    # تشغيل المهام المتوازية
    await asyncio.gather(
        listen_opensea(),
        watch_loop(),
        telegram_sender(),
        return_exceptions=True
    )

def main():
    """الدالة الرئيسية مع إعادة الاتصال"""
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("🛑 تم الإيقاف يدويًا.")
            save_successful_mints()
            break
        except Exception as e:
            log.critical(f"❌ توقف غير متوقع: {e}.")
            save_successful_mints()
            time.sleep(backoff)
            backoff = min(backoff * 2, 20)
            continue
        else:
            break

if __name__ == "__main__":
    main()
