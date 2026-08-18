"""
النظام الكامل — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها:
  - يكتشف مينتات اليوم على Robinhood + Ethereum
  - يشتري لجميع المحافظ المعرفة بالتوازي (Parallel Execution)
  - يرسل إشعار الشراء أو التحديث لكل محفظة على بوت التيليجرام الخاص بها
  - دعم تخزين الحالة في SQLite لمنع فقدان البيانات عند إعادة التشغيل
"""

import asyncio
import json
import logging
import os
import time
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Set, List
from contextlib import closing

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_wallet_lock,
    get_total_supply,
    is_mint_active,
    MAX_RETRY_ATTEMPTS,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"
FREE_PRICE_THRESHOLD_USD = float(os.environ.get("FREE_PRICE_THRESHOLD_USD", "0.01"))

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
WATCH_POLL_INTERVAL_SECONDS = 15
REJECTION_COOLDOWN_SECONDS = 120
DB_FILE = "mint_bot_state.db"

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

# قائمة انتظار رسائل التيليجرام
send_queue: asyncio.Queue[Dict[str, str]] = asyncio.Queue()

# المتغيرات العالمية (يتم تهيئتها من قاعدة البيانات)
successful_mints: Dict[str, Set[str]] = {}  # slug -> set(wallet_address)
watchlist: Dict[str, Dict[str, Any]] = {}
in_flight: Set[str] = set()
rejected_cooldown: Dict[str, float] = {}

# كائنات التحكم
db_lock = asyncio.Lock()
shutdown_event = asyncio.Event()


# ======================== إدارة قاعدة البيانات ========================

def init_database():
    """تهيئة قاعدة البيانات وإنشاء الجداول إذا لم تكن موجودة"""
    with closing(sqlite3.connect(DB_FILE)) as conn:
        with closing(conn.cursor()) as cursor:
            # جدول المينتات الناجحة
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS successful_mints (
                    slug TEXT,
                    wallet_address TEXT,
                    tx_hash TEXT,
                    quantity INTEGER,
                    chain_key TEXT,
                    timestamp INTEGER,
                    PRIMARY KEY (slug, wallet_address)
                )
            """)
            
            # جدول قائمة المراقبة
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlist (
                    slug TEXT PRIMARY KEY,
                    chain_key TEXT,
                    detail_json TEXT,
                    timestamp INTEGER
                )
            """)
            
            # جدول التبريد
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rejected_cooldown (
                    slug TEXT PRIMARY KEY,
                    timestamp INTEGER
                )
            """)
            
            conn.commit()


def load_state_from_db():
    """تحميل الحالة من قاعدة البيانات"""
    global successful_mints, watchlist, rejected_cooldown
    
    with closing(sqlite3.connect(DB_FILE)) as conn:
        with closing(conn.cursor()) as cursor:
            # تحميل المينتات الناجحة
            cursor.execute("SELECT slug, wallet_address FROM successful_mints")
            for slug, wallet in cursor.fetchall():
                if slug not in successful_mints:
                    successful_mints[slug] = set()
                successful_mints[slug].add(wallet)
            
            # تحميل قائمة المراقبة
            cursor.execute("SELECT slug, chain_key, detail_json FROM watchlist")
            for slug, chain_key, detail_json in cursor.fetchall():
                try:
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": json.loads(detail_json)
                    }
                except:
                    pass
            
            # تحميل قائمة التبريد
            cursor.execute("SELECT slug, timestamp FROM rejected_cooldown")
            for slug, timestamp in cursor.fetchall():
                # فقط إذا لم تنتهي فترة التبريد
                if time.time() - timestamp < REJECTION_COOLDOWN_SECONDS:
                    rejected_cooldown[slug] = timestamp


def save_successful_mint(slug: str, wallet_address: str, tx_hash: str, quantity: int, chain_key: str):
    """حفظ مينت ناجح في قاعدة البيانات"""
    with closing(sqlite3.connect(DB_FILE)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute("""
                INSERT OR REPLACE INTO successful_mints 
                (slug, wallet_address, tx_hash, quantity, chain_key, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (slug, wallet_address, tx_hash, quantity, chain_key, int(time.time())))
            conn.commit()


def save_watchlist(slug: str, chain_key: str, detail: Dict[str, Any]):
    """حفظ عنصر في قائمة المراقبة"""
    with closing(sqlite3.connect(DB_FILE)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute("""
                INSERT OR REPLACE INTO watchlist (slug, chain_key, detail_json, timestamp)
                VALUES (?, ?, ?, ?)
            """, (slug, chain_key, json.dumps(detail), int(time.time())))
            conn.commit()


def remove_from_watchlist(slug: str):
    """إزالة عنصر من قائمة المراقبة"""
    with closing(sqlite3.connect(DB_FILE)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute("DELETE FROM watchlist WHERE slug = ?", (slug,))
            conn.commit()


def save_rejected_cooldown(slug: str):
    """حفظ عنصر في قائمة التبريد"""
    with closing(sqlite3.connect(DB_FILE)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute("""
                INSERT OR REPLACE INTO rejected_cooldown (slug, timestamp)
                VALUES (?, ?)
            """, (slug, int(time.time())))
            conn.commit()


def remove_rejected_cooldown(slug: str):
    """إزالة عنصر من قائمة التبريد"""
    with closing(sqlite3.connect(DB_FILE)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute("DELETE FROM rejected_cooldown WHERE slug = ?", (slug,))
            conn.commit()


# ======================== الوظائف المساعدة ========================

def is_in_cooldown(slug: str) -> bool:
    """التحقق من أن العنصر في فترة التبريد"""
    ts = rejected_cooldown.get(slug)
    if ts is None:
        return False
    if time.time() - ts >= REJECTION_COOLDOWN_SECONDS:
        rejected_cooldown.pop(slug, None)
        # حذف من قاعدة البيانات أيضاً
        remove_rejected_cooldown(slug)
        return False
    return True


def mark_rejected(slug: str):
    """تحديد عنصر كـ مرفوض ووضعه في التبريد"""
    rejected_cooldown[slug] = time.time()
    save_rejected_cooldown(slug)


_eth_price_cache = {"value": None, "ts": 0}


def get_eth_price_usd() -> float:
    """الحصول على سعر ETH بالدولار مع التخزين المؤقت"""
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


def fetch_drop_detail(slug: str) -> tuple[Optional[bool], Optional[Dict[str, Any]]]:
    """جلب تفاصيل المينت من OpenSea API"""
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            return True, resp.json()
        if resp.status_code == 404:
            return False, None
        return None, None
    except Exception as e:
        log.warning(f"[Drops API] خطأ: {e}")
        return None, None


def parse_iso(ts: str) -> Optional[datetime]:
    """تحويل سلسلة ISO إلى datetime"""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def started_today_local(stage: Dict[str, Any]) -> bool:
    """التحقق من أن المينت بدأ اليوم بالتوقيت المحلي"""
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()


def stage_has_ended(stage: Dict[str, Any]) -> bool:
    """التحقق من انتهاء المرحلة"""
    end = parse_iso(stage.get("end_time", ""))
    if not end:
        return False
    return datetime.now(timezone.utc) > end


def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    """التحقق من أن السعر مجاني أو لا يُذكر"""
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD


# ======================== إدارة رسائل التيليجرام ========================

def enqueue_message(bot_token: str, chat_id: str, text: str):
    """إضافة إشعار جديد مع تحديد البوت والمستلم"""
    send_queue.put_nowait({
        "bot_token": bot_token,
        "chat_id": chat_id,
        "text": text
    })


def broadcast_message(text: str):
    """إرسال إشعار عام لجميع البوتات المربوطة بالـ 10 محافظ"""
    for w in WALLETS_DATA:
        enqueue_message(w["bot_token"], w["chat_id"], text)


async def telegram_sender():
    """مهمة إرسال رسائل التيليجرام"""
    while not shutdown_event.is_set():
        try:
            msg = await asyncio.wait_for(send_queue.get(), timeout=1.0)
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
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            log.error(f"خطأ في telegram_sender: {e}")
            await asyncio.sleep(1)


def build_single_wallet_success_msg(detail: Dict[str, Any], result: Dict[str, Any], chain_key: str) -> str:
    """بناء رسالة نجاح الشراء لمحفظة واحدة"""
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


def build_watching_message(detail: Dict[str, Any], reason: str) -> str:
    """بناء رسالة المراقبة"""
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"👀 <b>تحت المراقبة لمحافظتك</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}\nسنحاول الشراء تلقائيًا فور توفر الفرصة."


def build_gaveup_message(detail: Dict[str, Any], reason: str) -> str:
    """بناء رسالة انتهاء الفرصة"""
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"❌ <b>انتهت الفرصة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}"


# ======================== الشراء المتوازي ========================

async def purchase_task_for_wallet(
    w3, item: Dict[str, Any], slug: str, contract_address: str,
    price_wei: int, max_per_wallet: Optional[int], remaining: int,
    eth_price_usd: float, max_gas_fee_usd: float
) -> Dict[str, Any]:
    """مهمة شراء لمحفظة واحدة"""
    wallet_addr = item["wallet"]
    pk = item["private_key"]
    bot_token = item["bot_token"]
    chat_id = item["chat_id"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        # التحقق من أن المحفظة لم تشترِ بالفعل
        if wallet_addr in successful_mints.get(slug, set()):
            return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

        # محاولة الشراء
        res = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            w3, pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd,
            0  # retry_count
        )

        if res.get("success"):
            # تحديث الحالة في الذاكرة وفي قاعدة البيانات
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            
            # حفظ في قاعدة البيانات
            await asyncio.to_thread(
                save_successful_mint,
                slug, wallet_addr, res["tx_hash"],
                res["quantity"], item.get("chain_key", "")
            )
            
            # إرسال إشعار النجاح فقط للبوت المربوط بهذه المحفظة
            msg = build_single_wallet_success_msg(
                item.get("current_detail", {}), res, item.get("chain_key", "")
            )
            enqueue_message(bot_token, chat_id, msg)

        return res


async def try_buy_now_multi_wallet(slug: str, chain_key: str, detail: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """محاولة شراء لكل المحافظ بالتوازي"""
    stage = detail.get("active_stage")
    if not stage:
        return None

    # الحصول على العرض المتبقي من العقد
    contract_address = detail.get("contract_address")
    if not contract_address:
        return [{"success": False, "reason": "no_contract_address"}]

    w3 = W3_INSTANCES[chain_key]
    
    # التحقق من أن المينت لا يزال نشطاً على السلسلة
    is_active, end_time = await asyncio.to_thread(is_mint_active, w3, contract_address)
    if not is_active:
        return [{"success": False, "reason": "mint_not_active_on_chain"}]

    # الحصول على العرض المتبقي
    total_supply = await asyncio.to_thread(get_total_supply, w3, contract_address)
    max_supply = int(detail.get("max_supply") or 0)
    remaining = max_supply - (total_supply or 0)
    if remaining <= 0:
        return [{"success": False, "reason": "sold_out"}]

    eth_price_usd = get_eth_price_usd()

    # الحصول على السعر
    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

    # التحقق من أن السعر مجاني
    if not is_free_or_negligible(price_wei, eth_price_usd):
        return None  # مدفوع -> للمراقبة

    # الحصول على الحد الأقصى للشراء
    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]

    # تحديد المحافظ التي لم تشترِ بعد
    already_bought_wallets = successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in already_bought_wallets]

    if not pending_items:
        return [{"success": False, "reason": "all_wallets_completed"}]

    # إلحاق تفاصيل السياق للطلب
    for item in pending_items:
        item["current_detail"] = detail
        item["chain_key"] = chain_key

    # تنفيذ مهام الشراء بالتوازي
    tasks = [
        purchase_task_for_wallet(
            w3, item, slug, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd
        )
        for item in pending_items
    ]

    results = await asyncio.gather(*tasks)
    return list(results)


# ======================== تقييم المينتات وإدارة المراقبة ========================

async def evaluate_new_mint(slug: str, chain_key: str):
    """تقييم مينت جديد ومعالجته"""
    # التحقق من الشروط المسبقة
    if (
        len(successful_mints.get(slug, set())) >= len(WALLETS_DATA)
        or slug in watchlist
        or slug in in_flight
        or is_in_cooldown(slug)
    ):
        return

    in_flight.add(slug)
    try:
        # جلب تفاصيل المينت
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not found or not detail or not detail.get("is_minting"):
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return

        # التحقق من أن المينت مجاني قبل فحص تويتر لتوفير API Requests
        w3 = W3_INSTANCES[chain_key]
        eth_price_usd = get_eth_price_usd()
        contract_address = detail.get("contract_address")
        
        if contract_address:
            # التحقق من نشاط المينت على السلسلة
            is_active, _ = await asyncio.to_thread(is_mint_active, w3, contract_address)
            if not is_active:
                log.info(f"⏭️ تجاهل '{slug}': المينت غير نشط على السلسلة")
                return
            
            # التحقق من السعر
            onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
            price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
            
            if not is_free_or_negligible(price_wei, eth_price_usd):
                # إذا كان مدفوعاً، نضعه في قائمة المراقبة بدلاً من تجاهله
                if slug not in watchlist:
                    watchlist[slug] = {"chain_key": chain_key, "detail": detail}
                    save_watchlist(slug, chain_key, detail)
                    broadcast_message(build_watching_message(detail, "السعر الحالي مدفوع — تحت المراقبة."))
                return

        # فحص عبر X: يكفي وجود حساب X مربوط بالمجموعة
        twitter_username = await asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
        if not twitter_username:
            log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X مربوط.")
            mark_rejected(slug)
            return

        log.info(f"✅ '{slug}': يوجد حساب X مربوط (@{twitter_username}) — المتابعة للشراء.")

        # تنفيذ الشراء التلقائي
        results = await try_buy_now_multi_wallet(slug, chain_key, detail)

        if results is None:
            # مدفوع -> مراقبة
            if slug not in watchlist:
                watchlist[slug] = {"chain_key": chain_key, "detail": detail}
                save_watchlist(slug, chain_key, detail)
                broadcast_message(build_watching_message(detail, "السعر الحالي مدفوع — تحت المراقبة."))
            return

        # تحديث قائمة المراقبة إذا لم تشترِ جميع المحافظ
        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            if slug not in watchlist:
                watchlist[slug] = {"chain_key": chain_key, "detail": detail}
                save_watchlist(slug, chain_key, detail)
        else:
            # إذا اشترت جميع المحافظ، نزيل من قائمة المراقبة
            if slug in watchlist:
                watchlist.pop(slug, None)
                remove_from_watchlist(slug)

    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)


async def watch_loop():
    """حلقة مراقبة المينتات التي لم تشترِ بعد"""
    while not shutdown_event.is_set():
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        if not watchlist:
            continue

        for slug in list(watchlist.keys()):
            if slug in in_flight or len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                watchlist.pop(slug, None)
                remove_from_watchlist(slug)
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
                    remove_from_watchlist(slug)
                    broadcast_message(build_gaveup_message(entry["detail"], "المينت لم يعد نشطًا."))
                    continue

                stage = fresh_detail.get("active_stage")
                if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
                    watchlist.pop(slug, None)
                    remove_from_watchlist(slug)
                    broadcast_message(build_gaveup_message(fresh_detail, "انتهت المرحلة."))
                    continue

                # محاولة الشراء مجدداً
                results = await try_buy_now_multi_wallet(slug, chain_key, fresh_detail)

                if results is None:
                    # لا يزال مدفوعاً، استمر في المراقبة
                    watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}
                    save_watchlist(slug, chain_key, fresh_detail)
                    continue

                if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                    watchlist.pop(slug, None)
                    remove_from_watchlist(slug)
                else:
                    watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}
                    save_watchlist(slug, chain_key, fresh_detail)

            except Exception as e:
                log.error(f"خطأ بدورة مراقبة '{slug}': {e}")
            finally:
                in_flight.discard(slug)


async def listen_opensea():
    """الاستماع إلى أحداث OpenSea Stream"""
    msg_ref = 0
    while not shutdown_event.is_set():
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"متصل بـ OpenSea Stream — يراقب لـ {len(WALLETS_DATA)} محافظ.")
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()

                while not shutdown_event.is_set():
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

                    asyncio.create_task(evaluate_new_mint(slug, chain_key))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            if not shutdown_event.is_set():
                log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال...")
                await asyncio.sleep(3)
        except Exception as e:
            if not shutdown_event.is_set():
                log.error(f"خطأ غير متوقع: {e}.")
                await asyncio.sleep(5)


async def shutdown_handler():
    """معالجة إيقاف التشغيل بشكل نظيف"""
    await asyncio.sleep(1)  # انتظار بدء التشغيل
    try:
        while True:
            await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        log.info("جارٍ إيقاف التشغيل بشكل نظيف...")
        shutdown_event.set()


async def run():
    """تشغيل النظام الرئيسي"""
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false - البوت في وضع الانتظار")
        broadcast_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false).")
        # الانتظار إلى أجل غير مسمى بدلاً من إنهاء التشغيل
        await shutdown_event.wait()
        return

    # تهيئة قاعدة البيانات وتحميل الحالة
    await asyncio.to_thread(init_database)
    await asyncio.to_thread(load_state_from_db)
    
    log.info(f"✅ تم تحميل الحالة: {len(successful_mints)} مينت ناجح، {len(watchlist)} تحت المراقبة")
    broadcast_message("✅ تم تشغيل المحفظة الخاصة بك بنجاح وربطها بهذا البوت!")
    
    # تشغيل المهام الرئيسية
    await asyncio.gather(
        listen_opensea(),
        watch_loop(),
        telegram_sender(),
        shutdown_handler()
    )


def main():
    """الدالة الرئيسية مع إعادة المحاولة التلقائية"""
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
