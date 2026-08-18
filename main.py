"""
النظام الكامل - بوت شراء تلقائي متعدد المحافظ مع:
- 10 محافظ، لكل محفظة بوت تيليجرام خاص
- مراقبة المينتات المجانية والمدفوعة
- شراء تلقائي عند تحول المدفوع إلى مجاني
- حد أقصى للغاز 5 سنتات
- تخزين الحالة في SQLite
- التحقق من حساب X والموقع الإلكتروني
- إشعارات محسنة مع تفاصيل كاملة
- حد السعر المجاني 0.0001 دولار
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
    MAX_GAS_FEE_USD,
    is_free_or_negligible,
    FREE_PRICE_THRESHOLD_USD,
)

load_dotenv()

# ======================== إعدادات البيئة ========================

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"
CHECK_WEBSITE = os.environ.get("CHECK_WEBSITE", "false").lower() == "true"  # تفعيل التحقق من الموقع

# المحافظ والمفاتيح
PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]
TELEGRAM_BOT_TOKENS = [t.strip() for t in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if t.strip()]
TELEGRAM_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

if not (len(PRIVATE_KEYS) == len(WALLETS) == len(TELEGRAM_BOT_TOKENS) == len(TELEGRAM_CHAT_IDS)):
    raise ValueError("أعداد المفاتيح، المحافظ، توكنات البوتات، و Chat IDs غير متطابقة!")

WALLETS_DATA = [
    {
        "wallet": WALLETS[i],
        "private_key": PRIVATE_KEYS[i],
        "bot_token": TELEGRAM_BOT_TOKENS[i],
        "chat_id": TELEGRAM_CHAT_IDS[i],
    }
    for i in range(len(WALLETS))
]

# ======================== إعدادات الشبكات ========================

ALCHEMY_API_KEY_ROBINHOOD = os.environ["ALCHEMY_API_KEY"]
ALCHEMY_API_KEY_ETHEREUM = os.environ["ALCHEMY_API_KEY_ETHEREUM"]

CHAIN_CONFIGS = {
    "robinhood": {
        "stream_chain_name": "robinhood",
        "rpc_url": f"https://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ROBINHOOD}",
        "max_gas_fee_usd": MAX_GAS_FEE_USD,
        "label": "🟣 Robinhood Chain",
        "emoji": "🟣"
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ETHEREUM}",
        "max_gas_fee_usd": MAX_GAS_FEE_USD,
        "label": "🔵 Ethereum Mainnet",
        "emoji": "🔵"
    },
}

W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# ======================== الثوابت ========================

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
WATCH_POLL_INTERVAL_SECONDS = 15
REJECTION_COOLDOWN_SECONDS = 120
DB_FILE = "mint_bot_state.db"

# ======================== إعدادات التسجيل ========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

# ======================== المتغيرات العالمية ========================

send_queue: asyncio.Queue[Dict[str, str]] = asyncio.Queue()
successful_mints: Dict[str, Set[str]] = {}
watchlist: Dict[str, Dict[str, Any]] = {}
in_flight: Set[str] = set()
rejected_cooldown: Dict[str, float] = {}
shutdown_event = asyncio.Event()
_eth_price_cache = {"value": None, "ts": 0}

# ======================== إدارة قاعدة البيانات ========================

def init_database():
    """تهيئة قاعدة البيانات"""
    with closing(sqlite3.connect(DB_FILE)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS successful_mints (
                    slug TEXT,
                    wallet_address TEXT,
                    tx_hash TEXT,
                    quantity INTEGER,
                    chain_key TEXT,
                    gas_fee_usd REAL,
                    timestamp INTEGER,
                    PRIMARY KEY (slug, wallet_address)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlist (
                    slug TEXT PRIMARY KEY,
                    chain_key TEXT,
                    detail_json TEXT,
                    timestamp INTEGER
                )
            """)
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
            cursor.execute("SELECT slug, wallet_address FROM successful_mints")
            for slug, wallet in cursor.fetchall():
                if slug not in successful_mints:
                    successful_mints[slug] = set()
                successful_mints[slug].add(wallet)
            
            cursor.execute("SELECT slug, chain_key, detail_json FROM watchlist")
            for slug, chain_key, detail_json in cursor.fetchall():
                try:
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": json.loads(detail_json)
                    }
                except:
                    pass
            
            cursor.execute("SELECT slug, timestamp FROM rejected_cooldown")
            for slug, timestamp in cursor.fetchall():
                if time.time() - timestamp < REJECTION_COOLDOWN_SECONDS:
                    rejected_cooldown[slug] = timestamp


def save_successful_mint(slug: str, wallet_address: str, tx_hash: str, quantity: int, chain_key: str, gas_fee_usd: float):
    """حفظ مينت ناجح"""
    with closing(sqlite3.connect(DB_FILE)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute("""
                INSERT OR REPLACE INTO successful_mints 
                (slug, wallet_address, tx_hash, quantity, chain_key, gas_fee_usd, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (slug, wallet_address, tx_hash, quantity, chain_key, gas_fee_usd, int(time.time())))
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


# ======================== دوال التحقق من الموقع و X ========================

def get_twitter_username_from_opensea(slug: str, api_key: str) -> Optional[str]:
    """الحصول على اسم حساب X (تويتر) من OpenSea"""
    try:
        url = f"https://api.opensea.io/api/v2/collections/{slug}"
        headers = {"x-api-key": api_key}
        resp = requests.get(url, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            collection = data.get("collection", {})
            
            twitter_username = collection.get("twitter_username")
            if twitter_username:
                return twitter_username
            
            external_url = collection.get("external_url", "")
            if "twitter.com" in external_url or "x.com" in external_url:
                parts = external_url.split("/")
                if parts:
                    return parts[-1]
        
        return None
    except Exception as e:
        log.warning(f"[X] خطأ لجلب حساب X لـ {slug}: {e}")
        return None


def get_website_from_opensea(slug: str, api_key: str) -> Optional[str]:
    """الحصول على الموقع الإلكتروني من OpenSea"""
    try:
        url = f"https://api.opensea.io/api/v2/collections/{slug}"
        headers = {"x-api-key": api_key}
        resp = requests.get(url, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            collection = data.get("collection", {})
            
            website = collection.get("external_url")
            if website and website.startswith(("http://", "https://")):
                return website
            
            project_website = collection.get("project_website")
            if project_website and project_website.startswith(("http://", "https://")):
                return project_website
        
        return None
    except Exception as e:
        log.warning(f"[الموقع] خطأ لجلب الموقع لـ {slug}: {e}")
        return None


# ======================== الدوال المساعدة ========================

def get_eth_price_usd() -> float:
    """الحصول على سعر ETH مع التخزين المؤقت"""
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


def is_in_cooldown(slug: str) -> bool:
    """التحقق من أن العنصر في فترة التبريد"""
    ts = rejected_cooldown.get(slug)
    if ts is None:
        return False
    if time.time() - ts >= REJECTION_COOLDOWN_SECONDS:
        rejected_cooldown.pop(slug, None)
        remove_rejected_cooldown(slug)
        return False
    return True


def mark_rejected(slug: str):
    """تحديد عنصر كـ مرفوض"""
    rejected_cooldown[slug] = time.time()
    save_rejected_cooldown(slug)


def fetch_drop_detail(slug: str) -> tuple[Optional[bool], Optional[Dict[str, Any]]]:
    """جلب تفاصيل المينت من OpenSea"""
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
    """التحقق من أن المينت بدأ اليوم"""
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


# ======================== رسائل التيليجرام المحسنة ========================

def enqueue_message(bot_token: str, chat_id: str, text: str):
    """إضافة إشعار جديد"""
    send_queue.put_nowait({"bot_token": bot_token, "chat_id": chat_id, "text": text})


def broadcast_message(text: str):
    """إرسال إشعار لجميع المحافظ"""
    for w in WALLETS_DATA:
        enqueue_message(w["bot_token"], w["chat_id"], text)


def build_success_msg(detail: Dict[str, Any], result: Dict[str, Any], chain_key: str, twitter: str = None, website: str = None) -> str:
    """بناء رسالة نجاح الشراء - نسخة محسنة"""
    name = detail.get("collection_name") or detail.get("collection_slug") or "غير معروف"
    url = detail.get("opensea_url", "")
    chain_label = CHAIN_CONFIGS[chain_key]["label"]
    chain_emoji = CHAIN_CONFIGS[chain_key]["emoji"]
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    
    # حساب سعر التوكن بالدولار
    price_usd = (result.get('total_value_wei', 0) / 1e18) * get_eth_price_usd() if result.get('total_value_wei') else 0
    
    msg = (
        f"🟢 <b>✅ تم الشراء بنجاح!</b>\n"
        f"{'═' * 30}\n"
        f"{chain_emoji} <b>السلسلة:</b> {chain_label}\n"
        f"🆔 <b>المجموعة:</b> <a href='{url}'>{name}</a>\n"
        f"👛 <b>المحفظة:</b> <code>{w_short}</code>\n"
        f"📦 <b>الكمية:</b> {result['quantity']}\n"
        f"💰 <b>سعر التوكن:</b> ${price_usd:.6f}\n"
        f"⛽ <b>رسوم الغاز:</b> ${result['gas_fee_usd']:.4f} (حد: ${MAX_GAS_FEE_USD})\n"
        f"📊 <b>وحدات الغاز:</b> {result.get('gas_units', 'N/A')}\n"
    )
    
    if twitter:
        msg += f"🐦 <b>X:</b> @{twitter}\n"
    
    if website:
        msg += f"🌐 <b>الموقع:</b> <a href='{website}'>{website}</a>\n"
    
    msg += (
        f"🔗 <b>المعاملة:</b> <code>{result['tx_hash'][:10]}...{result['tx_hash'][-8:]}</code>\n"
        f"{'═' * 30}"
    )
    
    return msg


def build_watching_msg(detail: Dict[str, Any], reason: str, price_usd: float = None, twitter: str = None, website: str = None) -> str:
    """بناء رسالة المراقبة - نسخة محسنة"""
    name = detail.get("collection_name") or detail.get("collection_slug") or "غير معروف"
    url = detail.get("opensea_url", "")
    
    msg = (
        f"👀 <b>🔍 جارٍ مراقبة المينت</b>\n"
        f"{'═' * 30}\n"
        f"🆔 <b>المجموعة:</b> <a href='{url}'>{name}</a>\n"
    )
    
    if price_usd is not None:
        msg += f"💰 <b>السعر الحالي:</b> ${price_usd:.6f}\n"
    
    msg += f"📝 <b>السبب:</b> {reason}\n"
    
    if twitter:
        msg += f"🐦 <b>X:</b> @{twitter}\n"
    
    if website:
        msg += f"🌐 <b>الموقع:</b> <a href='{website}'>{website}</a>\n"
    
    msg += (
        f"📊 <b>حد السعر المجاني:</b> ${FREE_PRICE_THRESHOLD_USD:.6f}\n"
        f"⏳ <b>الحالة:</b> في انتظار تحول السعر إلى مجاني\n"
        f"{'═' * 30}\n"
        f"💡 سيتم الشراء تلقائياً فور وصول السعر إلى ${FREE_PRICE_THRESHOLD_USD:.6f} أو أقل"
    )
    
    return msg


def build_gaveup_msg(detail: Dict[str, Any], reason: str, twitter: str = None, website: str = None) -> str:
    """بناء رسالة انتهاء الفرصة - نسخة محسنة"""
    name = detail.get("collection_name") or detail.get("collection_slug") or "غير معروف"
    url = detail.get("opensea_url", "")
    
    msg = (
        f"🔴 <b>❌ انتهت الفرصة</b>\n"
        f"{'═' * 30}\n"
        f"🆔 <b>المجموعة:</b> <a href='{url}'>{name}</a>\n"
        f"📝 <b>السبب:</b> {reason}\n"
    )
    
    if twitter:
        msg += f"🐦 <b>X:</b> @{twitter}\n"
    
    if website:
        msg += f"🌐 <b>الموقع:</b> <a href='{website}'>{website}</a>\n"
    
    msg += f"{'═' * 30}\n"
    msg += f"⏰ تمت إزالة المشروع من قائمة المراقبة"
    
    return msg


def build_gas_alert_msg(slug: str, gas_fee_usd: float, twitter: str = None, website: str = None) -> str:
    """بناء رسالة تنبيه الغاز - نسخة محسنة"""
    msg = (
        f"⚠️ <b>⚡ تنبيه: رسوم الغاز مرتفعة!</b>\n"
        f"{'═' * 30}\n"
        f"🆔 <b>المشروع:</b> {slug}\n"
        f"⛽ <b>رسوم الغاز المقدرة:</b> ${gas_fee_usd:.4f}\n"
        f"⛔ <b>الحد الأقصى المسموح:</b> ${MAX_GAS_FEE_USD}\n"
    )
    
    if twitter:
        msg += f"🐦 <b>X:</b> @{twitter}\n"
    
    if website:
        msg += f"🌐 <b>الموقع:</b> <a href='{website}'>{website}</a>\n"
    
    msg += (
        f"{'═' * 30}\n"
        f"🛡️ تم تخطي المعاملة لحماية محفظتك من الرسوم المرتفعة"
    )
    
    return msg


def build_startup_msg() -> str:
    """بناء رسالة بدء التشغيل"""
    wallet_count = len(WALLETS_DATA)
    return (
        f"🚀 <b>تم تشغيل البوت بنجاح!</b>\n"
        f"{'═' * 30}\n"
        f"👛 <b>عدد المحافظ:</b> {wallet_count}\n"
        f"⛽ <b>حد الغاز الأقصى:</b> ${MAX_GAS_FEE_USD}\n"
        f"💰 <b>حد السعر المجاني:</b> ${FREE_PRICE_THRESHOLD_USD:.6f}\n"
        f"🔍 <b>التحقق من X:</b> ✅ مفعل\n"
        f"🌐 <b>التحقق من الموقع:</b> {'✅ مفعل' if CHECK_WEBSITE else '❌ معطل'}\n"
        f"{'═' * 30}\n"
        f"👀 جارٍ مراقبة المينتات التي يقل سعرها عن ${FREE_PRICE_THRESHOLD_USD:.6f}..."
    )


async def telegram_sender():
    """مهمة إرسال رسائل التيليجرام"""
    while not shutdown_event.is_set():
        try:
            msg = await asyncio.wait_for(send_queue.get(), timeout=1.0)
            try:
                api = f"https://api.telegram.org/bot{msg['bot_token']}"
                await asyncio.to_thread(
                    requests.post,
                    f"{api}/sendMessage",
                    data={
                        "chat_id": msg["chat_id"],
                        "text": msg["text"],
                        "parse_mode": "HTML",
                        "disable_web_page_preview": False
                    },
                    timeout=10,
                )
            except Exception as e:
                log.error(f"خطأ تليجرام: {e}")
            send_queue.task_done()
        except asyncio.TimeoutError:
            continue


# ======================== الشراء المتوازي ========================

async def purchase_task_for_wallet(
    w3, item: Dict[str, Any], slug: str, contract_address: str,
    price_wei: int, max_per_wallet: Optional[int], remaining: int,
    eth_price_usd: float, twitter: str = None, website: str = None
) -> Dict[str, Any]:
    """مهمة شراء لمحفظة واحدة"""
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
            w3, pk, wallet_addr, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd, 0
        )

        if res.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            
            await asyncio.to_thread(
                save_successful_mint,
                slug, wallet_addr, res["tx_hash"],
                res["quantity"], item.get("chain_key", ""),
                res.get("gas_fee_usd", 0.0)
            )
            
            msg = build_success_msg(
                item.get("current_detail", {}),
                res,
                item.get("chain_key", ""),
                twitter,
                website
            )
            enqueue_message(bot_token, chat_id, msg)
            
        elif res.get("reason") in ["gas_price_too_high", "gas_exceeds_limit"]:
            msg = build_gas_alert_msg(slug, res.get("gas_fee_usd", 0.0), twitter, website)
            enqueue_message(bot_token, chat_id, msg)

        return res


async def try_buy_now_multi_wallet(
    slug: str,
    chain_key: str,
    detail: Dict[str, Any],
    twitter: str = None,
    website: str = None
) -> Optional[List[Dict[str, Any]]]:
    """محاولة شراء لكل المحافظ بالتوازي"""
    stage = detail.get("active_stage")
    if not stage:
        return None

    contract_address = detail.get("contract_address")
    if not contract_address:
        return [{"success": False, "reason": "no_contract_address"}]

    w3 = W3_INSTANCES[chain_key]
    
    is_active, _ = await asyncio.to_thread(is_mint_active, w3, contract_address)
    if not is_active:
        return [{"success": False, "reason": "mint_not_active"}]

    total_supply = await asyncio.to_thread(get_total_supply, w3, contract_address)
    max_supply = int(detail.get("max_supply") or 0)
    remaining = max_supply - (total_supply or 0)
    if remaining <= 0:
        return [{"success": False, "reason": "sold_out"}]

    eth_price_usd = get_eth_price_usd()

    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

    # التحقق من السعر المجاني باستخدام الحد الجديد 0.0001
    if not is_free_or_negligible(price_wei, eth_price_usd):
        return None  # مدفوع → مراقبة

    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None

    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in successful_mints.get(slug, set())]
    if not pending_items:
        return [{"success": False, "reason": "all_wallets_completed"}]

    for item in pending_items:
        item["current_detail"] = detail
        item["chain_key"] = chain_key

    tasks = [
        purchase_task_for_wallet(
            w3, item, slug, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd,
            twitter, website
        )
        for item in pending_items
    ]

    return await asyncio.gather(*tasks)


# ======================== تقييم المينتات ========================

async def evaluate_new_mint(slug: str, chain_key: str):
    """تقييم مينت جديد مع التحقق من X والموقع"""
    if (
        len(successful_mints.get(slug, set())) >= len(WALLETS_DATA)
        or slug in watchlist
        or slug in in_flight
        or is_in_cooldown(slug)
    ):
        return

    in_flight.add(slug)
    try:
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not found or not detail or not detail.get("is_minting"):
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return

        # 1. التحقق من وجود حساب X (تويتر) أولاً
        twitter_username = await asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
        if not twitter_username:
            log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X")
            mark_rejected(slug)
            return

        # 2. التحقق من وجود موقع إلكتروني (إذا كان مفعلاً)
        website = None
        if CHECK_WEBSITE:
            website = await asyncio.to_thread(get_website_from_opensea, slug, OPENSEA_API_KEY)
            if not website:
                log.info(f"⏭️ تجاهل '{slug}': لا يوجد موقع إلكتروني")
                mark_rejected(slug)
                msg = build_watching_msg(detail, "لا يوجد موقع إلكتروني", twitter=twitter_username)
                broadcast_message(msg)
                return

        log.info(f"✅ '{slug}': X موجود (@{twitter_username})" + (f", الموقع: {website}" if website else ""))

        w3 = W3_INSTANCES[chain_key]
        eth_price_usd = get_eth_price_usd()
        contract_address = detail.get("contract_address")
        
        if contract_address:
            is_active, _ = await asyncio.to_thread(is_mint_active, w3, contract_address)
            if not is_active:
                return
            
            onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
            price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
            
            price_usd = (price_wei / 1e18) * eth_price_usd
            
            # إذا كان المينت مدفوعاً (أعلى من 0.0001 دولار)، ضعه في قائمة المراقبة
            if not is_free_or_negligible(price_wei, eth_price_usd):
                if slug not in watchlist:
                    watchlist[slug] = {"chain_key": chain_key, "detail": detail}
                    save_watchlist(slug, chain_key, detail)
                    msg = build_watching_msg(
                        detail,
                        f"السعر الحالي ${price_usd:.6f} - في انتظار الوصول إلى ${FREE_PRICE_THRESHOLD_USD:.6f} أو أقل",
                        price_usd,
                        twitter_username,
                        website
                    )
                    broadcast_message(msg)
                return

        # محاولة الشراء
        results = await try_buy_now_multi_wallet(slug, chain_key, detail, twitter_username, website)

        if results is None:
            # مدفوع → مراقبة
            if slug not in watchlist:
                watchlist[slug] = {"chain_key": chain_key, "detail": detail}
                save_watchlist(slug, chain_key, detail)
                msg = build_watching_msg(
                    detail,
                    f"في انتظار وصول السعر إلى ${FREE_PRICE_THRESHOLD_USD:.6f} أو أقل",
                    twitter=twitter_username,
                    website=website
                )
                broadcast_message(msg)
            return

        # تحديث قائمة المراقبة
        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            if slug not in watchlist:
                watchlist[slug] = {"chain_key": chain_key, "detail": detail}
                save_watchlist(slug, chain_key, detail)
        else:
            watchlist.pop(slug, None)
            remove_from_watchlist(slug)

    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)


# ======================== حلقة المراقبة ========================

async def watch_loop():
    """مراقبة المينتات المدفوعة"""
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
                    
                    twitter = await asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
                    website = await asyncio.to_thread(get_website_from_opensea, slug, OPENSEA_API_KEY) if CHECK_WEBSITE else None
                    
                    broadcast_message(build_gaveup_msg(entry["detail"], "المينت لم يعد نشطاً", twitter, website))
                    continue

                stage = fresh_detail.get("active_stage")
                if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
                    watchlist.pop(slug, None)
                    remove_from_watchlist(slug)
                    
                    twitter = await asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
                    website = await asyncio.to_thread(get_website_from_opensea, slug, OPENSEA_API_KEY) if CHECK_WEBSITE else None
                    
                    broadcast_message(build_gaveup_msg(fresh_detail, "انتهت المرحلة", twitter, website))
                    continue

                # جلب معلومات X والموقع للتحديث
                twitter = await asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
                website = await asyncio.to_thread(get_website_from_opensea, slug, OPENSEA_API_KEY) if CHECK_WEBSITE else None

                # محاولة الشراء مجدداً
                results = await try_buy_now_multi_wallet(slug, chain_key, fresh_detail, twitter, website)

                if results is None:
                    # لا يزال مدفوعاً
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
                log.error(f"خطأ بمراقبة '{slug}': {e}")
            finally:
                in_flight.discard(slug)


# ======================== الاتصال بـ OpenSea ========================

async def listen_opensea():
    """الاستماع لأحداث OpenSea"""
    msg_ref = 0
    while not shutdown_event.is_set():
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"✅ متصل بـ OpenSea - {len(WALLETS_DATA)} محافظ")
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
                    if slug:
                        asyncio.create_task(evaluate_new_mint(slug, chain_key))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            if not shutdown_event.is_set():
                log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال...")
                await asyncio.sleep(3)
        except Exception as e:
            if not shutdown_event.is_set():
                log.error(f"خطأ: {e}")
                await asyncio.sleep(5)


# ======================== التشغيل الرئيسي ========================

async def run():
    """تشغيل النظام"""
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false - وضع الانتظار")
        broadcast_message("🔴 البوت في وضع الإيقاف")
        await shutdown_event.wait()
        return

    # تهيئة قاعدة البيانات
    await asyncio.to_thread(init_database)
    await asyncio.to_thread(load_state_from_db)
    
    log.info(f"✅ تم التحميل: {len(successful_mints)} مينت, {len(watchlist)} مراقبة")
    log.info(f"💰 حد الغاز: ${MAX_GAS_FEE_USD}")
    log.info(f"💰 حد السعر المجاني: ${FREE_PRICE_THRESHOLD_USD:.6f}")
    log.info(f"🌐 التحقق من الموقع: {'مفعل' if CHECK_WEBSITE else 'معطل'}")
    
    broadcast_message(build_startup_msg())
    
    await asyncio.gather(
        listen_opensea(),
        watch_loop(),
        telegram_sender()
    )


def main():
    """الدالة الرئيسية"""
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدوياً")
            break
        except Exception as e:
            log.critical(f"توقف غير متوقع: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break


if __name__ == "__main__":
    main()
