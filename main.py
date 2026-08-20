"""
النظام الكامل — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها:
  - يكتشف مينتات اليوم على Robinhood + Ethereum
  - يشتري لجميع المحافظ المعرفة بالتوازي (Parallel Execution)
  - يرسل إشعار الشراء أو التحديث لكل محفظة على بوت التيليجرام الخاص بها
  - يدعم EIP-1559، إعادة المحاولة، تخزين دائم، وإدارة أفضل للأخطاء
  - كشف دقيق للمينتات مع فلترة متقدمة
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Set, List, Any, Tuple

import requests
import websockets
from dotenv import load_dotenv
from web3 import Web3
from web3.exceptions import TransactionNotFound, ContractLogicError

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_wallet_lock,
    send_transaction_with_retry,
    WalletData,
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
WALLETS_DATA: List[WalletData] = []
for i in range(len(WALLETS)):
    WALLETS_DATA.append(WalletData(
        wallet=WALLETS[i],
        private_key=PRIVATE_KEYS[i],
        bot_token=TELEGRAM_BOT_TOKENS[i],
        chat_id=TELEGRAM_CHAT_IDS[i],
    ))

ALCHEMY_API_KEY_ROBINHOOD = os.environ["ALCHEMY_API_KEY"]
ALCHEMY_API_KEY_ETHEREUM = os.environ["ALCHEMY_API_KEY_ETHEREUM"]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"
COLLECTIONS_API = "https://api.opensea.io/api/v2/collections"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

# ==================== إعدادات الأداء ====================
HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 15
REJECTION_COOLDOWN_SECONDS = 120
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2
SAVE_INTERVAL_SECONDS = 60  # حفظ التقدم كل دقيقة
MIN_TWITTER_FOLLOWERS = 100  # الحد الأدنى لمتابعي X (اختياري)
MAX_MINT_AGE_SECONDS = 3600  # أقصى عمر للمينت (ساعة واحدة)

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
        "rpc_url": f"https://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ROBINHOOD}",
        "max_gas_fee_usd": 0.05,
        "chain_id": 6900,
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ETHEREUM}",
        "max_gas_fee_usd": 0.50,
        "chain_id": 1,
    },
}

W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# ==================== التخزين الدائم ====================
SUCCESS_FILE = Path("successful_mints.json")
WATCHLIST_FILE = Path("watchlist.json")
DETECTED_MINTS_FILE = Path("detected_mints.json")

# المتغيرات العامة
successful_mints: Dict[str, Set[str]] = {}
watchlist: Dict[str, Dict] = {}
detected_mints: Dict[str, Dict] = {}  # تتبع المينتات المكتشفة
in_flight: Set[str] = set()
rejected_cooldown: Dict[str, float] = {}
_eth_price_cache = {"value": None, "ts": 0}

# ==================== دوال الكشف المتقدمة ====================

def load_persistent_data():
    """تحميل جميع البيانات من الملفات"""
    global successful_mints, watchlist, detected_mints
    
    # تحميل المينتات الناجحة
    if SUCCESS_FILE.exists():
        try:
            with open(SUCCESS_FILE, 'r') as f:
                data = json.load(f)
                successful_mints = {k: set(v) for k, v in data.items()}
                log.info(f"✅ تم تحميل {len(successful_mints)} مينت ناجح")
        except Exception as e:
            log.warning(f"⚠️ تعذر تحميل المينتات الناجحة: {e}")
            successful_mints = {}
    
    # تحميل قائمة المراقبة
    if WATCHLIST_FILE.exists():
        try:
            with open(WATCHLIST_FILE, 'r') as f:
                watchlist = json.load(f)
                log.info(f"✅ تم تحميل {len(watchlist)} مجموعة تحت المراقبة")
        except Exception as e:
            log.warning(f"⚠️ تعذر تحميل قائمة المراقبة: {e}")
            watchlist = {}
    
    # تحميل المينتات المكتشفة
    if DETECTED_MINTS_FILE.exists():
        try:
            with open(DETECTED_MINTS_FILE, 'r') as f:
                detected_mints = json.load(f)
                # تنظيف المينتات القديمة
                now = time.time()
                for slug in list(detected_mints.keys()):
                    if now - detected_mints[slug].get('detected_at', 0) > MAX_MINT_AGE_SECONDS:
                        del detected_mints[slug]
                log.info(f"✅ تم تحميل {len(detected_mints)} مينت مكتشف")
        except Exception as e:
            log.warning(f"⚠️ تعذر تحميل المينتات المكتشفة: {e}")
            detected_mints = {}

def save_persistent_data():
    """حفظ جميع البيانات إلى الملفات"""
    try:
        # حفظ المينتات الناجحة
        with open(SUCCESS_FILE, 'w') as f:
            json.dump({k: list(v) for k, v in successful_mints.items()}, f, indent=2)
        
        # حفظ قائمة المراقبة
        with open(WATCHLIST_FILE, 'w') as f:
            json.dump(watchlist, f, indent=2)
        
        # حفظ المينتات المكتشفة
        with open(DETECTED_MINTS_FILE, 'w') as f:
            json.dump(detected_mints, f, indent=2)
        
        log.debug("💾 تم حفظ البيانات بنجاح")
    except Exception as e:
        log.error(f"❌ خطأ في حفظ البيانات: {e}")

async def periodic_save():
    """حفظ البيانات بشكل دوري"""
    while True:
        await asyncio.sleep(SAVE_INTERVAL_SECONDS)
        save_persistent_data()

# ==================== دوال كشف المينتات ====================

def is_valid_mint_event(payload: dict) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    التحقق من صحة حدث المينت
    يعيد: (is_valid, chain_key, slug)
    """
    try:
        # 1. التحقق من أن المصدر هو العقد الصفري (مينت جديد)
        from_account = payload.get("from_account", {})
        from_address = from_account.get("address", "").lower()
        if from_address != ZERO_ADDRESS:
            return False, None, None
        
        # 2. التحقق من وجود المجموعة
        collection = payload.get("collection", {})
        slug = collection.get("slug", "")
        if not slug:
            return False, None, None
        
        # 3. التحقق من السلسلة
        item = payload.get("item", {})
        chain = item.get("chain", {})
        stream_chain_name = chain.get("name", "")
        chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
        if chain_key is None:
            return False, None, None
        
        # 4. التحقق من أن الكمية > 0
        quantity = payload.get("quantity", 0)
        if quantity <= 0:
            return False, None, None
        
        return True, chain_key, slug
        
    except Exception as e:
        log.debug(f"خطأ في التحقق من حدث المينت: {e}")
        return False, None, None

def get_mint_metadata(slug: str) -> Optional[Dict]:
    """جلب بيانات إضافية عن المينت من OpenSea"""
    try:
        # جلب تفاصيل المجموعة
        resp = requests.get(
            f"{COLLECTIONS_API}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            return {
                'name': data.get('name', slug),
                'description': data.get('description', ''),
                'image_url': data.get('image_url', ''),
                'opensea_url': data.get('opensea_url', ''),
                'total_supply': data.get('total_supply', 0),
                'floor_price': data.get('floor_price', 0),
            }
    except Exception as e:
        log.debug(f"خطأ في جلب بيانات المجموعة: {e}")
    return None

def is_high_value_mint(detail: dict) -> bool:
    """تقييم قيمة المينت"""
    try:
        # 1. التحقق من وجود حساب X
        twitter_username = detail.get('twitter_username', '')
        if not twitter_username:
            return False
        
        # 2. التحقق من الشعبية (إذا كان متاحاً)
        twitter_followers = detail.get('twitter_followers', 0)
        if twitter_followers < MIN_TWITTER_FOLLOWERS:
            return False
        
        # 3. التحقق من حجم المجموعة
        total_supply = int(detail.get('total_supply', 0))
        max_supply = int(detail.get('max_supply', 0))
        if max_supply > 0 and total_supply >= max_supply:
            return False  # اكتملت المجموعة
        
        # 4. التحقق من نشاط المينت
        if not detail.get('is_minting', False):
            return False
        
        return True
        
    except Exception as e:
        log.debug(f"خطأ في تقييم قيمة المينت: {e}")
        return False

# ==================== دوال مساعدة ====================

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
    """جلب سعر ETH من CoinGecko مع التخزين المؤقت"""
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
        log.warning(f"⚠️ [السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0

def fetch_drop_detail(slug: str) -> Tuple[Optional[bool], Optional[Dict]]:
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
        log.warning(f"⚠️ استجابة غير متوقعة من API: {resp.status_code}")
        return None, None
    except Exception as e:
        log.warning(f"⚠️ [Drops API] خطأ: {e}")
        return None, None

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

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    """التحقق من أن السعر مجاني أو زهيد"""
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

def detect_mint_quality(slug: str, detail: dict) -> Dict[str, Any]:
    """تقييم جودة المينت وإرجاع تفاصيل التقييم"""
    quality = {
        'score': 0,
        'has_twitter': False,
        'twitter_username': None,
        'has_high_supply': False,
        'is_active': False,
        'is_free': False,
        'reasons': []
    }
    
    try:
        # 1. التحقق من وجود تويتر
        twitter = detail.get('twitter_username')
        if twitter:
            quality['has_twitter'] = True
            quality['twitter_username'] = twitter
            quality['score'] += 30
            quality['reasons'].append(f"✅ لديه حساب X: @{twitter}")
        else:
            quality['reasons'].append("❌ لا يوجد حساب X")
        
        # 2. التحقق من العرض
        total_supply = int(detail.get('total_supply', 0))
        max_supply = int(detail.get('max_supply', 0))
        if max_supply > 1000:
            quality['has_high_supply'] = True
            quality['score'] += 20
            quality['reasons'].append(f"✅ عرض كبير: {max_supply} قطعة")
        else:
            quality['reasons'].append(f"⚠️ عرض صغير: {max_supply} قطعة")
        
        # 3. التحقق من النشاط
        if detail.get('is_minting', False):
            quality['is_active'] = True
            quality['score'] += 30
            quality['reasons'].append("✅ مينت نشط")
        else:
            quality['reasons'].append("❌ مينت غير نشط")
        
        # 4. التحقق من السعر
        stage = detail.get('active_stage', {})
        price = int(stage.get('price', '0'))
        if price == 0:
            quality['is_free'] = True
            quality['score'] += 20
            quality['reasons'].append("✅ مجاني")
        else:
            eth_price = get_eth_price_usd()
            price_usd = (price / 1e18) * eth_price
            quality['reasons'].append(f"⚠️ السعر: ${price_usd:.2f}")
        
        # 5. تقييم عام
        if quality['score'] >= 70:
            quality['reasons'].insert(0, "⭐️⭐️⭐️ ممتاز - مناسب للشراء")
        elif quality['score'] >= 50:
            quality['reasons'].insert(0, "⭐️⭐️ جيد - يمكن الشراء")
        else:
            quality['reasons'].insert(0, "⭐️ ضعيف - يفضل التخطي")
            
    except Exception as e:
        log.error(f"خطأ في تقييم المينت: {e}")
        quality['reasons'].append(f"❌ خطأ في التقييم: {e}")
    
    return quality

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
        enqueue_message(w.bot_token, w.chat_id, text)

def broadcast_mint_detection(slug: str, quality: Dict[str, Any], detail: dict):
    """إرسال إشعار باكتشاف مينت جديد مع التقييم"""
    name = detail.get('collection_name') or slug
    reasons = "\n".join(quality['reasons'])
    
    msg = (
        f"🔔 <b>تم اكتشاف مينت جديد!</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"الرابط: {detail.get('opensea_url', '')}\n\n"
        f"📊 <b>تقييم الجودة:</b>\n"
        f"{reasons}\n\n"
        f"النقاط: {quality['score']}/100"
    )
    
    broadcast_message(msg)

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
                timeout=10,
            )
        except Exception as e:
            log.error(f"❌ خطأ إرسال تليجرام: {e}")
        send_queue.task_done()
        await asyncio.sleep(0.1)

# ==================== دوال بناء الرسائل ====================

def build_single_wallet_success_msg(detail: dict, result: dict, chain_key: str) -> str:
    """بناء رسالة نجاح الشراء لمحفظة واحدة"""
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    chain_label = "Robinhood Chain" if chain_key == "robinhood" else "Ethereum Mainnet"
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    return (
        f"✅ <b>تم الشراء بنجاح لمحفظتك!</b> ({chain_label})\n\n"
        f"المحفظة: <code>{w_short}</code>\n"
        f"المجموعة: <b>{name}</b>\n"
        f"الكمية: {result['quantity']}\n"
        f"رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
        f"المعاملة: {result['tx_hash']}\n"
        f"🔗 {url}"
    )

def build_watching_message(detail: dict, reason: str) -> str:
    """بناء رسالة وضع المراقبة"""
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"👀 <b>تحت المراقبة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}\nسنحاول الشراء تلقائيًا فور توفر الفرصة."

def build_gaveup_message(detail: dict, reason: str) -> str:
    """بناء رسالة انتهاء الفرصة"""
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"❌ <b>انتهت الفرصة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}"

# ==================== الشراء المتوازي ====================

async def purchase_task_for_wallet(
    w3, item: WalletData, slug: str, contract_address: str, 
    price_wei: int, max_per_wallet: Optional[int], remaining: int, 
    eth_price_usd: float, max_gas_fee_usd: float
):
    """مهمة الشراء لمحفظة واحدة"""
    wallet_addr = item.wallet
    pk = item.private_key
    bot_token = item.bot_token
    chat_id = item.chat_id

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        # التحقق من الشراء المسبق
        if wallet_addr in successful_mints.get(slug, set()):
            return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

        # محاولة الشراء مع إعادة المحاولة
        res = await send_transaction_with_retry(
            w3, pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd,
            max_retries=MAX_RETRIES
        )

        if res.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            
            # إرسال إشعار النجاح
            msg = build_single_wallet_success_msg(
                item.current_detail, res, item.chain_key
            )
            enqueue_message(bot_token, chat_id, msg)
            
            # حفظ البيانات فوراً
            save_persistent_data()
            
            # تحديث المينتات المكتشفة
            if slug in detected_mints:
                detected_mints[slug]['purchased'] = True
                detected_mints[slug]['purchased_by'] = list(successful_mints[slug])

        return res

async def try_buy_now_multi_wallet(slug: str, chain_key: str, detail: dict) -> Optional[List[dict]]:
    """محاولة الشراء لجميع المحافظ بالتوازي"""
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

    # جلب السعر من العقد
    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

    # التحقق من السعر المجاني
    if not is_free_or_negligible(price_wei, eth_price_usd):
        return None  # مدفوع -> للمراقبة

    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]

    # المحافظ التي لم تشترِ بعد
    already_bought = successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item.wallet not in already_bought]

    if not pending_items:
        return [{"success": False, "reason": "all_wallets_completed"}]

    # إضافة تفاصيل السياق
    for item in pending_items:
        item.current_detail = detail
        item.chain_key = chain_key

    # تنفيذ الشراء بالتوازي مع معالجة الأخطاء
    tasks = [
        purchase_task_for_wallet(
            w3, item, slug, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd
        )
        for item in pending_items
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # معالجة النتائج التي قد تكون استثناءات
    processed_results = []
    for r in results:
        if isinstance(r, Exception):
            processed_results.append({"success": False, "reason": f"exception: {str(r)}"})
        else:
            processed_results.append(r)
    
    return processed_results

# ==================== تقييم المينتات ====================

async def evaluate_new_mint(slug: str, chain_key: str):
    """تقييم المينت الجديد وتنفيذ الشراء إذا كان مناسباً"""
    # التحقق من الشروط المسبقة
    if (len(successful_mints.get(slug, set())) >= len(WALLETS_DATA) or
        slug in watchlist or slug in in_flight or is_in_cooldown(slug)):
        return

    in_flight.add(slug)
    try:
        # 1. جلب تفاصيل المينت
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not found or not detail or not detail.get("is_minting"):
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return

        # 2. تحديث المينتات المكتشفة
        if slug not in detected_mints:
            detected_mints[slug] = {
                'detected_at': time.time(),
                'chain': chain_key,
                'slug': slug,
                'name': detail.get('collection_name') or slug,
                'purchased': False,
                'purchased_by': [],
                'total_supply': detail.get('total_supply', 0),
                'max_supply': detail.get('max_supply', 0),
            }
            save_persistent_data()

        # 3. التحقق من السعر المجاني
        w3 = W3_INSTANCES[chain_key]
        eth_price_usd = get_eth_price_usd()
        contract_address = detail.get("contract_address")
        
        if contract_address:
            onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
            price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
            
            if not is_free_or_negligible(price_wei, eth_price_usd):
                # مينت مدفوع - إضافة للمراقبة مع تقييم الجودة
                twitter_username = await asyncio.to_thread(
                    get_twitter_username_from_opensea, slug, OPENSEA_API_KEY
                )
                
                quality = detect_mint_quality(slug, {
                    **detail,
                    'twitter_username': twitter_username
                })
                
                # إرسال إشعار بالاكتشاف
                broadcast_mint_detection(slug, quality, detail)
                
                # إضافة للمراقبة إذا كان جيداً
                if quality['score'] >= 50:
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": detail,
                        "twitter_username": twitter_username,
                        "quality_score": quality['score']
                    }
                    broadcast_message(build_watching_message(
                        detail, 
                        f"السعر مدفوع، لكن الجيد ({quality['score']}/100)"
                    ))
                    save_persistent_data()
                return

        # 4. التحقق من وجود حساب X عبر OpenSea
        twitter_username = await asyncio.to_thread(
            get_twitter_username_from_opensea, slug, OPENSEA_API_KEY
        )
        if not twitter_username:
            log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X مربوط.")
            mark_rejected(slug)
            
            # إرسال إشعار بالاكتشاف مع تقييم ضعيف
            quality = detect_mint_quality(slug, detail)
            broadcast_mint_detection(slug, quality, detail)
            return

        log.info(f"✅ '{slug}': يوجد حساب X مربوط (@{twitter_username}) — المتابعة للشراء.")
        
        # تحديث تفاصيل المينت
        detail['twitter_username'] = twitter_username
        
        # 5. تقييم جودة المينت
        quality = detect_mint_quality(slug, detail)
        broadcast_mint_detection(slug, quality, detail)

        # 6. تنفيذ الشراء إذا كانت الجودة جيدة
        if quality['score'] >= 50:  # 50 نقطة كحد أدنى
            results = await try_buy_now_multi_wallet(slug, chain_key, detail)

            if results is None:
                watchlist[slug] = {
                    "chain_key": chain_key,
                    "detail": detail,
                    "twitter_username": twitter_username,
                    "quality_score": quality['score']
                }
                broadcast_message(build_watching_message(
                    detail, 
                    f"السعر الحالي مدفوع — تحت المراقبة ({quality['score']}/100)"
                ))
                save_persistent_data()
                return

            # تحديث حالة المينت
            if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
                watchlist[slug] = {
                    "chain_key": chain_key,
                    "detail": detail,
                    "twitter_username": twitter_username,
                    "quality_score": quality['score']
                }
                save_persistent_data()
        else:
            log.info(f"⏭️ تجاهل '{slug}': جودة منخفضة ({quality['score']}/100)")

    except Exception as e:
        log.error(f"❌ خطأ بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)

# ==================== حلقة المراقبة ====================

async def watch_loop():
    """حلقة مراقبة المينتات المدفوعة"""
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        if not watchlist:
            continue

        for slug in list(watchlist.keys()):
            # التحقق من اكتمال الشراء
            if slug in in_flight or len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                watchlist.pop(slug, None)
                save_persistent_data()
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
                    broadcast_message(build_gaveup_message(entry["detail"], "المينت لم يعد نشطًا."))
                    save_persistent_data()
                    continue

                stage = fresh_detail.get("active_stage")
                if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
                    watchlist.pop(slug, None)
                    broadcast_message(build_gaveup_message(fresh_detail, "انتهت المرحلة."))
                    save_persistent_data()
                    continue

                # تحديث التويتر إذا لم يكن موجوداً
                if 'twitter_username' not in entry:
                    twitter_username = await asyncio.to_thread(
                        get_twitter_username_from_opensea, slug, OPENSEA_API_KEY
                    )
                    if twitter_username:
                        entry['twitter_username'] = twitter_username
                        fresh_detail['twitter_username'] = twitter_username

                # محاولة الشراء مرة أخرى
                results = await try_buy_now_multi_wallet(slug, chain_key, fresh_detail)

                if results is None:
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": fresh_detail,
                        "twitter_username": entry.get('twitter_username'),
                        "quality_score": entry.get('quality_score', 0)
                    }
                    continue

                if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                    watchlist.pop(slug, None)
                else:
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": fresh_detail,
                        "twitter_username": entry.get('twitter_username'),
                        "quality_score": entry.get('quality_score', 0)
                    }
                
                save_persistent_data()

            except Exception as e:
                log.error(f"❌ خطأ بدورة مراقبة '{slug}': {e}")
            finally:
                in_flight.discard(slug)

# ==================== الاتصال بـ OpenSea Stream ====================

async def listen_opensea():
    """الاستماع لتدفق المينتات من OpenSea مع كشف محسّن"""
    msg_ref = 0
    detected_count = 0
    
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
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

                    # معالجة الرسائل
                    if isinstance(parsed, list) and len(parsed) == 5:
                        _jref, _ref, _topic, event_name, payload_wrapper = parsed
                    else:
                        continue

                    # التحقق من أحداث المينت
                    if event_name != "item_transferred":
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    
                    # التحقق من صحة حدث المينت
                    is_valid, chain_key, slug = is_valid_mint_event(payload)
                    if not is_valid or not slug:
                        continue

                    # التحقق من عدم تكرار المينت
                    if slug in detected_mints:
                        # تحديث الوقت
                        detected_mints[slug]['detected_at'] = time.time()
                        continue

                    detected_count += 1
                    log.info(f"🔔 تم اكتشاف مينت جديد #{detected_count}: {slug} على {chain_key}")

                    # تشغيل تقييم المينت
                    asyncio.create_task(evaluate_new_mint(slug, chain_key))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"⚠️ انقطع الاتصال ({e}). إعادة الاتصال...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"❌ خطأ غير متوقع في الاستماع: {e}.")
            await asyncio.sleep(5)

# ==================== التشغيل الرئيسي ====================

async def run():
    """تشغيل النظام الرئيسي"""
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        broadcast_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false).")
        await telegram_sender()
        return

    # تحميل البيانات المحفوظة
    load_persistent_data()
    
    # إرسال رسالة التشغيل
    broadcast_message(f"✅ تم تشغيل المحفظة الخاصة بك بنجاح وربطها بهذا البوت!")
    broadcast_message(f"📊 النظام جاهز لكشف المينتات الجديدة وتقييمها تلقائياً")
    
    # تشغيل المهام المتوازية
    await asyncio.gather(
        listen_opensea(),
        watch_loop(),
        telegram_sender(),
        periodic_save(),
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
            save_persistent_data()
            break
        except Exception as e:
            log.critical(f"❌ توقف غير متوقع: {e}.")
            save_persistent_data()
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break

if __name__ == "__main__":
    main()
