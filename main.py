"""
النظام الكامل المحسن — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها.
ميزات متقدمة: كشف دقيق، تقييم ذكي، شراء متوازي، وإدارة محسنة.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Set, List, Any, Tuple
from dataclasses import dataclass, field

import aiohttp
import requests
import websockets
from dotenv import load_dotenv
from web3 import Web3

from buyer import (
    WalletData,
    PurchaseStrategy,
    purchase_parallel,
    get_web3,
    get_onchain_public_price_wei,
    get_wallet_stats,
    format_wallet_stats,
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

# إنشاء هيكلية المحافظ مع استراتيجيات متنوعة
WALLETS_DATA: List[WalletData] = []
for i in range(len(WALLETS)):
    # توزيع الاستراتيجيات (أول 3 عدوانية، التالية 4 متوازنة، الباقي محافظة)
    if i < 3:
        strategy = PurchaseStrategy.AGGRESSIVE
    elif i < 7:
        strategy = PurchaseStrategy.BALANCED
    else:
        strategy = PurchaseStrategy.CONSERVATIVE
    
    WALLETS_DATA.append(WalletData(
        wallet=WALLETS[i],
        private_key=PRIVATE_KEYS[i],
        bot_token=TELEGRAM_BOT_TOKENS[i],
        chat_id=TELEGRAM_CHAT_IDS[i],
        strategy=strategy,
    ))

ALCHEMY_API_KEY_ROBINHOOD = os.environ["ALCHEMY_API_KEY"]
ALCHEMY_API_KEY_ETHEREUM = os.environ["ALCHEMY_API_KEY_ETHEREUM"]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"
COLLECTIONS_API = "https://api.opensea.io/api/v2/collections"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

# ==================== إعدادات الأداء المحسنة ====================
HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 15
REJECTION_COOLDOWN_SECONDS = 120
SAVE_INTERVAL_SECONDS = 60
MIN_TWITTER_FOLLOWERS = 100
MAX_MINT_AGE_SECONDS = 3600
MIN_QUALITY_SCORE = 50
MAX_CONCURRENT_EVALUATIONS = 5  # الحد الأقصى للتقييمات المتزامنة

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

W3_INSTANCES = {}
for key, cfg in CHAIN_CONFIGS.items():
    try:
        W3_INSTANCES[key] = get_web3(cfg["rpc_url"])
        log.info(f"✅ تم الاتصال بـ {key}")
    except Exception as e:
        log.error(f"❌ فشل الاتصال بـ {key}: {e}")
        raise

STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# ==================== التخزين الدائم ====================
SUCCESS_FILE = Path("successful_mints.json")
WATCHLIST_FILE = Path("watchlist.json")
DETECTED_MINTS_FILE = Path("detected_mints.json")
STATS_FILE = Path("wallet_stats.json")

# المتغيرات العامة
successful_mints: Dict[str, Set[str]] = {}
watchlist: Dict[str, Dict] = {}
detected_mints: Dict[str, Dict] = {}
in_flight: Set[str] = set()
rejected_cooldown: Dict[str, float] = {}
_eth_price_cache = {"value": None, "ts": 0}
evaluation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EVALUATIONS)

# ==================== نظام التخزين المحسن ====================
class PersistentStorage:
    """نظام تخزين دائم محسن"""
    
    def __init__(self):
        self.files = {
            "successful": SUCCESS_FILE,
            "watchlist": WATCHLIST_FILE,
            "detected": DETECTED_MINTS_FILE,
            "stats": STATS_FILE,
        }
        self.data = {
            "successful": {},
            "watchlist": {},
            "detected": {},
            "stats": {},
        }
        self._load_all()
    
    def _load_all(self):
        """تحميل جميع البيانات"""
        # تحميل المينتات الناجحة
        if SUCCESS_FILE.exists():
            try:
                with open(SUCCESS_FILE, 'r') as f:
                    data = json.load(f)
                    self.data["successful"] = {k: set(v) for k, v in data.items()}
                    log.info(f"✅ تم تحميل {len(self.data['successful'])} مينت ناجح")
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل المينتات الناجحة: {e}")
        
        # تحميل قائمة المراقبة
        if WATCHLIST_FILE.exists():
            try:
                with open(WATCHLIST_FILE, 'r') as f:
                    self.data["watchlist"] = json.load(f)
                    log.info(f"✅ تم تحميل {len(self.data['watchlist'])} مجموعة تحت المراقبة")
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل قائمة المراقبة: {e}")
        
        # تحميل المينتات المكتشفة
        if DETECTED_MINTS_FILE.exists():
            try:
                with open(DETECTED_MINTS_FILE, 'r') as f:
                    self.data["detected"] = json.load(f)
                    log.info(f"✅ تم تحميل {len(self.data['detected'])} مينت مكتشف")
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل المينتات المكتشفة: {e}")
        
        # تحميل إحصائيات المحافظ
        if STATS_FILE.exists():
            try:
                with open(STATS_FILE, 'r') as f:
                    self.data["stats"] = json.load(f)
                    # تحديث إحصائيات المحافظ
                    for wd in WALLETS_DATA:
                        if wd.wallet in self.data["stats"]:
                            wd.stats.update(self.data["stats"][wd.wallet])
                    log.info(f"✅ تم تحميل إحصائيات {len(self.data['stats'])} محفظة")
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل الإحصائيات: {e}")
    
    def save_all(self):
        """حفظ جميع البيانات"""
        try:
            # حفظ المينتات الناجحة
            with open(SUCCESS_FILE, 'w') as f:
                json.dump(
                    {k: list(v) for k, v in self.data["successful"].items()},
                    f, indent=2
                )
            
            # حفظ قائمة المراقبة
            with open(WATCHLIST_FILE, 'w') as f:
                json.dump(self.data["watchlist"], f, indent=2)
            
            # حفظ المينتات المكتشفة
            with open(DETECTED_MINTS_FILE, 'w') as f:
                json.dump(self.data["detected"], f, indent=2)
            
            # حفظ إحصائيات المحافظ
            stats_data = {}
            for wd in WALLETS_DATA:
                stats_data[wd.wallet] = wd.stats
            with open(STATS_FILE, 'w') as f:
                json.dump(stats_data, f, indent=2)
            
            log.debug("💾 تم حفظ البيانات بنجاح")
        except Exception as e:
            log.error(f"❌ خطأ في حفظ البيانات: {e}")

storage = PersistentStorage()

# تحديث المتغيرات العامة من التخزين
successful_mints = storage.data["successful"]
watchlist = storage.data["watchlist"]
detected_mints = storage.data["detected"]

async def periodic_save():
    """حفظ البيانات بشكل دوري"""
    while True:
        await asyncio.sleep(SAVE_INTERVAL_SECONDS)
        storage.save_all()

# ==================== نظام الكشف المتقدم ====================
class MintDetector:
    """كاشف المينتات المتقدم"""
    
    def __init__(self):
        self.detection_count = 0
        self.last_detection_time = None
    
    def is_valid_mint_event(self, payload: dict) -> Tuple[bool, Optional[str], Optional[str]]:
        """التحقق من صحة حدث المينت"""
        try:
            # التحقق من المصدر
            from_account = payload.get("from_account", {})
            from_address = from_account.get("address", "").lower()
            if from_address != ZERO_ADDRESS:
                return False, None, None
            
            # التحقق من المجموعة
            collection = payload.get("collection", {})
            slug = collection.get("slug", "")
            if not slug:
                return False, None, None
            
            # التحقق من السلسلة
            item = payload.get("item", {})
            chain = item.get("chain", {})
            stream_chain_name = chain.get("name", "")
            chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
            if chain_key is None:
                return False, None, None
            
            # التحقق من الكمية
            quantity = payload.get("quantity", 0)
            if quantity <= 0:
                return False, None, None
            
            return True, chain_key, slug
            
        except Exception as e:
            log.debug(f"خطأ في التحقق من حدث المينت: {e}")
            return False, None, None
    
    def record_detection(self, slug: str):
        """تسجيل اكتشاف مينت"""
        self.detection_count += 1
        self.last_detection_time = time.time()
        log.info(f"🔔 تم اكتشاف مينت جديد #{self.detection_count}: {slug}")

detector = MintDetector()

# ==================== نظام التقييم الذكي ====================
class MintEvaluator:
    """مقيم المينتات الذكي"""
    
    def __init__(self):
        self.evaluation_history: Dict[str, Dict] = {}
    
    def evaluate_quality(self, slug: str, detail: dict) -> Dict[str, Any]:
        """تقييم جودة المينت"""
        quality = {
            'score': 0,
            'has_twitter': False,
            'twitter_username': None,
            'has_high_supply': False,
            'is_active': False,
            'is_free': False,
            'reasons': [],
            'recommendation': 'skip'
        }
        
        try:
            # 1. التحقق من تويتر
            twitter = detail.get('twitter_username')
            if twitter:
                quality['has_twitter'] = True
                quality['twitter_username'] = twitter
                quality['score'] += 30
                quality['reasons'].append(f"✅ حساب X: @{twitter}")
            else:
                quality['reasons'].append("❌ لا يوجد حساب X")
            
            # 2. التحقق من العرض
            total_supply = int(detail.get('total_supply', 0))
            max_supply = int(detail.get('max_supply', 0))
            if max_supply > 1000:
                quality['has_high_supply'] = True
                quality['score'] += 20
                quality['reasons'].append(f"✅ عرض كبير: {max_supply}")
            else:
                quality['reasons'].append(f"⚠️ عرض صغير: {max_supply}")
            
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
            
            # 5. التوصية النهائية
            if quality['score'] >= 70:
                quality['recommendation'] = 'buy'
                quality['reasons'].insert(0, "⭐️⭐️⭐️ ممتاز - شراء فوري")
            elif quality['score'] >= 50:
                quality['recommendation'] = 'watch'
                quality['reasons'].insert(0, "⭐️⭐️ جيد - مراقبة")
            else:
                quality['recommendation'] = 'skip'
                quality['reasons'].insert(0, "⭐️ ضعيف - تخطي")
            
        except Exception as e:
            log.error(f"خطأ في تقييم المينت: {e}")
            quality['reasons'].append(f"❌ خطأ: {e}")
        
        # حفظ في السجل
        self.evaluation_history[slug] = quality
        
        return quality
    
    def get_recommendation(self, slug: str) -> str:
        """الحصول على التوصية لمينت معين"""
        if slug in self.evaluation_history:
            return self.evaluation_history[slug]['recommendation']
        return 'unknown'

evaluator = MintEvaluator()

# ==================== نظام رسائل التيليجرام المحسن ====================
class TelegramManager:
    """مدير رسائل التيليجرام"""
    
    def __init__(self):
        self.send_queue: asyncio.Queue = asyncio.Queue()
        self.message_count = 0
        self.last_message_time = None
    
    def enqueue(self, bot_token: str, chat_id: str, text: str):
        """إضافة رسالة للطابور"""
        self.send_queue.put_nowait({
            "bot_token": bot_token,
            "chat_id": chat_id,
            "text": text
        })
    
    def broadcast(self, text: str):
        """إرسال رسالة لجميع البوتات"""
        for w in WALLETS_DATA:
            self.enqueue(w.bot_token, w.chat_id, text)
    
    def send_to_wallet(self, wallet_index: int, text: str):
        """إرسال رسالة لمحفظة محددة"""
        if 0 <= wallet_index < len(WALLETS_DATA):
            w = WALLETS_DATA[wallet_index]
            self.enqueue(w.bot_token, w.chat_id, text)
    
    async def sender_loop(self):
        """حلقة إرسال الرسائل"""
        while True:
            msg = await self.send_queue.get()
            try:
                telegram_api = f"https://api.telegram.org/bot{msg['bot_token']}"
                response = await asyncio.to_thread(
                    requests.post,
                    f"{telegram_api}/sendMessage",
                    data={
                        "chat_id": msg["chat_id"],
                        "text": msg["text"],
                        "parse_mode": "HTML"
                    },
                    timeout=10,
                )
                
                if response.status_code == 200:
                    self.message_count += 1
                    self.last_message_time = time.time()
                else:
                    log.warning(f"⚠️ فشل إرسال رسالة: {response.status_code}")
                    
            except Exception as e:
                log.error(f"❌ خطأ إرسال تليجرام: {e}")
            
            self.send_queue.task_done()
            await asyncio.sleep(0.1)  # تجنب تجاوز حدود API
    
    def build_success_message(self, detail: dict, result: dict, chain_key: str) -> str:
        """بناء رسالة نجاح الشراء"""
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
            f"المعاملة: <code>{result['tx_hash'][:10]}...</code>\n"
            f"🔗 {url}"
        )
    
    def build_detection_message(self, slug: str, quality: Dict[str, Any], detail: dict) -> str:
        """بناء رسالة اكتشاف مينت"""
        name = detail.get('collection_name') or slug
        reasons = "\n".join(quality['reasons'])
        
        return (
            f"🔔 <b>تم اكتشاف مينت جديد!</b>\n\n"
            f"المجموعة: <b>{name}</b>\n"
            f"الرابط: {detail.get('opensea_url', '')}\n\n"
            f"📊 <b>تقييم الجودة:</b>\n"
            f"{reasons}\n\n"
            f"النقاط: {quality['score']}/100\n"
            f"التوصية: {quality['recommendation'].upper()}"
        )

telegram = TelegramManager()

# ==================== دوال مساعدة ====================
def is_in_cooldown(slug: str) -> bool:
    """التحقق من وجود تبريد"""
    ts = rejected_cooldown.get(slug)
    if ts is None:
        return False
    if time.time() - ts >= REJECTION_COOLDOWN_SECONDS:
        rejected_cooldown.pop(slug, None)
        return False
    return True

def mark_rejected(slug: str):
    """تسجيل وقت الرفض"""
    rejected_cooldown[slug] = time.time()

def get_eth_price_usd() -> float:
    """جلب سعر ETH مع التخزين المؤقت"""
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

async def fetch_drop_detail(slug: str) -> Tuple[Optional[bool], Optional[Dict]]:
    """جلب تفاصيل المينت"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{DROPS_API_BASE}/{slug}",
                headers={"x-api-key": OPENSEA_API_KEY},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    return True, await resp.json()
                if resp.status == 404:
                    return False, None
                log.warning(f"⚠️ استجابة غير متوقعة: {resp.status}")
                return None, None
    except Exception as e:
        log.warning(f"⚠️ [Drops API] خطأ: {e}")
        return None, None

def parse_iso(ts: str):
    """تحويل التاريخ"""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def started_today_local(stage: dict) -> bool:
    """التحقق من بدء المينت اليوم"""
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

# ==================== منطق الشراء ====================
async def try_buy_now_multi_wallet(
    slug: str,
    chain_key: str,
    detail: dict
) -> Optional[List[Dict[str, Any]]]:
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
    onchain_price = await get_onchain_public_price_wei(w3, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
    
    # التحقق من السعر المجاني
    if not is_free_or_negligible(price_wei, eth_price_usd):
        return None  # مدفوع -> للمراقبة
    
    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]
    
    # المحافظ التي لم تشترِ بعد
    already_bought = successful_mints.get(slug, set())
    pending_wallets = [
        wd for wd in WALLETS_DATA
        if wd.wallet not in already_bought
    ]
    
    if not pending_wallets:
        return [{"success": False, "reason": "all_wallets_completed"}]
    
    # تحديث بيانات السياق
    for wd in pending_wallets:
        wd.current_detail = detail
        wd.chain_key = chain_key
    
    # تنفيذ الشراء المتوازي
    results = await purchase_parallel(
        w3=w3,
        wallets_data=pending_wallets,
        nft_contract=contract_address,
        price_wei_per_token=price_wei,
        max_per_wallet=max_per_wallet,
        remaining_supply=remaining,
        eth_price_usd=eth_price_usd,
        max_gas_fee_usd=max_gas_fee_usd,
    )
    
    # معالجة النتائج
    for result in results:
        if result.success:
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(result.wallet)
            
            # إرسال إشعار النجاح
            wallet_index = next(
                i for i, wd in enumerate(WALLETS_DATA)
                if wd.wallet == result.wallet
            )
            msg = telegram.build_success_message(detail, {
                'wallet': result.wallet,
                'quantity': result.quantity,
                'gas_fee_usd': result.gas_fee_usd,
                'tx_hash': result.tx_hash,
            }, chain_key)
            telegram.send_to_wallet(wallet_index, msg)
    
    # حفظ البيانات
    storage.save_all()
    
    return [vars(r) for r in results]

# ==================== تقييم المينتات ====================
async def evaluate_new_mint(slug: str, chain_key: str):
    """تقييم المينت الجديد وتنفيذ الشراء"""
    async with evaluation_semaphore:
        if (len(successful_mints.get(slug, set())) >= len(WALLETS_DATA) or
            slug in watchlist or slug in in_flight or is_in_cooldown(slug)):
            return
        
        in_flight.add(slug)
        try:
            # جلب تفاصيل المينت
            found, detail = await fetch_drop_detail(slug)
            if not found or not detail or not detail.get("is_minting"):
                return
            
            stage = detail.get("active_stage")
            if not stage or not started_today_local(stage):
                return
            
            # تحديث المينتات المكتشفة
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
                storage.save_all()
                detector.record_detection(slug)
            
            # التحقق من السعر
            w3 = W3_INSTANCES[chain_key]
            eth_price_usd = get_eth_price_usd()
            contract_address = detail.get("contract_address")
            
            if contract_address:
                onchain_price = await get_onchain_public_price_wei(w3, contract_address)
                price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
                
                if not is_free_or_negligible(price_wei, eth_price_usd):
                    # مينت مدفوع - تقييم وإضافة للمراقبة
                    twitter_username = await asyncio.to_thread(
                        get_twitter_username_from_opensea, slug, OPENSEA_API_KEY
                    )
                    detail['twitter_username'] = twitter_username
                    
                    quality = evaluator.evaluate_quality(slug, detail)
                    
                    # إرسال إشعار بالاكتشاف
                    telegram.broadcast(
                        telegram.build_detection_message(slug, quality, detail)
                    )
                    
                    # إضافة للمراقبة إذا كان جيداً
                    if quality['recommendation'] in ['buy', 'watch']:
                        watchlist[slug] = {
                            "chain_key": chain_key,
                            "detail": detail,
                            "twitter_username": twitter_username,
                            "quality_score": quality['score'],
                        }
                        telegram.broadcast(
                            f"👀 <b>تحت المراقبة</b>\n\n"
                            f"المجموعة: <b>{detail.get('collection_name', slug)}</b>\n"
                            f"السبب: السعر مدفوع، لكن الجودة {quality['score']}/100"
                        )
                        storage.save_all()
                    return
            
            # التحقق من تويتر
            twitter_username = await asyncio.to_thread(
                get_twitter_username_from_opensea, slug, OPENSEA_API_KEY
            )
            if not twitter_username:
                log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X")
                mark_rejected(slug)
                return
            
            detail['twitter_username'] = twitter_username
            
            # تقييم الجودة
            quality = evaluator.evaluate_quality(slug, detail)
            telegram.broadcast(
                telegram.build_detection_message(slug, quality, detail)
            )
            
            # تنفيذ الشراء
            if quality['recommendation'] in ['buy', 'watch']:
                results = await try_buy_now_multi_wallet(slug, chain_key, detail)
                
                if results is None:
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": detail,
                        "twitter_username": twitter_username,
                        "quality_score": quality['score'],
                    }
                    storage.save_all()
                elif len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": detail,
                        "twitter_username": twitter_username,
                        "quality_score": quality['score'],
                    }
                    storage.save_all()
            
        except Exception as e:
            log.error(f"❌ خطأ بتقييم '{slug}': {e}")
        finally:
            in_flight.discard(slug)

# ==================== حلقة المراقبة ====================
async def watch_loop():
    """حلقة مراقبة المينتات"""
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        if not watchlist:
            continue
        
        for slug in list(watchlist.keys()):
            if slug in in_flight or len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                watchlist.pop(slug, None)
                storage.save_all()
                continue
            
            entry = watchlist.get(slug)
            if not entry:
                continue
            
            in_flight.add(slug)
            try:
                chain_key = entry["chain_key"]
                found, fresh_detail = await fetch_drop_detail(slug)
                
                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    watchlist.pop(slug, None)
                    telegram.broadcast(
                        f"❌ <b>انتهت الفرصة</b>\n\n"
                        f"المجموعة: <b>{entry['detail'].get('collection_name', slug)}</b>\n"
                        f"السبب: المينت لم يعد نشطاً"
                    )
                    storage.save_all()
                    continue
                
                stage = fresh_detail.get("active_stage")
                if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
                    watchlist.pop(slug, None)
                    telegram.broadcast(
                        f"❌ <b>انتهت الفرصة</b>\n\n"
                        f"المجموعة: <b>{fresh_detail.get('collection_name', slug)}</b>\n"
                        f"السبب: انتهت المرحلة"
                    )
                    storage.save_all()
                    continue
                
                # تحديث التويتر
                if 'twitter_username' not in entry:
                    twitter_username = await asyncio.to_thread(
                        get_twitter_username_from_opensea, slug, OPENSEA_API_KEY
                    )
                    if twitter_username:
                        entry['twitter_username'] = twitter_username
                        fresh_detail['twitter_username'] = twitter_username
                
                # محاولة الشراء
                results = await try_buy_now_multi_wallet(slug, chain_key, fresh_detail)
                
                if results is None:
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": fresh_detail,
                        "twitter_username": entry.get('twitter_username'),
                        "quality_score": entry.get('quality_score', 0),
                    }
                    continue
                
                if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                    watchlist.pop(slug, None)
                else:
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": fresh_detail,
                        "twitter_username": entry.get('twitter_username'),
                        "quality_score": entry.get('quality_score', 0),
                    }
                
                storage.save_all()
                
            except Exception as e:
                log.error(f"❌ خطأ بدورة مراقبة '{slug}': {e}")
            finally:
                in_flight.discard(slug)

# ==================== الاتصال بـ OpenSea ====================
async def listen_opensea():
    """الاستماع لتدفق المينتات"""
    msg_ref = 0
    
    while True:
        try:
            async with websockets.connect(
                STREAM_URL,
                ping_interval=None,
                open_timeout=15
            ) as ws:
                log.info(f"✅ متصل بـ OpenSea Stream — يراقب لـ {len(WALLETS_DATA)} محافظ")
                
                join_ref = str(msg_ref)
                await ws.send(json.dumps([
                    join_ref, join_ref, "collection:*", "phx_join", {}
                ]))
                msg_ref += 1
                last_heartbeat = time.time()
                
                while True:
                    # نبضات القلب
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([
                            None, hb_ref, "phoenix", "heartbeat", {}
                        ]))
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
                    
                    # التحقق من صحة الحدث
                    is_valid, chain_key, slug = detector.is_valid_mint_event(payload)
                    if not is_valid or not slug:
                        continue
                    
                    # تجنب التكرار
                    if slug in detected_mints:
                        detected_mints[slug]['detected_at'] = time.time()
                        continue
                    
                    # تشغيل التقييم
                    asyncio.create_task(evaluate_new_mint(slug, chain_key))
                    
        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"⚠️ انقطع الاتصال ({e}). إعادة الاتصال...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"❌ خطأ غير متوقع: {e}")
            await asyncio.sleep(5)

# ==================== واجهة الأوامر ====================
async def command_handler():
    """معالج الأوامر من التيليجرام"""
    # يمكن إضافة معالجة أوامر مثل /status, /stats, /stop
    while True:
        await asyncio.sleep(60)  # فحص دوري
        
        # إرسال تقارير دورية (كل ساعة)
        if int(time.time()) % 3600 < 60:
            for i, wd in enumerate(WALLETS_DATA):
                stats_msg = format_wallet_stats(wd)
                telegram.send_to_wallet(i, stats_msg)

# ==================== التشغيل الرئيسي ====================
async def run():
    """تشغيل النظام الرئيسي"""
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        telegram.broadcast("🔴 البوت شغال لكن بوضع الإيقاف (BOT_ENABLED=false)")
        await telegram.sender_loop()
        return
    
    # رسالة التشغيل
    telegram.broadcast(
        f"✅ <b>تم تشغيل النظام بنجاح!</b>\n\n"
        f"عدد المحافظ: {len(WALLETS_DATA)}\n"
        f"الاستراتيجيات:\n"
        f"- عدوانية: 3 محافظ\n"
        f"- متوازنة: 4 محافظ\n"
        f"- محافظة: 3 محافظ\n\n"
        f"النظام جاهز لكشف وتقييم المينتات الجديدة"
    )
    
    # تشغيل جميع المهام
    await asyncio.gather(
        listen_opensea(),
        watch_loop(),
        telegram.sender_loop(),
        periodic_save(),
        command_handler(),
        return_exceptions=True
    )

def main():
    """الدالة الرئيسية مع إعادة الاتصال"""
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("🛑 تم الإيقاف يدوياً")
            storage.save_all()
            break
        except Exception as e:
            log.critical(f"❌ توقف غير متوقع: {e}")
            storage.save_all()
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break

if __name__ == "__main__":
    main()
