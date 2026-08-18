# main.py - نسخة محسنة بالكامل

"""
النظام الكامل — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها:
  - يكتشف مينتات اليوم على Robinhood + Ethereum
  - يشتري لجميع المحافظ المعرفة بالتوازي (Parallel Execution)
  - يرسل إشعار الشراء أو التحديث لكل محفظة على بوت التيليجرام الخاص بها
  - نظام إشعارات محسن مع تجميع ذكي وإرسال تقارير دورية
  - نظام محاولات متطور مع إعادة محاولة تكيفية
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_wallet_lock,
    get_attempt_summary,
    get_global_summary,
    clear_attempts,
    should_retry_purchase,
    is_permanent_failure,
    AttemptStatus,
    ErrorType,
    ErrorClassifier,
    attempt_manager,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

# ===================== الإعدادات =====================

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
        "index": i,
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
REJECTION_COOLDOWN_SECONDS = 120
SUMMARY_INTERVAL_SECONDS = 300  # 5 دقائق

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
        "label": "Robinhood Chain",
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ETHEREUM}",
        "max_gas_fee_usd": 0.50,
        "label": "Ethereum Mainnet",
    },
}

W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# ===================== نظام الإشعارات المحسن =====================

class NotificationPriority(Enum):
    """أولوية الإشعارات"""
    CRITICAL = 0   # نجاح شراء، أخطاء حرجة
    HIGH = 1       # فشل دائم، تنبيهات مهمة
    MEDIUM = 2     # تحديثات حالة، مراقبة
    LOW = 3        # إشعارات عادية
    DEBUG = 4      # إشعارات تصحيح

@dataclass
class Notification:
    """هيكل الإشعار الموحد"""
    bot_token: str
    chat_id: str
    text: str
    priority: NotificationPriority = NotificationPriority.MEDIUM
    category: str = "general"
    slug: Optional[str] = None
    wallet: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    sent: bool = False
    
    def should_send(self) -> bool:
        return bool(self.bot_token and self.chat_id and self.text)
    
    def is_critical(self) -> bool:
        return self.priority in [NotificationPriority.CRITICAL, NotificationPriority.HIGH]

class NotificationManager:
    """مدير الإشعارات المتكامل مع تجميع ذكي وتقليل التكرار"""
    
    def __init__(self):
        self._queue: asyncio.Queue[Notification] = asyncio.Queue()
        self._pending_groups: Dict[str, List[Notification]] = defaultdict(list)
        self._cooldown: Dict[str, float] = {}
        self._cooldown_seconds = 5  # تجميع الإشعارات لنفس المجموعة
        self._last_summary = time.time()
        self._lock = asyncio.Lock()
    
    def _get_group_key(self, slug: str, category: str) -> str:
        return f"{slug}_{category}"
    
    def _should_cooldown(self, key: str) -> bool:
        last = self._cooldown.get(key, 0)
        return time.time() - last < self._cooldown_seconds
    
    def enqueue(self, notification: Notification):
        """إضافة إشعار مع تجميع ذكي"""
        # الإشعارات الحرجة ترسل فوراً
        if notification.is_critical() or notification.category == "success":
            self._queue.put_nowait(notification)
            return
        
        # تجميع الإشعارات العادية
        if notification.slug:
            key = self._get_group_key(notification.slug, notification.category)
            
            # إذا كان هناك تبريد، نضيف للإشعارات المعلقة
            if self._should_cooldown(key):
                self._pending_groups[key].append(notification)
                return
            
            # نرسل الإشعار ونسجل التبريد
            self._queue.put_nowait(notification)
            self._cooldown[key] = time.time()
            
            # جدولة إرسال الإشعارات المعلقة بعد التبريد
            asyncio.create_task(self._flush_group_after_cooldown(key))
        else:
            # إشعارات بدون slug ترسل مباشرة
            self._queue.put_nowait(notification)
    
    async def _flush_group_after_cooldown(self, key: str):
        """إرسال الإشعارات المجمعة بعد انتهاء التبريد"""
        await asyncio.sleep(self._cooldown_seconds + 0.5)
        
        async with self._lock:
            notifications = self._pending_groups.pop(key, [])
            if not notifications:
                return
            
            # بناء رسالة مجمعة
            if notifications:
                # استخدام أول إشعار كمرجع
                first = notifications[0]
                if first.category == "failure":
                    header = f"❌ <b>فشل الشراء للمجموعة: {first.slug}</b>\n\n"
                elif first.category == "watching":
                    header = f"👀 <b>مراقبة المجموعة: {first.slug}</b>\n\n"
                else:
                    header = f"📢 <b>تحديث للمجموعة: {first.slug}</b>\n\n"
                
                # تجميع الرسائل حسب المحفظة
                wallet_messages: Dict[str, List[str]] = defaultdict(list)
                for n in notifications:
                    if n.wallet:
                        w_short = n.wallet[:6] + "..." + n.wallet[-4:]
                        wallet_messages[w_short].append(n.text)
                    else:
                        wallet_messages["عام"].append(n.text)
                
                lines = [header]
                for wallet, texts in wallet_messages.items():
                    lines.append(f"<b>المحفظة {wallet}:</b>")
                    lines.extend(texts)
                
                full_text = "\n".join(lines)
                
                # إرسال للجميع
                for wallet_data in WALLETS_DATA:
                    group_notification = Notification(
                        bot_token=wallet_data["bot_token"],
                        chat_id=wallet_data["chat_id"],
                        text=full_text,
                        priority=first.priority,
                        category=first.category,
                        slug=first.slug,
                    )
                    self._queue.put_nowait(group_notification)
    
    async def get(self) -> Optional[Notification]:
        """الحصول على إشعار من الطابور"""
        try:
            return await self._queue.get()
        except asyncio.CancelledError:
            return None
    
    def task_done(self):
        """تأكيد معالجة الإشعار"""
        self._queue.task_done()
    
    async def send_summary(self) -> bool:
        """إرسال تقرير دوري"""
        now = time.time()
        if now - self._last_summary < SUMMARY_INTERVAL_SECONDS:
            return False
        
        self._last_summary = now
        
        # الحصول على الملخص من مدير المحاولات
        summary = await get_global_summary()
        
        # بناء التقرير
        lines = [
            "📊 <b>تقرير دوري لحالة النظام</b>",
            f"⏰ {datetime.now().strftime('%H:%M:%S')}",
            "",
            f"📈 <b>إجمالي المحاولات:</b> {summary['total_attempts']}",
        ]
        
        if summary['total_attempts'] > 0:
            success_rate = (summary['success'] / summary['total_attempts']) * 100
            lines.append(f"✅ <b>نسبة النجاح:</b> {success_rate:.1f}%")
            lines.append(f"✅ نجاح: {summary['success']}")
            lines.append(f"❌ فشل: {summary['failed']}")
            lines.append(f"🔄 قيد الإعادة: {summary.get('retry', 0)}")
            lines.append(f"⏹️ تم التخلي: {summary.get('gave_up', 0)}")
        
        lines.append("")
        lines.append(f"👀 <b>قيد المراقبة:</b> {len(watchlist)} مجموعة")
        lines.append(f"✅ <b>مكتملة:</b> {len(successful_mints)} مجموعة")
        
        # تفاصيل لكل محفظة
        lines.append("")
        lines.append("<b>تفاصيل المحافظ:</b>")
        for wallet_data in WALLETS_DATA:
            wallet = wallet_data["wallet"]
            w_short = wallet[:6] + "..." + wallet[-4:]
            
            # حساب إحصائيات المحفظة
            wallet_summary = await get_global_summary()
            wallet_data_summary = wallet_summary.get('by_wallet', {}).get(wallet, {})
            attempts = wallet_data_summary.get('attempts', 0)
            success = wallet_data_summary.get('success', 0)
            failed = wallet_data_summary.get('failed', 0)
            
            if attempts > 0:
                status = f"✅ {success} نجاح | ❌ {failed} فشل"
            else:
                status = "🟢 نشط (بدون محاولات)"
            
            lines.append(f"• {w_short}: {status}")
        
        summary_text = "\n".join(lines)
        
        # إرسال للجميع
        for wallet_data in WALLETS_DATA:
            notification = Notification(
                bot_token=wallet_data["bot_token"],
                chat_id=wallet_data["chat_id"],
                text=summary_text,
                priority=NotificationPriority.LOW,
                category="summary",
            )
            self._queue.put_nowait(notification)
        
        return True

# ===================== المتغيرات العالمية =====================

notification_manager = NotificationManager()

# تتبع المحافظ التي اشترت بنجاح
successful_mints: Dict[str, Set[str]] = {}
watchlist: Dict[str, Dict] = {}
in_flight: Set[str] = set()
rejected_cooldown: Dict[str, float] = {}

# ===================== دوال المساعدة =====================

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

def fetch_drop_detail(slug: str) -> Tuple[Optional[bool], Optional[Dict]]:
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

# ===================== بناء رسائل الإشعارات =====================

def build_single_wallet_success_msg(detail: dict, result: dict, chain_key: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug", "غير معروف")
    url = detail.get("opensea_url", "")
    chain_label = CHAIN_CONFIGS.get(chain_key, {}).get("label", chain_key)
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    
    lines = [
        f"✅ <b>تم الشراء بنجاح!</b> ({chain_label})",
        "",
        f"المجموعة: <b>{name}</b>",
        f"المحفظة: <code>{w_short}</code>",
        f"الكمية: {result['quantity']}",
        f"رسوم الغاز: ${result['gas_fee_usd']:.4f}",
        f"المعاملة: <code>{result['tx_hash'][:16]}...</code>",
    ]
    if url:
        lines.append(f"🔗 {url}")
    
    return "\n".join(lines)

def build_single_wallet_failure_msg(detail: dict, result: dict, chain_key: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug", "غير معروف")
    chain_label = CHAIN_CONFIGS.get(chain_key, {}).get("label", chain_key)
    w_short = result.get('wallet', '')[:6] + "..." + result.get('wallet', '')[-4:] if result.get('wallet') else "غير معروف"
    
    reason_map = {
        "balance_too_low": "الرصيد منخفض جداً",
        "gas_too_high": "رسوم الغاز مرتفعة جداً",
        "no_fee_recipient": "لا يوجد مستلم رسوم متاح",
        "simulation_failed": "فشل محاكاة المعاملة",
        "insufficient_funds_for_total_cost": "الرصيد لا يكفي للتكلفة الإجمالية",
        "tx_error": "خطأ في إرسال المعاملة",
        "max_retries_exceeded": "تجاوز الحد الأقصى للمحاولات",
        "invalid_address": "عنوان غير صحيح",
        "sold_out": "المجموعة نفدت",
        "already_bought": "تم الشراء مسبقاً",
        "all_wallets_completed": "جميع المحافظ اكتملت",
    }
    
    reason_text = reason_map.get(result.get('reason', ''), result.get('reason', 'خطأ غير معروف'))
    error_text = result.get('error', '')
    attempts = result.get('attempts', 0)
    
    lines = [
        f"❌ <b>فشل الشراء</b> ({chain_label})",
        "",
        f"المجموعة: <b>{name}</b>",
        f"المحفظة: <code>{w_short}</code>",
        f"السبب: {reason_text}",
    ]
    
    if error_text:
        lines.append(f"التفاصيل: {error_text[:200]}")
    
    if attempts > 0:
        lines.append(f"عدد المحاولات: {attempts}")
    
    return "\n".join(lines)

def build_watching_message(detail: dict, reason: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug", "غير معروف")
    return (
        f"👀 <b>تحت المراقبة</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"السبب: {reason}\n"
        f"سنحاول الشراء تلقائياً فور توفر الفرصة."
    )

def build_gaveup_message(detail: dict, reason: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug", "غير معروف")
    return (
        f"⏹️ <b>انتهت الفرصة</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"السبب: {reason}"
    )

def build_retry_message(detail: dict, wallet: str, attempt_num: int, max_attempts: int, error: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug", "غير معروف")
    w_short = wallet[:6] + "..." + wallet[-4:] if wallet else "غير معروف"
    return (
        f"🔄 <b>إعادة محاولة الشراء</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"المحفظة: <code>{w_short}</code>\n"
        f"المحاولة: {attempt_num}/{max_attempts}\n"
        f"الخطأ السابق: {error[:100]}..."
    )

# ===================== الشراء المتوازي =====================

async def purchase_task_for_wallet(
    w3: Web3,
    wallet_data: Dict[str, Any],
    slug: str,
    contract_address: str,
    price_wei: int,
    max_per_wallet: Optional[int],
    remaining: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
    detail: Dict[str, Any],
    chain_key: str,
) -> Dict[str, Any]:
    """مهمة شراء لمحفظة واحدة مع قفل"""
    wallet_addr = wallet_data["wallet"]
    pk = wallet_data["private_key"]
    bot_token = wallet_data["bot_token"]
    chat_id = wallet_data["chat_id"]
    
    # قفل المحفظة لمنع التضارب
    lock = get_wallet_lock(wallet_addr)
    async with lock:
        # التحقق من الشراء المسبق
        if slug in successful_mints and wallet_addr in successful_mints[slug]:
            return {
                "success": False,
                "wallet": wallet_addr,
                "reason": "already_bought"
            }
        
        # التحقق من إمكانية إعادة المحاولة
        if not await should_retry_purchase(wallet_addr, slug):
            attempt = await attempt_manager.get(wallet_addr, slug)
            if attempt:
                return {
                    "success": False,
                    "wallet": wallet_addr,
                    "reason": "max_retries_exceeded",
                    "attempts": attempt.attempt_count,
                    "max_attempts": attempt.max_attempts,
                    "last_error": attempt.last_error,
                    "error_type": attempt.error_type.value
                }
        
        # محاولة الشراء
        result = await attempt_purchase_single_wallet(
            w3, pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd,
            slug=slug,
            chain_key=chain_key,
        )
        
        # معالجة النتيجة
        if result.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            
            # إشعار نجاح لكل محفظة
            msg = build_single_wallet_success_msg(detail, result, chain_key)
            notification_manager.enqueue(Notification(
                bot_token=bot_token,
                chat_id=chat_id,
                text=msg,
                priority=NotificationPriority.CRITICAL,
                category="success",
                slug=slug,
                wallet=wallet_addr,
            ))
        else:
            # إشعار فشل
            reason = result.get('reason', '')
            
            # إذا كان فشل دائم، نرسل إشعار فوري
            is_permanent = result.get('error_type') == 'permanent' or result.get('reason') == 'max_retries_exceeded'
            
            if is_permanent:
                msg = build_single_wallet_failure_msg(detail, result, chain_key)
                notification_manager.enqueue(Notification(
                    bot_token=bot_token,
                    chat_id=chat_id,
                    text=msg,
                    priority=NotificationPriority.HIGH,
                    category="failure",
                    slug=slug,
                    wallet=wallet_addr,
                ))
            
            # إذا كانت محاولة فاشلة ولكن قابلة للإعادة، نرسل إشعار أقل أولوية
            elif result.get('attempts', 0) > 0:
                attempt = await attempt_manager.get(wallet_addr, slug)
                if attempt and attempt.status == AttemptStatus.RETRY:
                    msg = build_retry_message(
                        detail, wallet_addr,
                        attempt.attempt_count, attempt.max_attempts,
                        attempt.last_error or "خطأ غير معروف"
                    )
                    notification_manager.enqueue(Notification(
                        bot_token=bot_token,
                        chat_id=chat_id,
                        text=msg,
                        priority=NotificationPriority.MEDIUM,
                        category="retry",
                        slug=slug,
                        wallet=wallet_addr,
                    ))
        
        return result

async def try_buy_now_multi_wallet(slug: str, chain_key: str, detail: dict) -> Optional[List[dict]]:
    """محاولة شراء لكل المحافظ بالتوازي"""
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
    
    # فقط المينتات المجانية
    if not is_free_or_negligible(price_wei, eth_price_usd):
        return None
    
    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]
    
    already_bought = successful_mints.get(slug, set())
    pending_wallets = [w for w in WALLETS_DATA if w["wallet"] not in already_bought]
    
    if not pending_wallets:
        return [{"success": False, "reason": "all_wallets_completed"}]
    
    # تنفيذ المهام بالتوازي
    tasks = [
        purchase_task_for_wallet(
            w3, wallet_data, slug, contract_address,
            price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd,
            detail, chain_key
        )
        for wallet_data in pending_wallets
    ]
    
    results = await asyncio.gather(*tasks)
    return list(results)

# ===================== تقييم المينتات الجديدة =====================

async def evaluate_new_mint(slug: str, chain_key: str):
    """تقييم مينت جديد واتخاذ القرار المناسب"""
    # التحقق من التكرار
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
        
        # التحقق من السعر
        w3 = W3_INSTANCES[chain_key]
        eth_price_usd = get_eth_price_usd()
        contract_address = detail.get("contract_address")
        
        if contract_address:
            onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
            price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
            
            if not is_free_or_negligible(price_wei, eth_price_usd):
                log.info(f"⏭️ تجاهل '{slug}': سعر مدفوع (${(price_wei/1e18)*eth_price_usd:.4f})")
                mark_rejected(slug)
                return
        
        # التحقق من تويتر
        twitter_username = await asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
        if not twitter_username:
            log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X مربوط.")
            mark_rejected(slug)
            return
        
        log.info(f"✅ '{slug}': يوجد حساب X مربوط (@{twitter_username}) — جاري الشراء.")
        
        # محاولة الشراء
        results = await try_buy_now_multi_wallet(slug, chain_key, detail)
        
        if results is None:
            # سعر مدفوع - نضعه في المراقبة
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            broadcast_message(build_watching_message(detail, "السعر الحالي مدفوع — تحت المراقبة."))
            return
        
        # تحديث حالة المراقبة
        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
        else:
            watchlist.pop(slug, None)
            broadcast_message(f"✅ <b>اكتمل الشراء للمجموعة: {slug}</b>")
            
    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)

async def watch_loop():
    """حلقة مراقبة المجموعات التي لم تشترى بعد"""
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        
        if not watchlist:
            continue
        
        # إرسال تقرير دوري
        await notification_manager.send_summary()
        
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
                    broadcast_message(build_gaveup_message(entry["detail"], "المينت لم يعد نشطاً."))
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

# ===================== وظائف الإرسال العامة =====================

def broadcast_message(text: str, priority: NotificationPriority = NotificationPriority.LOW,
                      category: str = "broadcast"):
    """إرسال إشعار عام لجميع البوتات"""
    for wallet_data in WALLETS_DATA:
        notification_manager.enqueue(Notification(
            bot_token=wallet_data["bot_token"],
            chat_id=wallet_data["chat_id"],
            text=text,
            priority=priority,
            category=category,
        ))

async def telegram_sender():
    """معالج إرسال رسائل التيليجرام"""
    while True:
        notification = await notification_manager.get()
        if notification is None:
            continue
        
        try:
            if not notification.should_send():
                notification_manager.task_done()
                continue
            
            telegram_api = f"https://api.telegram.org/bot{notification.bot_token}"
            await asyncio.to_thread(
                requests.post,
                f"{telegram_api}/sendMessage",
                data={
                    "chat_id": notification.chat_id,
                    "text": notification.text,
                    "parse_mode": "HTML",
                },
                timeout=10,
            )
            notification.sent = True
            log.debug(f"إرسال إشعار: {notification.category} - {notification.slug or 'general'}")
            
        except Exception as e:
            log.error(f"خطأ إرسال تليجرام: {e}")
        
        notification_manager.task_done()
        await asyncio.sleep(0.05)

# ===================== الاستماع إلى OpenSea Stream =====================

async def listen_opensea():
    """الاستماع إلى تدفق OpenSea للمينتات الجديدة"""
    msg_ref = 0
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"🟢 متصل بـ OpenSea Stream — يراقب {len(WALLETS_DATA)} محافظ.")
                
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
                    
                    # معالجة المينت الجديد
                    asyncio.create_task(evaluate_new_mint(slug, chain_key))
                    
        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"⚠️ انقطع الاتصال ({e}). إعادة الاتصال...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"❌ خطأ غير متوقع: {e}.")
            await asyncio.sleep(5)

# ===================== التشغيل الرئيسي =====================

async def run():
    """تشغيل النظام الرئيسي"""
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        broadcast_message("🔴 البوت شغال لكن بوضع الإيقاف (BOT_ENABLED=false).")
        await telegram_sender()
        return
    
    # رسالة بدء التشغيل
    broadcast_message(
        f"✅ <b>تم تشغيل البوت بنجاح!</b>\n\n"
        f"📊 <b>عدد المحافظ:</b> {len(WALLETS_DATA)}\n"
        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"جاري مراقبة المينتات المجانية..."
    )
    
    # تشغيل المهام المتوازية
    await asyncio.gather(
        listen_opensea(),
        watch_loop(),
        telegram_sender(),
    )

def main():
    """الدالة الرئيسية مع إعادة تشغيل تلقائي"""
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("🛑 تم الإيقاف يدوياً.")
            break
        except Exception as e:
            log.critical(f"💥 توقف غير متوقع: {e}.")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break

if __name__ == "__main__":
    main()
