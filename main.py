"""
النظام الكامل — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها:
  - يكتشف مينتات اليوم على Robinhood + Ethereum
  - يشتري لجميع المحافظ المعرفة بالتوازي (Parallel Execution)
  - يرسل إشعار الشراء أو التحديث لكل محفظة على بوت التيليجرام الخاص بها
  - يراقب المينتات المدفوعة التي قد تتحول لمرحلة مجانية
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field

import requests
import websockets
from dotenv import load_dotenv
from web3 import Web3

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    LockManager,
    purchase_with_retry,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

# ---------------------------------------------------------------------------
# الإعدادات والتكوين
# ---------------------------------------------------------------------------

@dataclass
class ChainConfig:
    """إعدادات سلسلة البلوكشين"""
    stream_chain_name: str
    rpc_url: str
    max_gas_fee_usd: float
    max_retries: int = 3
    retry_delay_base: float = 1.0

@dataclass
class WalletConfig:
    """إعدادات المحفظة الواحدة"""
    wallet: str
    private_key: str
    bot_token: str
    chat_id: str
    chain_key: str = ""
    current_detail: Dict[str, Any] = field(default_factory=dict)

@dataclass
class WatchlistEntry:
    """عنصر في قائمة المراقبة"""
    chain_key: str
    detail: Dict[str, Any]
    added_at: float
    last_check: float = 0
    check_count: int = 0
    max_checks: int = 100  # حد أقصى للمراقبة
    twitter_checked: bool = False  # هل تم فحص تويتر مسبقاً
    twitter_username: Optional[str] = None  # اسم مستخدم تويتر إذا تم فحصه
    was_paid: bool = False  # هل كان المينت مدفوعاً
    last_price_usd: float = 0.0  # آخر سعر تم رصده

class Config:
    """إدارة الإعدادات من ملف .env"""
    
    def __init__(self):
        self.opensea_api_key = self._get_env("OPENSEA_API_KEY", required=True)
        self.bot_enabled = self._get_env("BOT_ENABLED", "false").lower() == "true"
        
        # إعدادات Alchemy المنفصلة لكل سلسلة
        self.alchemy_api_key_robinhood = self._get_env("ALCHEMY_API_KEY", required=True)
        self.alchemy_api_key_ethereum = self._get_env("ALCHEMY_API_KEY_ETHEREUM", required=True)
        
        self.chains = {
            "robinhood": ChainConfig(
                stream_chain_name="robinhood",
                rpc_url=f"https://robinhood-mainnet.g.alchemy.com/v2/{self.alchemy_api_key_robinhood}",
                max_gas_fee_usd=float(self._get_env("MAX_GAS_FEE_ROBINHOOD", "0.05")),
                max_retries=int(self._get_env("MAX_RETRIES_ROBINHOOD", "3")),
            ),
            "ethereum": ChainConfig(
                stream_chain_name="ethereum",
                rpc_url=f"https://eth-mainnet.g.alchemy.com/v2/{self.alchemy_api_key_ethereum}",
                max_gas_fee_usd=float(self._get_env("MAX_GAS_FEE_ETHEREUM", "0.50")),
                max_retries=int(self._get_env("MAX_RETRIES_ETHEREUM", "3")),
            ),
        }
        
        # تحميل المحافظ
        self.wallets = self._load_wallets()
        
        # إعدادات المراقبة
        self.heartbeat_interval = int(self._get_env("HEARTBEAT_INTERVAL", "20"))
        self.recv_timeout = int(self._get_env("RECV_TIMEOUT", "5"))
        self.watch_poll_interval = int(self._get_env("WATCH_POLL_INTERVAL", "15"))
        self.rejection_cooldown = int(self._get_env("REJECTION_COOLDOWN", "120"))
        self.free_price_threshold = float(self._get_env("FREE_PRICE_THRESHOLD", "0.01"))
        self.max_watchlist_size = int(self._get_env("MAX_WATCHLIST_SIZE", "50"))
        self.min_balance_reserve_usd = float(self._get_env("MIN_BALANCE_RESERVE_USD", "0.10"))
        
    def _get_env(self, key: str, default: str = "", required: bool = False) -> str:
        """جلب قيمة من البيئة مع التحقق من وجودها"""
        value = os.environ.get(key, default).strip()
        if required and not value:
            raise ValueError(f"المتغير {key} مطلوب في ملف .env!")
        return value
    
    def _load_wallets(self) -> List[WalletConfig]:
        """تحميل وتجهيز بيانات المحافظ"""
        private_keys = [k.strip() for k in self._get_env("PRIVATE_KEYS", required=True).split(",") if k.strip()]
        wallets = [w.strip() for w in self._get_env("WALLETS", required=True).split(",") if w.strip()]
        bot_tokens = [t.strip() for t in self._get_env("TELEGRAM_BOT_TOKENS", required=True).split(",") if t.strip()]
        chat_ids = [c.strip() for c in self._get_env("TELEGRAM_CHAT_IDS", required=True).split(",") if c.strip()]
        
        if not (len(private_keys) == len(wallets) == len(bot_tokens) == len(chat_ids)):
            raise ValueError(
                f"أعداد المفاتيح ({len(private_keys)})، المحافظ ({len(wallets)})، "
                f"البوتات ({len(bot_tokens)})، و Chat IDs ({len(chat_ids)}) غير متطابقة!"
            )
        
        return [
            WalletConfig(
                wallet=wallets[i],
                private_key=private_keys[i],
                bot_token=bot_tokens[i],
                chat_id=chat_ids[i],
            )
            for i in range(len(wallets))
        ]

# ---------------------------------------------------------------------------
# إعداد النظام
# ---------------------------------------------------------------------------

config = Config()

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={config.opensea_api_key}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

# إنشاء Web3 instances
w3_instances = {key: get_web3(cfg.rpc_url) for key, cfg in config.chains.items()}
stream_name_to_chain = {cfg.stream_chain_name: key for key, cfg in config.chains.items()}

# إدارة الأقفال
lock_manager = LockManager()

# تتبع الحالة
successful_mints: Dict[str, set] = {}
watchlist: Dict[str, WatchlistEntry] = {}
in_flight: set = set()

# Cache لسعر ETH
_eth_price_cache = {"value": None, "ts": 0, "ttl": 300}

# ---------------------------------------------------------------------------
# إدارة الأسعار
# ---------------------------------------------------------------------------

def get_eth_price_usd() -> float:
    """جلب سعر ETH مع cache"""
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < _eth_price_cache["ttl"]):
        return _eth_price_cache["value"]
    
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8,
        )
        resp.raise_for_status()
        price = float(resp.json()["ethereum"]["usd"])
        _eth_price_cache.update({"value": price, "ts": now})
        return price
    except Exception as e:
        log.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0

def calculate_price_usd(price_wei: int, eth_price_usd: float) -> float:
    """حساب السعر بالدولار من wei"""
    return (price_wei / 1e18) * eth_price_usd

# ---------------------------------------------------------------------------
# إدارة التخزين المؤقت للرفض
# ---------------------------------------------------------------------------

class RejectionManager:
    """إدارة فترة التبريد للمجموعات المرفوضة"""
    
    def __init__(self, cooldown_seconds: int):
        self.cooldown_seconds = cooldown_seconds
        self._rejections: Dict[str, float] = {}
    
    def is_rejected(self, slug: str) -> bool:
        """التحقق إذا كان slug في فترة التبريد"""
        ts = self._rejections.get(slug)
        if ts is None:
            return False
        
        if time.time() - ts >= self.cooldown_seconds:
            self._rejections.pop(slug, None)
            return False
        
        return True
    
    def mark_rejected(self, slug: str):
        """وضع علامة رفض لمجموعة"""
        self._rejections[slug] = time.time()
    
    def unmark_rejected(self, slug: str):
        """إزالة علامة الرفض"""
        self._rejections.pop(slug, None)
    
    def cleanup(self):
        """تنظيف الإدخالات القديمة"""
        now = time.time()
        expired = [s for s, t in self._rejections.items() if now - t >= self.cooldown_seconds]
        for s in expired:
            self._rejections.pop(s, None)

rejection_manager = RejectionManager(config.rejection_cooldown)

# ---------------------------------------------------------------------------
# إدارة API OpenSea
# ---------------------------------------------------------------------------

class OpenSeaAPI:
    """التعامل مع OpenSea API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.opensea.io/api/v2"
    
    def fetch_drop_detail(self, slug: str) -> Tuple[bool, Optional[Dict]]:
        """جلب تفاصيل drop محدد
        
        Returns:
            (found, detail) - found=True إذا وجد الـ drop
        """
        try:
            resp = requests.get(
                f"{self.base_url}/drops/{slug}",
                headers={"x-api-key": self.api_key},
                timeout=10,
            )
            
            if resp.status_code == 200:
                return True, resp.json()
            elif resp.status_code == 404:
                return False, None
            elif resp.status_code == 429:
                log.warning("Rate limit تم الوصول إليه في OpenSea API")
                return None, None
            else:
                log.warning(f"خطأ غير متوقع من OpenSea API: {resp.status_code}")
                return None, None
                
        except requests.Timeout:
            log.warning(f"Timeout في جلب تفاصيل {slug}")
            return None, None
        except Exception as e:
            log.warning(f"[Drops API] خطأ: {e}")
            return None, None

opensea_api = OpenSeaAPI(config.opensea_api_key)

# ---------------------------------------------------------------------------
# إدارة التيليجرام
# ---------------------------------------------------------------------------

class TelegramManager:
    """إدارة إرسال رسائل التيليجرام مع rate limiting"""
    
    def __init__(self, max_messages_per_second: int = 10):
        self.send_queue: asyncio.Queue = asyncio.Queue()
        self.max_per_second = max_messages_per_second
        self.last_send: Dict[str, float] = {}
        self._sender_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """بدء معالج الإرسال"""
        self._sender_task = asyncio.create_task(self._sender_loop())
    
    async def stop(self):
        """إيقاف معالج الإرسال"""
        if self._sender_task:
            self._sender_task.cancel()
            try:
                await self._sender_task
            except asyncio.CancelledError:
                pass
    
    def enqueue(self, bot_token: str, chat_id: str, text: str):
        """إضافة رسالة للطابور"""
        try:
            self.send_queue.put_nowait({
                "bot_token": bot_token,
                "chat_id": chat_id,
                "text": text,
                "timestamp": time.time(),
            })
        except asyncio.QueueFull:
            log.error("طابور التيليجرام ممتلئ!")
    
    def broadcast(self, text: str, wallets: List[WalletConfig]):
        """إرسال رسالة لجميع المحافظ"""
        for wallet in wallets:
            self.enqueue(wallet.bot_token, wallet.chat_id, text)
    
    async def _sender_loop(self):
        """معالج إرسال الرسائل مع rate limiting"""
        while True:
            try:
                msg = await self.send_queue.get()
                
                # Rate limiting
                await self._wait_for_rate_limit(msg["bot_token"])
                
                # إرسال الرسالة
                telegram_api = f"https://api.telegram.org/bot{msg['bot_token']}"
                response = await asyncio.to_thread(
                    requests.post,
                    f"{telegram_api}/sendMessage",
                    data={
                        "chat_id": msg["chat_id"],
                        "text": msg["text"],
                        "parse_mode": "HTML",
                    },
                    timeout=10,
                )
                
                if response.status_code != 200:
                    log.error(f"خطأ في إرسال تيليجرام: {response.status_code} - {response.text[:100]}")
                
                self.last_send[msg["bot_token"]] = time.time()
                self.send_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"خطأ في معالج التيليجرام: {e}")
                await asyncio.sleep(1)
    
    async def _wait_for_rate_limit(self, bot_token: str):
        """انتظار حتى يسمح rate limit بالإرسال"""
        if bot_token in self.last_send:
            elapsed = time.time() - self.last_send[bot_token]
            min_interval = 1.0 / self.max_per_second
            
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)

telegram_manager = TelegramManager()

# ---------------------------------------------------------------------------
# بناء الرسائل
# ---------------------------------------------------------------------------

def build_success_message(wallet_config: WalletConfig, result: dict, detail: dict) -> str:
    """بناء رسالة نجاح مخصصة لمحفظة"""
    name = detail.get("collection_name") or detail.get("collection_slug", "Unknown")
    url = detail.get("opensea_url", "")
    chain_label = "Robinhood Chain" if wallet_config.chain_key == "robinhood" else "Ethereum Mainnet"
    wallet_short = f"{wallet_config.wallet[:6]}...{wallet_config.wallet[-4:]}"
    
    return (
        f"✅ <b>تم الشراء بنجاح!</b> ({chain_label})\n\n"
        f"المحفظة: <code>{wallet_short}</code>\n"
        f"المجموعة: <b>{name}</b>\n"
        f"الكمية: {result.get('quantity', 0)}\n"
        f"رسوم الغاز: ${result.get('gas_fee_usd', 0):.4f}\n"
        f"المعاملة: {result.get('tx_hash', 'N/A')}\n"
        f"🔗 {url}"
    )

def build_watching_message(detail: dict, reason: str) -> str:
    """بناء رسالة مراقبة"""
    name = detail.get("collection_name") or detail.get("collection_slug", "Unknown")
    return (
        f"👀 <b>تحت المراقبة</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"السبب: {reason}\n"
        f"سنحاول الشراء تلقائيًا فور توفر الفرصة."
    )

def build_price_drop_message(detail: dict, old_price: float, new_price: float) -> str:
    """بناء رسالة انخفاض السعر"""
    name = detail.get("collection_name") or detail.get("collection_slug", "Unknown")
    return (
        f"🎉 <b>انخفض السعر!</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"السعر القديم: ${old_price:.4f}\n"
        f"السعر الجديد: ${new_price:.4f}\n"
        f"جاري محاولة الشراء..."
    )

def build_gave_up_message(detail: dict, reason: str) -> str:
    """بناء رسالة إلغاء"""
    name = detail.get("collection_name") or detail.get("collection_slug", "Unknown")
    return (
        f"❌ <b>انتهت الفرصة</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"السبب: {reason}"
    )

# ---------------------------------------------------------------------------
# منطق الشراء
# ---------------------------------------------------------------------------

async def purchase_for_wallet(
    wallet_config: WalletConfig,
    w3: Web3,
    slug: str,
    contract_address: str,
    price_wei: int,
    max_per_wallet: Optional[int],
    remaining: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> dict:
    """شراء لمحفظة واحدة مع إدارة القفل"""
    
    wallet_addr = wallet_config.wallet.lower()
    lock = lock_manager.get_lock(wallet_addr)
    
    async with lock:
        try:
            # التحقق من عدم الشراء المسبق داخل القفل
            if wallet_addr in successful_mints.get(slug, set()):
                return {
                    "success": False,
                    "wallet": wallet_config.wallet,
                    "reason": "already_bought"
                }
            
            # محاولة الشراء مع retry
            result = await purchase_with_retry(
                w3=w3,
                private_key=wallet_config.private_key,
                wallet_address=wallet_config.wallet,
                nft_contract=contract_address,
                price_wei_per_token=price_wei,
                max_per_wallet=max_per_wallet,
                remaining_supply=remaining,
                eth_price_usd=eth_price_usd,
                max_gas_fee_usd=max_gas_fee_usd,
                max_retries=config.chains[wallet_config.chain_key].max_retries,
                retry_delay_base=config.chains[wallet_config.chain_key].retry_delay_base,
            )
            
            # إذا نجح الشراء، سجل وأرسل إشعار
            if result.get("success"):
                if slug not in successful_mints:
                    successful_mints[slug] = set()
                successful_mints[slug].add(wallet_addr)
                
                # إرسال إشعار النجاح لهذه المحفظة فقط
                message = build_success_message(wallet_config, result, wallet_config.current_detail)
                telegram_manager.enqueue(
                    wallet_config.bot_token,
                    wallet_config.chat_id,
                    message
                )
            
            return result
            
        finally:
            lock_manager.release_lock(wallet_addr)

async def try_buy_now_multi_wallet(
    slug: str,
    chain_key: str,
    detail: dict,
) -> Optional[List[dict]]:
    """محاولة الشراء لجميع المحافظ"""
    
    stage = detail.get("active_stage")
    if not stage:
        return None
    
    # حساب الكمية المتبقية
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    
    if remaining <= 0:
        return [{"success": False, "reason": "sold_out"}]
    
    # التحقق من وجود عنوان العقد
    contract_address = detail.get("contract_address")
    if not contract_address:
        return [{"success": False, "reason": "no_contract_address"}]
    
    w3 = w3_instances[chain_key]
    eth_price_usd = get_eth_price_usd()
    
    # جلب السعر من السلسلة
    onchain_price = await asyncio.to_thread(
        get_onchain_public_price_wei,
        w3,
        contract_address
    )
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
    
    # التحقق من أن السعر مجاني أو مهمل
    if not is_free_or_negligible(price_wei, eth_price_usd):
        return None  # مدفوع -> للمراقبة
    
    # تحديد الحد الأقصى للمحفظة
    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    
    max_gas_fee_usd = config.chains[chain_key].max_gas_fee_usd
    
    # تجهيز المحافظ التي لم تشترِ بعد
    already_bought = successful_mints.get(slug, set())
    pending_wallets = [
        w for w in config.wallets
        if w.wallet.lower() not in already_bought
    ]
    
    if not pending_wallets:
        return [{"success": False, "reason": "all_wallets_completed"}]
    
    # تحديث سياق المحافظ
    for wallet in pending_wallets:
        wallet.chain_key = chain_key
        wallet.current_detail = detail
    
    # تنفيذ الشراء المتوازي
    tasks = [
        purchase_for_wallet(
            wallet_config=wallet,
            w3=w3,
            slug=slug,
            contract_address=contract_address,
            price_wei=price_wei,
            max_per_wallet=max_per_wallet,
            remaining=remaining,
            eth_price_usd=eth_price_usd,
            max_gas_fee_usd=max_gas_fee_usd,
        )
        for wallet in pending_wallets
    ]
    
    # استخدام return_exceptions لتجنب فشل الكل
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # معالجة الاستثناءات
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            log.error(f"خطأ في شراء المحفظة {pending_wallets[i].wallet[:8]}: {result}")
            processed_results.append({
                "success": False,
                "wallet": pending_wallets[i].wallet,
                "reason": "exception",
                "error": str(result),
            })
        else:
            processed_results.append(result)
    
    return processed_results

# ---------------------------------------------------------------------------
# فحص تويتر مع cache
# ---------------------------------------------------------------------------

async def check_twitter(slug: str, entry: Optional[WatchlistEntry] = None) -> Tuple[bool, Optional[str]]:
    """فحص تويتر مع استخدام cache إذا كان متاحاً"""
    
    # استخدام cache إذا كان متاحاً
    if entry and entry.twitter_checked:
        return bool(entry.twitter_username), entry.twitter_username
    
    # فحص جديد
    twitter_username = await asyncio.to_thread(
        get_twitter_username_from_opensea,
        slug,
        config.opensea_api_key
    )
    
    # تحديث entry إذا كان موجوداً
    if entry:
        entry.twitter_checked = True
        entry.twitter_username = twitter_username
    
    return bool(twitter_username), twitter_username

# ---------------------------------------------------------------------------
# تقييم المينتات الجديدة
# ---------------------------------------------------------------------------

async def evaluate_new_mint(slug: str, chain_key: str):
    """تقييم مينت جديد واتخاذ قرار الشراء أو المراقبة"""
    
    # التحقق من الحالة
    if (
        len(successful_mints.get(slug, set())) >= len(config.wallets)
        or slug in watchlist
        or slug in in_flight
        or rejection_manager.is_rejected(slug)
    ):
        return
    
    in_flight.add(slug)
    
    try:
        # جلب تفاصيل المينت
        found, detail = await asyncio.to_thread(
            opensea_api.fetch_drop_detail,
            slug
        )
        
        if not found or not detail or not detail.get("is_minting"):
            return
        
        # التحقق من المرحلة النشطة
        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return
        
        # فحص السعر
        w3 = w3_instances[chain_key]
        eth_price_usd = get_eth_price_usd()
        contract_address = detail.get("contract_address")
        
        price_wei = None
        price_usd = 0.0
        
        if contract_address:
            onchain_price = await asyncio.to_thread(
                get_onchain_public_price_wei,
                w3,
                contract_address
            )
            price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
            price_usd = calculate_price_usd(price_wei, eth_price_usd)
        
        # ✅ المينت مجاني - فحص تويتر والشراء
        if price_wei is None or is_free_or_negligible(price_wei, eth_price_usd):
            log.info(f"🆓 '{slug}' مجاني (${price_usd:.4f}) - فحص تويتر والشراء")
            
            # فحص تويتر
            has_twitter, twitter_username = await check_twitter(slug)
            
            if not has_twitter:
                log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X مربوط.")
                rejection_manager.mark_rejected(slug)
                return
            
            log.info(f"✅ '{slug}': يوجد حساب X (@{twitter_username}) — جاري الشراء.")
            
            # محاولة الشراء
            results = await try_buy_now_multi_wallet(slug, chain_key, detail)
            
            if results is None:
                await add_to_watchlist(slug, chain_key, detail, "السعر الحالي مدفوع")
                return
            
            if len(successful_mints.get(slug, set())) < len(config.wallets):
                await add_to_watchlist(slug, chain_key, detail, "لم تكتمل كل المحافظ")
        
        # ⚠️ المينت مدفوع - قد يصبح مجاني لاحقاً
        else:
            log.info(f"💰 '{slug}' مدفوع (${price_usd:.4f}) - إضافة للمراقبة")
            
            # فحص تويتر مسبقاً لتوفير الوقت لاحقاً
            has_twitter, twitter_username = await check_twitter(slug)
            
            if has_twitter:
                # إضافة للمراقبة مع معلومات تويتر
                entry = WatchlistEntry(
                    chain_key=chain_key,
                    detail=detail,
                    added_at=time.time(),
                    twitter_checked=True,
                    twitter_username=twitter_username,
                    was_paid=True,
                    last_price_usd=price_usd,
                )
                watchlist[slug] = entry
                
                # إشعار المراقبة
                message = build_watching_message(
                    detail, 
                    f"مدفوع حالياً (${price_usd:.4f}) - نراقب لاحتمال انخفاض السعر"
                )
                telegram_manager.broadcast(message, config.wallets)
            else:
                # لا يوجد تويتر - تجاهل نهائي
                log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X مربوط.")
                rejection_manager.mark_rejected(slug)
    
    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}", exc_info=True)
    finally:
        in_flight.discard(slug)

async def add_to_watchlist(slug: str, chain_key: str, detail: dict, reason: str):
    """إضافة عنصر لقائمة المراقبة"""
    
    # التحقق من حجم قائمة المراقبة
    if len(watchlist) >= config.max_watchlist_size:
        log.warning(f"قائمة المراقبة ممتلئة ({config.max_watchlist_size}). تجاهل '{slug}'")
        return
    
    entry = WatchlistEntry(
        chain_key=chain_key,
        detail=detail,
        added_at=time.time(),
    )
    watchlist[slug] = entry
    
    # إشعار المراقبة
    message = build_watching_message(detail, reason)
    telegram_manager.broadcast(message, config.wallets)

# ---------------------------------------------------------------------------
# حلقة المراقبة
# ---------------------------------------------------------------------------

async def watch_loop():
    """مراقبة المينتات في قائمة المراقبة والتحقق من تغير الأسعار"""
    
    while True:
        await asyncio.sleep(config.watch_poll_interval)
        
        if not watchlist:
            continue
        
        # تنظيف دوري
        rejection_manager.cleanup()
        
        for slug in list(watchlist.keys()):
            entry = watchlist.get(slug)
            if not entry:
                continue
            
            # التحقق من شروط الإيقاف
            if slug in in_flight:
                continue
            
            if len(successful_mints.get(slug, set())) >= len(config.wallets):
                watchlist.pop(slug, None)
                continue
            
            # التحقق من عدد المحاولات
            if entry.check_count >= entry.max_checks:
                watchlist.pop(slug, None)
                message = build_gave_up_message(entry.detail, "تجاوز الحد الأقصى للمحاولات")
                telegram_manager.broadcast(message, config.wallets)
                continue
            
            in_flight.add(slug)
            entry.check_count += 1
            entry.last_check = time.time()
            
            try:
                # جلب التفاصيل المحدثة
                found, fresh_detail = await asyncio.to_thread(
                    opensea_api.fetch_drop_detail,
                    slug
                )
                
                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    watchlist.pop(slug, None)
                    message = build_gave_up_message(entry.detail, "المينت لم يعد نشطًا")
                    telegram_manager.broadcast(message, config.wallets)
                    continue
                
                # التحقق من المرحلة
                stage = fresh_detail.get("active_stage")
                if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
                    watchlist.pop(slug, None)
                    message = build_gave_up_message(fresh_detail, "انتهت المرحلة")
                    telegram_manager.broadcast(message, config.wallets)
                    continue
                
                # فحص السعر الحالي
                w3 = w3_instances[entry.chain_key]
                eth_price_usd = get_eth_price_usd()
                contract_address = fresh_detail.get("contract_address")
                
                if not contract_address:
                    continue
                
                onchain_price = await asyncio.to_thread(
                    get_onchain_public_price_wei,
                    w3,
                    contract_address
                )
                
                if onchain_price is None:
                    continue
                
                price_wei = onchain_price
                price_usd = calculate_price_usd(price_wei, eth_price_usd)
                
                # ✅ السعر أصبح مجاني/منخفض!
                if is_free_or_negligible(price_wei, eth_price_usd):
                    # إذا كان المينت كان مدفوعاً وأصبح مجانياً
                    if entry.was_paid and entry.last_price_usd > 0:
                        log.info(f"🎉 '{slug}' انخفض سعره من ${entry.last_price_usd:.4f} إلى ${price_usd:.4f}!")
                        
                        # إشعار بانخفاض السعر
                        message = build_price_drop_message(
                            fresh_detail,
                            entry.last_price_usd,
                            price_usd
                        )
                        telegram_manager.broadcast(message, config.wallets)
                    
                    # التحقق من تويتر (باستخدام cache إذا كان متاحاً)
                    has_twitter, twitter_username = await check_twitter(slug, entry)
                    
                    if not has_twitter:
                        log.info(f"⏭️ '{slug}': لا يوجد حساب X مربوط.")
                        watchlist.pop(slug, None)
                        rejection_manager.mark_rejected(slug)
                        continue
                    
                    log.info(f"✅ '{slug}': يوجد حساب X (@{twitter_username}) — جاري الشراء.")
                    
                    # محاولة الشراء
                    results = await try_buy_now_multi_wallet(
                        slug,
                        entry.chain_key,
                        fresh_detail
                    )
                    
                    if results is None:
                        # لم يكتمل الشراء - تحديث البيانات
                        entry.detail = fresh_detail
                        entry.was_paid = False
                        entry.last_price_usd = price_usd
                    elif len(successful_mints.get(slug, set())) >= len(config.wallets):
                        # اكتمل الشراء لجميع المحافظ
                        watchlist.pop(slug, None)
                        log.info(f"🎉 اكتمل الشراء لجميع المحافظ في '{slug}'")
                    else:
                        # تحديث البيانات
                        entry.detail = fresh_detail
                        entry.was_paid = False
                        entry.last_price_usd = price_usd
                
                # ⚠️ المينت لا يزال مدفوعاً
                else:
                    # تحديث البيانات
                    entry.detail = fresh_detail
                    entry.was_paid = True
                    entry.last_price_usd = price_usd
                    
                    # تسجيل التغير في السعر
                    if entry.check_count % 10 == 0:  # كل 10 فحوصات
                        log.info(f"💰 '{slug}' لا يزال مدفوعاً (${price_usd:.4f}) - فحص #{entry.check_count}")
            
            except Exception as e:
                log.error(f"خطأ بدورة مراقبة '{slug}': {e}", exc_info=True)
            finally:
                in_flight.discard(slug)

# ---------------------------------------------------------------------------
# الاستماع لـ OpenSea Stream
# ---------------------------------------------------------------------------

class OpenSeaStream:
    """إدارة الاتصال بـ OpenSea WebSocket"""
    
    def __init__(self, url: str):
        self.url = url
        self.msg_ref = 0
        self.backoff = 1
        self.max_backoff = 60
    
    async def listen(self):
        """الاستماع للأحداث"""
        
        while True:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=None,
                    open_timeout=15,
                ) as ws:
                    log.info(f"متصل بـ OpenSea Stream — يراقب {len(config.wallets)} محافظ.")
                    
                    # إعادة تعيين backoff عند نجاح الاتصال
                    self.backoff = 1
                    
                    # الانضمام للقناة
                    await self._join_channel(ws)
                    
                    last_heartbeat = time.time()
                    
                    while True:
                        # إرسال heartbeat
                        if time.time() - last_heartbeat > config.heartbeat_interval:
                            await self._send_heartbeat(ws)
                            last_heartbeat = time.time()
                        
                        # استقبال الرسائل
                        try:
                            raw = await asyncio.wait_for(
                                ws.recv(),
                                timeout=config.recv_timeout
                            )
                        except asyncio.TimeoutError:
                            continue
                        
                        # معالجة الرسالة
                        await self._process_message(raw)
            
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال خلال {self.backoff} ثانية...")
                await asyncio.sleep(self.backoff)
                self.backoff = min(self.backoff * 2, self.max_backoff)
            
            except Exception as e:
                log.error(f"خطأ غير متوقع: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    async def _join_channel(self, ws):
        """الانضمام لقناة المجموعات"""
        join_ref = str(self.msg_ref)
        await ws.send(json.dumps([
            join_ref,
            join_ref,
            "collection:*",
            "phx_join",
            {}
        ]))
        self.msg_ref += 1
    
    async def _send_heartbeat(self, ws):
        """إرسال heartbeat"""
        hb_ref = str(self.msg_ref)
        await ws.send(json.dumps([
            None,
            hb_ref,
            "phoenix",
            "heartbeat",
            {}
        ]))
        self.msg_ref += 1
    
    async def _process_message(self, raw: str):
        """معالجة رسالة من WebSocket"""
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return
        
        # التحقق من شكل الرسالة
        if not isinstance(parsed, list) or len(parsed) != 5:
            return
        
        _, _, _, event_name, payload_wrapper = parsed
        
        if event_name != "item_transferred":
            return
        
        # استخراج البيانات
        payload = (payload_wrapper or {}).get("payload") or {}
        item = payload.get("item", {}) or {}
        stream_chain_name = (item.get("chain", {}) or {}).get("name", "")
        
        # التحقق من السلسلة
        chain_key = stream_name_to_chain.get(stream_chain_name)
        if chain_key is None:
            return
        
        # التحقق من أن النقل من العنوان الصفري (mint جديد)
        from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
        if from_address != ZERO_ADDRESS:
            return
        
        # استخراج slug
        slug = (payload.get("collection", {}) or {}).get("slug", "")
        if not slug:
            return
        
        # إذا كان slug في قائمة المراقبة، قم بإلغاء rejection cooldown
        if slug in watchlist:
            rejection_manager.unmark_rejected(slug)
        
        # إنشاء مهمة للتقييم
        asyncio.create_task(evaluate_new_mint(slug, chain_key))

# ---------------------------------------------------------------------------
# الدوال المساعدة
# ---------------------------------------------------------------------------

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    """التحقق من أن السعر مجاني أو مهمل"""
    price_usd = calculate_price_usd(price_wei, eth_price_usd)
    return price_usd < config.free_price_threshold

def parse_iso(ts: str) -> Optional[datetime]:
    """تحويل timestamp إلى datetime"""
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

# ---------------------------------------------------------------------------
# التشغيل الرئيسي
# ---------------------------------------------------------------------------

async def run():
    """تشغيل النظام"""
    
    log.info(f"بدء تشغيل النظام مع {len(config.wallets)} محافظ")
    
    # بدء معالج التيليجرام
    await telegram_manager.start()
    
    if not config.bot_enabled:
        log.warning("🔴 BOT_ENABLED=false")
        telegram_manager.broadcast(
            "🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false).",
            config.wallets
        )
        # انتظار حتى يتم إرسال الرسالة
        await telegram_manager.send_queue.join()
        return
    
    # إرسال رسالة التشغيل
    telegram_manager.broadcast(
        "✅ تم تشغيل المحفظة الخاصة بك بنجاح وربطها بهذا البوت!",
        config.wallets
    )
    
    # بدء المراقبة والاستماع
    stream = OpenSeaStream(STREAM_URL)
    
    try:
        await asyncio.gather(
            stream.listen(),
            watch_loop(),
        )
    except asyncio.CancelledError:
        log.info("تم إلغاء المهام")
    except Exception as e:
        log.critical(f"خطأ حرج: {e}", exc_info=True)
    finally:
        await telegram_manager.stop()

def main():
    """الدالة الرئيسية"""
    backoff = 2
    max_backoff = 30
    
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدويًا.")
            break
        except Exception as e:
            log.critical(f"توقف غير متوقع: {e}", exc_info=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
            continue
        else:
            break

if __name__ == "__main__":
    main()
