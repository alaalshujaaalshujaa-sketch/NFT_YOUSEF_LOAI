"""
النظام الكامل — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها:
  - يكتشف مينتات اليوم على Robinhood + Ethereum
  - يشتري لجميع المحافظ المعرفة بالتوازي (Parallel Execution)
  - يرسل إشعارات فقط للمينتات القادمة وعند نجاح الشراء
  - يتعامل مع المينتات التي لم تبدأ بعد أو انتهت
  - يعرض روابط المينت في رسائل التيليجرام
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum

import requests
import websockets
from dotenv import load_dotenv
from web3 import Web3

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_mint_times,
    LockManager,
    purchase_with_retry,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

# ---------------------------------------------------------------------------
# الإعدادات والتكوين
# ---------------------------------------------------------------------------

class WatchPriority(Enum):
    """أولوية المراقبة"""
    URGENT = "urgent"
    NORMAL = "normal"
    LOW = "low"

class MintStatus(Enum):
    """حالة المينت"""
    NOT_STARTED = "not_started"
    ACTIVE = "active"
    ENDED = "ended"
    UNKNOWN = "unknown"

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
    max_checks: int = 500
    twitter_checked: bool = False
    twitter_username: Optional[str] = None
    was_paid: bool = False
    last_price_usd: float = 0.0
    priority: WatchPriority = WatchPriority.NORMAL
    mint_status: MintStatus = MintStatus.UNKNOWN
    mint_start_time: Optional[int] = None
    mint_end_time: Optional[int] = None
    waiting_since: Optional[float] = None
    start_notified: bool = False

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
        
        # إعدادات المراقبة - استخدام القيم الافتراضية فقط
        self.heartbeat_interval = int(self._get_env("HEARTBEAT_INTERVAL", "20"))
        self.recv_timeout = int(self._get_env("RECV_TIMEOUT", "5"))
        self.watch_poll_interval = int(self._get_env("WATCH_POLL_INTERVAL", "15"))
        self.rejection_cooldown = int(self._get_env("REJECTION_COOLDOWN", "120"))
        self.free_price_threshold = float(self._get_env("FREE_PRICE_THRESHOLD", "0.01"))
        self.max_watchlist_size = int(self._get_env("MAX_WATCHLIST_SIZE", "50"))
        self.min_balance_reserve_usd = float(self._get_env("MIN_BALANCE_RESERVE_USD", "0.10"))
        
        # قيم ثابتة داخل الكود
        self.urgent_price_threshold = 0.05
        self.normal_check_interval = 30
        self.low_check_interval = 60
        self.max_wait_time = 3600  # ساعة كحد أقصى
        self.notify_before_start = 300  # إشعار قبل 5 دقائق
        
    def _get_env(self, key: str, default: str = "", required: bool = False) -> str:
        value = os.environ.get(key, default).strip()
        if required and not value:
            raise ValueError(f"المتغير {key} مطلوب في ملف .env!")
        return value
    
    def _load_wallets(self) -> List[WalletConfig]:
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

w3_instances = {key: get_web3(cfg.rpc_url) for key, cfg in config.chains.items()}
stream_name_to_chain = {cfg.stream_chain_name: key for key, cfg in config.chains.items()}

lock_manager = LockManager()

successful_mints: Dict[str, set] = {}
watchlist: Dict[str, WatchlistEntry] = {}
in_flight: Set[str] = set()
known_slugs: Set[str] = set()

_eth_price_cache = {"value": None, "ts": 0, "ttl": 300}

# ---------------------------------------------------------------------------
# إدارة الأسعار
# ---------------------------------------------------------------------------

def get_eth_price_usd() -> float:
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
    return (price_wei / 1e18) * eth_price_usd

def determine_priority(price_usd: float) -> WatchPriority:
    if price_usd < config.urgent_price_threshold:
        return WatchPriority.URGENT
    elif price_usd < 0.5:
        return WatchPriority.NORMAL
    else:
        return WatchPriority.LOW

# ---------------------------------------------------------------------------
# فحص حالة المينت
# ---------------------------------------------------------------------------

def get_mint_status(w3: Web3, contract_address: str) -> Tuple[MintStatus, Optional[int], Optional[int]]:
    try:
        start_time, end_time = get_mint_times(w3, contract_address)
        current_time = int(time.time())
        
        if start_time and current_time < start_time:
            return MintStatus.NOT_STARTED, start_time, end_time
        
        if end_time and current_time > end_time:
            return MintStatus.ENDED, start_time, end_time
        
        if start_time and end_time:
            return MintStatus.ACTIVE, start_time, end_time
        
        return MintStatus.UNKNOWN, start_time, end_time
        
    except Exception as e:
        log.warning(f"[حالة المينت] تعذر الفحص: {e}")
        return MintStatus.UNKNOWN, None, None

# ---------------------------------------------------------------------------
# إدارة التخزين المؤقت للرفض
# ---------------------------------------------------------------------------

class RejectionManager:
    def __init__(self, cooldown_seconds: int):
        self.cooldown_seconds = cooldown_seconds
        self._rejections: Dict[str, float] = {}
    
    def is_rejected(self, slug: str) -> bool:
        ts = self._rejections.get(slug)
        if ts is None:
            return False
        
        if time.time() - ts >= self.cooldown_seconds:
            self._rejections.pop(slug, None)
            return False
        
        return True
    
    def mark_rejected(self, slug: str):
        self._rejections[slug] = time.time()
    
    def unmark_rejected(self, slug: str):
        self._rejections.pop(slug, None)
    
    def cleanup(self):
        now = time.time()
        expired = [s for s, t in self._rejections.items() if now - t >= self.cooldown_seconds]
        for s in expired:
            self._rejections.pop(s, None)

rejection_manager = RejectionManager(config.rejection_cooldown)

# ---------------------------------------------------------------------------
# إدارة API OpenSea
# ---------------------------------------------------------------------------

class OpenSeaAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.opensea.io/api/v2"
        self._cache: Dict[str, Tuple[float, Dict]] = {}
        self._cache_ttl = 30
    
    def fetch_drop_detail(self, slug: str, use_cache: bool = True) -> Tuple[bool, Optional[Dict]]:
        if use_cache and slug in self._cache:
            timestamp, data = self._cache[slug]
            if time.time() - timestamp < self._cache_ttl:
                return True, data
        
        try:
            resp = requests.get(
                f"{self.base_url}/drops/{slug}",
                headers={"x-api-key": self.api_key},
                timeout=10,
            )
            
            if resp.status_code == 200:
                data = resp.json()
                self._cache[slug] = (time.time(), data)
                return True, data
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
    
    def cleanup_cache(self):
        now = time.time()
        expired = [s for s, (ts, _) in self._cache.items() if now - ts > self._cache_ttl * 2]
        for s in expired:
            self._cache.pop(s, None)

opensea_api = OpenSeaAPI(config.opensea_api_key)

# ---------------------------------------------------------------------------
# إدارة التيليجرام
# ---------------------------------------------------------------------------

class TelegramManager:
    def __init__(self, max_messages_per_second: int = 10):
        self.send_queue: asyncio.Queue = asyncio.Queue()
        self.max_per_second = max_messages_per_second
        self.last_send: Dict[str, float] = {}
        self._sender_task: Optional[asyncio.Task] = None
        self._recent_messages: Dict[str, float] = {}
    
    async def start(self):
        self._sender_task = asyncio.create_task(self._sender_loop())
    
    async def stop(self):
        if self._sender_task:
            self._sender_task.cancel()
            try:
                await self._sender_task
            except asyncio.CancelledError:
                pass
    
    def enqueue(self, bot_token: str, chat_id: str, text: str, deduplicate: bool = True):
        if deduplicate:
            msg_key = f"{bot_token}:{chat_id}:{text[:100]}"
            last_time = self._recent_messages.get(msg_key, 0)
            if time.time() - last_time < 5:
                return
            self._recent_messages[msg_key] = time.time()
        
        try:
            self.send_queue.put_nowait({
                "bot_token": bot_token,
                "chat_id": chat_id,
                "text": text,
                "timestamp": time.time(),
            })
        except asyncio.QueueFull:
            log.error("طابور التيليجرام ممتلئ!")
    
    def broadcast(self, text: str, wallets: List[WalletConfig], deduplicate: bool = True):
        for wallet in wallets:
            self.enqueue(wallet.bot_token, wallet.chat_id, text, deduplicate)
    
    async def _sender_loop(self):
        while True:
            try:
                msg = await self.send_queue.get()
                
                await self._wait_for_rate_limit(msg["bot_token"])
                
                telegram_api = f"https://api.telegram.org/bot{msg['bot_token']}"
                response = await asyncio.to_thread(
                    requests.post,
                    f"{telegram_api}/sendMessage",
                    data={
                        "chat_id": msg["chat_id"],
                        "text": msg["text"],
                        "parse_mode": "HTML",
                        "disable_web_page_preview": False,
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
        if bot_token in self.last_send:
            elapsed = time.time() - self.last_send[bot_token]
            min_interval = 1.0 / self.max_per_second
            
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)

telegram_manager = TelegramManager()

# ---------------------------------------------------------------------------
# دوال بناء الروابط
# ---------------------------------------------------------------------------

def get_opensea_url(detail: dict) -> str:
    url = detail.get("opensea_url", "")
    if url:
        return url
    
    slug = detail.get("collection_slug") or detail.get("collection_name", "")
    if slug:
        return f"https://opensea.io/collection/{slug}"
    
    return ""

def get_tx_url(chain_key: str, tx_hash: str) -> str:
    if not tx_hash:
        return ""
    
    if chain_key == "ethereum":
        return f"https://etherscan.io/tx/{tx_hash}"
    elif chain_key == "robinhood":
        return f"https://robinhoodscan.com/tx/{tx_hash}"
    
    return ""

# ---------------------------------------------------------------------------
# بناء الرسائل (فقط للبدء والشراء)
# ---------------------------------------------------------------------------

def build_start_notification(detail: dict, wait_seconds: int) -> str:
    """بناء رسالة إشعار بدء المينت"""
    name = detail.get("collection_name") or detail.get("collection_slug", "Unknown")
    opensea_url = get_opensea_url(detail)
    
    wait_minutes = wait_seconds // 60
    wait_secs = wait_seconds % 60
    
    if wait_minutes > 0:
        time_str = f"{wait_minutes} دقيقة و {wait_secs} ثانية"
    else:
        time_str = f"{wait_secs} ثانية"
    
    message = (
        f"🔔 <b>مينت قادم!</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"يبدأ خلال: {time_str}\n"
        f"سنقوم بالشراء تلقائياً عند البدء."
    )
    
    if opensea_url:
        message += f"\n\n🔗 <a href='{opensea_url}'>OpenSea</a>"
    
    return message

def build_success_message(wallet_config: WalletConfig, result: dict, detail: dict) -> str:
    """بناء رسالة نجاح الشراء"""
    name = detail.get("collection_name") or detail.get("collection_slug", "Unknown")
    opensea_url = get_opensea_url(detail)
    chain_label = "Robinhood Chain" if wallet_config.chain_key == "robinhood" else "Ethereum Mainnet"
    wallet_short = f"{wallet_config.wallet[:6]}...{wallet_config.wallet[-4:]}"
    tx_hash = result.get('tx_hash', '')
    tx_url = get_tx_url(wallet_config.chain_key, tx_hash)
    
    message = (
        f"✅ <b>تم الشراء بنجاح!</b> ({chain_label})\n\n"
        f"المحفظة: <code>{wallet_short}</code>\n"
        f"المجموعة: <b>{name}</b>\n"
        f"الكمية: {result.get('quantity', 0)}\n"
        f"رسوم الغاز: ${result.get('gas_fee_usd', 0):.4f}\n"
    )
    
    if opensea_url:
        message += f"\n🔗 <a href='{opensea_url}'>OpenSea</a>"
    
    if tx_url:
        message += f"\n📝 <a href='{tx_url}'>المعاملة</a>"
    
    return message

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
            if wallet_addr in successful_mints.get(slug, set()):
                return {
                    "success": False,
                    "wallet": wallet_config.wallet,
                    "reason": "already_bought"
                }
            
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
            
            if result.get("success"):
                if slug not in successful_mints:
                    successful_mints[slug] = set()
                successful_mints[slug].add(wallet_addr)
                
                # إرسال إشعار النجاح فقط
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
    
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    
    if remaining <= 0:
        return [{"success": False, "reason": "sold_out"}]
    
    contract_address = detail.get("contract_address")
    if not contract_address:
        return [{"success": False, "reason": "no_contract_address"}]
    
    w3 = w3_instances[chain_key]
    eth_price_usd = get_eth_price_usd()
    
    mint_status, start_time, end_time = get_mint_status(w3, contract_address)
    
    if mint_status == MintStatus.NOT_STARTED:
        wait_seconds = start_time - int(time.time())
        log.info(f"⏰ '{slug}' لم يبدأ بعد. الانتظار {wait_seconds} ثانية")
        return [{"success": False, "reason": "mint_not_started", "wait_seconds": wait_seconds}]
    
    if mint_status == MintStatus.ENDED:
        log.info(f"⏰ '{slug}' انتهى بالفعل")
        return [{"success": False, "reason": "mint_ended"}]
    
    onchain_price = await asyncio.to_thread(
        get_onchain_public_price_wei,
        w3,
        contract_address
    )
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
    
    if not is_free_or_negligible(price_wei, eth_price_usd):
        return None
    
    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    
    max_gas_fee_usd = config.chains[chain_key].max_gas_fee_usd
    
    already_bought = successful_mints.get(slug, set())
    pending_wallets = [
        w for w in config.wallets
        if w.wallet.lower() not in already_bought
    ]
    
    if not pending_wallets:
        return [{"success": False, "reason": "all_wallets_completed"}]
    
    for wallet in pending_wallets:
        wallet.chain_key = chain_key
        wallet.current_detail = detail
    
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
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
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
    if entry and entry.twitter_checked:
        return bool(entry.twitter_username), entry.twitter_username
    
    twitter_username = await asyncio.to_thread(
        get_twitter_username_from_opensea,
        slug,
        config.opensea_api_key
    )
    
    if entry:
        entry.twitter_checked = True
        entry.twitter_username = twitter_username
    
    return bool(twitter_username), twitter_username

# ---------------------------------------------------------------------------
# تقييم المينتات الجديدة
# ---------------------------------------------------------------------------

async def evaluate_new_mint(slug: str, chain_key: str):
    """تقييم مينت جديد واتخاذ قرار الشراء أو المراقبة"""
    
    if slug in known_slugs:
        return
    
    if (
        len(successful_mints.get(slug, set())) >= len(config.wallets)
        or slug in watchlist
        or slug in in_flight
        or rejection_manager.is_rejected(slug)
    ):
        return
    
    in_flight.add(slug)
    known_slugs.add(slug)
    
    try:
        found, detail = await asyncio.to_thread(
            opensea_api.fetch_drop_detail,
            slug,
            False
        )
        
        if not found or not detail or not detail.get("is_minting"):
            return
        
        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return
        
        w3 = w3_instances[chain_key]
        eth_price_usd = get_eth_price_usd()
        contract_address = detail.get("contract_address")
        
        price_wei = None
        price_usd = 0.0
        
        if contract_address:
            mint_status, start_time, end_time = get_mint_status(w3, contract_address)
            
            # المينت لم يبدأ بعد
            if mint_status == MintStatus.NOT_STARTED:
                wait_seconds = start_time - int(time.time())
                
                if wait_seconds < config.max_wait_time:
                    log.info(f"⏰ '{slug}' سيبدأ خلال {wait_seconds} ثانية - إضافة للمراقبة")
                    
                    entry = WatchlistEntry(
                        chain_key=chain_key,
                        detail=detail,
                        added_at=time.time(),
                        was_paid=True,
                        last_price_usd=0,
                        priority=WatchPriority.URGENT,
                        mint_status=MintStatus.NOT_STARTED,
                        mint_start_time=start_time,
                        mint_end_time=end_time,
                        waiting_since=time.time(),
                    )
                    watchlist[slug] = entry
                    
                    # إرسال إشعار البدء فقط إذا كان سيبدأ خلال 5 دقائق
                    if wait_seconds <= config.notify_before_start:
                        message = build_start_notification(detail, wait_seconds)
                        telegram_manager.broadcast(message, config.wallets)
                else:
                    log.info(f"⏰ '{slug}' سيبدأ بعد وقت طويل ({wait_seconds} ثانية) - تجاهل")
                
                return
            
            # المينت انتهى
            if mint_status == MintStatus.ENDED:
                log.info(f"⏰ '{slug}' انتهى بالفعل - تجاهل")
                return
            
            # المينت نشط - فحص السعر
            onchain_price = await asyncio.to_thread(
                get_onchain_public_price_wei,
                w3,
                contract_address
            )
            price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
            price_usd = calculate_price_usd(price_wei, eth_price_usd)
        
        # المينت مجاني ونشط - فحص تويتر والشراء
        if price_wei is None or is_free_or_negligible(price_wei, eth_price_usd):
            log.info(f"🆓 '{slug}' مجاني (${price_usd:.4f}) - فحص تويتر والشراء")
            
            has_twitter, twitter_username = await check_twitter(slug)
            
            if not has_twitter:
                log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X مربوط.")
                rejection_manager.mark_rejected(slug)
                return
            
            log.info(f"✅ '{slug}': يوجد حساب X (@{twitter_username}) — جاري الشراء.")
            
            results = await try_buy_now_multi_wallet(slug, chain_key, detail)
            
            if results is None:
                # مدفوع - أضف للمراقبة بدون إشعار
                await add_to_watchlist_silent(slug, chain_key, detail)
                return
            
            if any(r.get("reason") == "mint_not_started" for r in results):
                log.info(f"⏰ '{slug}' لم يبدأ بعد - إضافة للمراقبة")
                if slug in watchlist:
                    watchlist[slug].mint_status = MintStatus.NOT_STARTED
                    watchlist[slug].waiting_since = time.time()
                return
            
            if len(successful_mints.get(slug, set())) < len(config.wallets):
                await add_to_watchlist_silent(slug, chain_key, detail)
        
        # المينت مدفوع - أضف للمراقبة بدون إشعار
        else:
            log.info(f"💰 '{slug}' مدفوع (${price_usd:.4f}) - إضافة للمراقبة")
            
            priority = determine_priority(price_usd)
            
            entry = WatchlistEntry(
                chain_key=chain_key,
                detail=detail,
                added_at=time.time(),
                was_paid=True,
                last_price_usd=price_usd,
                priority=priority,
                mint_status=MintStatus.ACTIVE,
                mint_start_time=start_time if 'start_time' in locals() else None,
                mint_end_time=end_time if 'end_time' in locals() else None,
            )
            watchlist[slug] = entry
            # لا إشعار هنا
    
    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}", exc_info=True)
    finally:
        in_flight.discard(slug)

async def add_to_watchlist_silent(slug: str, chain_key: str, detail: dict):
    """إضافة للمراقبة بدون إشعار"""
    if len(watchlist) >= config.max_watchlist_size:
        log.warning(f"قائمة المراقبة ممتلئة ({config.max_watchlist_size}). تجاهل '{slug}'")
        return
    
    if slug in watchlist:
        watchlist[slug].detail = detail
        return
    
    entry = WatchlistEntry(
        chain_key=chain_key,
        detail=detail,
        added_at=time.time(),
    )
    watchlist[slug] = entry

# ---------------------------------------------------------------------------
# حلقة المراقبة الذكية
# ---------------------------------------------------------------------------

async def watch_loop():
    """مراقبة ذكية بأولويات متغيرة"""
    
    last_normal_check = 0
    last_low_check = 0
    last_cache_cleanup = 0
    
    while True:
        await asyncio.sleep(config.watch_poll_interval)
        
        now = time.time()
        
        if not watchlist:
            continue
        
        rejection_manager.cleanup()
        
        if now - last_cache_cleanup > 300:
            opensea_api.cleanup_cache()
            last_cache_cleanup = now
        
        urgent_entries = {
            slug: entry for slug, entry in watchlist.items()
            if entry.priority == WatchPriority.URGENT
        }
        normal_entries = {
            slug: entry for slug, entry in watchlist.items()
            if entry.priority == WatchPriority.NORMAL
        }
        low_entries = {
            slug: entry for slug, entry in watchlist.items()
            if entry.priority == WatchPriority.LOW
        }
        
        for slug, entry in urgent_entries.items():
            await check_watchlist_entry(slug, entry)
        
        if now - last_normal_check >= config.normal_check_interval:
            for slug, entry in normal_entries.items():
                await check_watchlist_entry(slug, entry)
            last_normal_check = now
        
        if now - last_low_check >= config.low_check_interval:
            for slug, entry in low_entries.items():
                await check_watchlist_entry(slug, entry)
            last_low_check = now

async def check_watchlist_entry(slug: str, entry: WatchlistEntry):
    """فحص عنصر واحد في قائمة المراقبة"""
    
    if slug in in_flight:
        return
    
    if len(successful_mints.get(slug, set())) >= len(config.wallets):
        watchlist.pop(slug, None)
        return
    
    if entry.check_count >= entry.max_checks:
        watchlist.pop(slug, None)
        return
    
    if entry.mint_status == MintStatus.NOT_STARTED and entry.waiting_since:
        wait_duration = time.time() - entry.waiting_since
        if wait_duration > config.max_wait_time:
            watchlist.pop(slug, None)
            log.info(f"⏰ '{slug}' تم الانتظار لفترة طويلة ({wait_duration:.0f} ثانية) - إلغاء")
            return
    
    in_flight.add(slug)
    entry.check_count += 1
    entry.last_check = time.time()
    now = time.time()
    
    try:
        found, fresh_detail = await asyncio.to_thread(
            opensea_api.fetch_drop_detail,
            slug,
            True
        )
        
        if not found or not fresh_detail or not fresh_detail.get("is_minting"):
            watchlist.pop(slug, None)
            return
        
        stage = fresh_detail.get("active_stage")
        if not stage:
            watchlist.pop(slug, None)
            return
        
        w3 = w3_instances[entry.chain_key]
        contract_address = fresh_detail.get("contract_address")
        
        if not contract_address:
            return
        
        mint_status, start_time, end_time = get_mint_status(w3, contract_address)
        
        # المينت لم يبدأ بعد
        if mint_status == MintStatus.NOT_STARTED:
            entry.mint_status = MintStatus.NOT_STARTED
            entry.mint_start_time = start_time
            entry.mint_end_time = end_time
            
            if entry.waiting_since is None:
                entry.waiting_since = time.time()
            
            wait_seconds = start_time - int(time.time()) if start_time else 0
            
            # إرسال إشعار البدء فقط إذا لم يتم إرساله من قبل وسيبدأ خلال 5 دقائق
            if not entry.start_notified and wait_seconds <= config.notify_before_start:
                message = build_start_notification(fresh_detail, wait_seconds)
                telegram_manager.broadcast(message, config.wallets)
                entry.start_notified = True
                log.info(f"🔔 إرسال إشعار بدء '{slug}' - متبقي {wait_seconds} ثانية")
            
            log.info(f"⏰ '{slug}' لم يبدأ بعد - متبقي {wait_seconds} ثانية")
            return
        
        # المينت انتهى
        if mint_status == MintStatus.ENDED:
            watchlist.pop(slug, None)
            return
        
        # المينت نشط
        entry.mint_status = MintStatus.ACTIVE
        entry.mint_start_time = start_time
        entry.mint_end_time = end_time
        
        if stage_has_ended(stage) and not fresh_detail.get("next_stage"):
            watchlist.pop(slug, None)
            return
        
        eth_price_usd = get_eth_price_usd()
        
        onchain_price = await asyncio.to_thread(
            get_onchain_public_price_wei,
            w3,
            contract_address
        )
        
        if onchain_price is None:
            return
        
        price_wei = onchain_price
        price_usd = calculate_price_usd(price_wei, eth_price_usd)
        
        entry.priority = determine_priority(price_usd)
        
        # السعر مجاني - شراء
        if is_free_or_negligible(price_wei, eth_price_usd):
            has_twitter, twitter_username = await check_twitter(slug, entry)
            
            if not has_twitter:
                watchlist.pop(slug, None)
                rejection_manager.mark_rejected(slug)
                return
            
            log.info(f"✅ '{slug}': يوجد حساب X (@{twitter_username}) — جاري الشراء.")
            
            results = await try_buy_now_multi_wallet(
                slug,
                entry.chain_key,
                fresh_detail
            )
            
            if results is None:
                entry.detail = fresh_detail
                entry.was_paid = False
                entry.last_price_usd = price_usd
            elif any(r.get("reason") == "mint_not_started" for r in results):
                entry.mint_status = MintStatus.NOT_STARTED
                entry.waiting_since = time.time()
            elif len(successful_mints.get(slug, set())) >= len(config.wallets):
                watchlist.pop(slug, None)
                log.info(f"🎉 اكتمل الشراء لجميع المحافظ في '{slug}'")
            else:
                entry.detail = fresh_detail
                entry.was_paid = False
                entry.last_price_usd = price_usd
        
        # المينت لا يزال مدفوعاً
        else:
            entry.detail = fresh_detail
            entry.was_paid = True
            entry.last_price_usd = price_usd
            
            if entry.check_count % 10 == 0:
                log.info(f"💰 '{slug}' لا يزال مدفوعاً (${price_usd:.4f}) - أولوية {entry.priority.value}")
    
    except Exception as e:
        log.error(f"خطأ بدورة مراقبة '{slug}': {e}", exc_info=True)
    finally:
        in_flight.discard(slug)

# ---------------------------------------------------------------------------
# الاستماع لـ OpenSea Stream
# ---------------------------------------------------------------------------

class OpenSeaStream:
    def __init__(self, url: str):
        self.url = url
        self.msg_ref = 0
        self.backoff = 1
        self.max_backoff = 60
    
    async def listen(self):
        while True:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=None,
                    open_timeout=15,
                ) as ws:
                    log.info(f"متصل بـ OpenSea Stream — يراقب {len(config.wallets)} محافظ.")
                    
                    self.backoff = 1
                    
                    await self._join_channel(ws)
                    
                    last_heartbeat = time.time()
                    
                    while True:
                        if time.time() - last_heartbeat > config.heartbeat_interval:
                            await self._send_heartbeat(ws)
                            last_heartbeat = time.time()
                        
                        try:
                            raw = await asyncio.wait_for(
                                ws.recv(),
                                timeout=config.recv_timeout
                            )
                        except asyncio.TimeoutError:
                            continue
                        
                        await self._process_message(raw)
            
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال خلال {self.backoff} ثانية...")
                await asyncio.sleep(self.backoff)
                self.backoff = min(self.backoff * 2, self.max_backoff)
            
            except Exception as e:
                log.error(f"خطأ غير متوقع: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    async def _join_channel(self, ws):
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
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return
        
        if not isinstance(parsed, list) or len(parsed) != 5:
            return
        
        _, _, _, event_name, payload_wrapper = parsed
        
        if event_name != "item_transferred":
            return
        
        payload = (payload_wrapper or {}).get("payload") or {}
        item = payload.get("item", {}) or {}
        stream_chain_name = (item.get("chain", {}) or {}).get("name", "")
        
        chain_key = stream_name_to_chain.get(stream_chain_name)
        if chain_key is None:
            return
        
        from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
        if from_address != ZERO_ADDRESS:
            return
        
        slug = (payload.get("collection", {}) or {}).get("slug", "")
        if not slug:
            return
        
        if slug in watchlist:
            rejection_manager.unmark_rejected(slug)
        
        if slug in known_slugs:
            known_slugs.discard(slug)
        
        asyncio.create_task(evaluate_new_mint(slug, chain_key))

# ---------------------------------------------------------------------------
# الدوال المساعدة
# ---------------------------------------------------------------------------

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = calculate_price_usd(price_wei, eth_price_usd)
    return price_usd < config.free_price_threshold

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

# ---------------------------------------------------------------------------
# التشغيل الرئيسي
# ---------------------------------------------------------------------------

async def run():
    log.info(f"بدء تشغيل النظام مع {len(config.wallets)} محافظ")
    
    await telegram_manager.start()
    
    if not config.bot_enabled:
        log.warning("🔴 BOT_ENABLED=false")
        telegram_manager.broadcast(
            "🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false).",
            config.wallets
        )
        await telegram_manager.send_queue.join()
        return
    
    # إشعار التشغيل فقط
    telegram_manager.broadcast(
        "✅ تم تشغيل النظام بنجاح!",
        config.wallets
    )
    
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
