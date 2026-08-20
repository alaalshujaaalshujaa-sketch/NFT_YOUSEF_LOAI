"""
النظام الكامل المحسن — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها.
المعيار الوحيد: وجود حساب X (Twitter)
بدون رسائل اكتشاف - فقط رسائل الشراء
مع دعم RPC بدائل ومعالجة متقدمة للأخطاء
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
from enum import Enum

import aiohttp
import requests
import websockets
from dotenv import load_dotenv
from web3 import Web3
from web3.exceptions import TransactionNotFound, ContractLogicError, TimeExhausted

load_dotenv()

# ==================== إعدادات السجلات ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

# ==================== الثوابت ====================
SEADROP_ADDRESS = Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5")
ZERO_ADDRESS = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

SEADROP_ABI = [
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
            {"name": "feeRecipient", "type": "address"},
            {"name": "minterIfNotPayer", "type": "address"},
            {"name": "quantity", "type": "uint256"},
        ],
        "name": "mintPublic",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getAllowedFeeRecipients",
        "outputs": [{"name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getPublicDrop",
        "outputs": [{
            "components": [
                {"name": "mintPrice", "type": "uint80"},
                {"name": "startTime", "type": "uint48"},
                {"name": "endTime", "type": "uint48"},
                {"name": "maxTotalMintableByWallet", "type": "uint16"},
                {"name": "feeBps", "type": "uint16"},
                {"name": "restrictFeeRecipients", "type": "bool"},
            ],
            "name": "",
            "type": "tuple",
        }],
        "stateMutability": "view",
        "type": "function",
    },
]

# ==================== استراتيجيات الشراء ====================
class PurchaseStrategy(Enum):
    AGGRESSIVE = "aggressive"
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"

STRATEGY_CONFIGS = {
    PurchaseStrategy.AGGRESSIVE: {
        "max_gas_multiplier": 2.0,
        "retry_delay": 1,
        "max_retries": 5,
        "priority_fee_multiplier": 1.5,
    },
    PurchaseStrategy.CONSERVATIVE: {
        "max_gas_multiplier": 1.2,
        "retry_delay": 5,
        "max_retries": 2,
        "priority_fee_multiplier": 1.0,
    },
    PurchaseStrategy.BALANCED: {
        "max_gas_multiplier": 1.5,
        "retry_delay": 3,
        "max_retries": 3,
        "priority_fee_multiplier": 1.2,
    },
}

# ==================== هيكلة البيانات ====================
@dataclass
class WalletData:
    wallet: str
    private_key: str
    bot_token: str
    chat_id: str
    current_detail: dict = None
    chain_key: str = ""
    strategy: PurchaseStrategy = PurchaseStrategy.BALANCED
    stats: Dict[str, Any] = field(default_factory=lambda: {
        "total_attempts": 0,
        "successful": 0,
        "failed": 0,
        "total_gas_spent": 0.0,
        "last_purchase_time": None,
    })
    pending_tx_count: int = 0

@dataclass
class PurchaseResult:
    success: bool
    wallet: str
    reason: str = ""
    tx_hash: str = ""
    quantity: int = 0
    gas_fee_usd: float = 0.0
    total_value_wei: int = 0
    error: str = ""
    timestamp: float = field(default_factory=time.time)

# ==================== الإعدادات الأساسية ====================
OPENSEA_API_KEY = os.environ.get("OPENSEA_API_KEY", "")
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]
TELEGRAM_BOT_TOKENS = [t.strip() for t in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if t.strip()]
TELEGRAM_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

if not OPENSEA_API_KEY:
    log.error("❌ OPENSEA_API_KEY غير موجود في .env")
    exit(1)

if not (len(PRIVATE_KEYS) == len(WALLETS) == len(TELEGRAM_BOT_TOKENS) == len(TELEGRAM_CHAT_IDS)):
    raise ValueError("أعداد المفاتيح، المحافظ، توكنات البوتات، و Chat IDs غير متطابقة في ملف .env!")

# إنشاء هيكلية المحافظ
WALLETS_DATA: List[WalletData] = []
for i in range(len(WALLETS)):
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

ALCHEMY_API_KEY_ROBINHOOD = os.environ.get("ALCHEMY_API_KEY", "")
ALCHEMY_API_KEY_ETHEREUM = os.environ.get("ALCHEMY_API_KEY_ETHEREUM", "")

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

LOCAL_TZ = timezone(timedelta(hours=3))

# ==================== إعدادات الأداء ====================
HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 15
REJECTION_COOLDOWN_SECONDS = 120
SAVE_INTERVAL_SECONDS = 60
MAX_CONCURRENT_EVALUATIONS = 10

# إعدادات المستوى 2 و 3
POLL_NEW_DROPS_INTERVAL = int(os.environ.get("POLL_NEW_DROPS_INTERVAL", "60"))
MONITOR_RECENT_CONTRACTS = int(os.environ.get("MONITOR_RECENT_CONTRACTS", "30"))
CONTRACT_MONITOR_INTERVAL = int(os.environ.get("CONTRACT_MONITOR_INTERVAL", "15"))

# ==================== إعدادات RPC مع بدائل ====================
def get_fallback_rpcs(chain: str) -> List[str]:
    """الحصول على قائمة RPCs بديلة"""
    fallbacks = {
        "ethereum": [
            "https://eth.llamarpc.com",
            "https://rpc.ankr.com/eth",
            "https://ethereum.publicnode.com",
            "https://1rpc.io/eth",
            "https://cloudflare-eth.com",
        ],
        "robinhood": [
            "https://robinhood-mainnet.g.alchemy.com/v2/demo",
            "https://rpc.ankr.com/eth",
        ]
    }
    return fallbacks.get(chain, [])

CHAIN_CONFIGS = {
    "robinhood": {
        "stream_chain_name": "robinhood",
        "rpc_url": f"https://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ROBINHOOD}" if ALCHEMY_API_KEY_ROBINHOOD else "https://rpc.ankr.com/eth",
        "fallback_rpcs": get_fallback_rpcs("robinhood"),
        "max_gas_fee_usd": 0.05,
        "chain_id": 6900,
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ETHEREUM}" if ALCHEMY_API_KEY_ETHEREUM else "https://eth.llamarpc.com",
        "fallback_rpcs": get_fallback_rpcs("ethereum"),
        "max_gas_fee_usd": 0.50,
        "chain_id": 1,
    },
}

# ==================== دوال Web3 ====================
def get_web3(rpc_url: str) -> Web3:
    """إنشاء اتصال Web3 مع محاولات متعددة"""
    try:
        w3 = Web3(Web3.HTTPProvider(
            rpc_url,
            request_kwargs={
                'timeout': 30,
                'headers': {'Content-Type': 'application/json'}
            }
        ))
        
        if w3.is_connected():
            try:
                block = w3.eth.block_number
                log.info(f"✅ RPC متصل - البلوك: {block}")
                return w3
            except Exception as e:
                log.warning(f"⚠️ RPC متصل لكن لا يستجيب: {e}")
                raise ConnectionError(f"RPC لا يستجيب: {e}")
        else:
            raise ConnectionError("RPC غير متصل")
            
    except Exception as e:
        log.warning(f"⚠️ فشل الاتصال بـ {rpc_url[:50]}...: {e}")
        raise

def create_web3_with_fallback(chain_key: str) -> Web3:
    """إنشاء Web3 مع RPC بدائل"""
    cfg = CHAIN_CONFIGS[chain_key]
    
    # محاولة RPC الرئيسي
    try:
        return get_web3(cfg["rpc_url"])
    except Exception as e:
        log.warning(f"⚠️ فشل RPC الرئيسي لـ {chain_key}: {e}")
    
    # محاولة RPCs البديلة
    for fallback_url in cfg.get("fallback_rpcs", []):
        try:
            log.info(f"🔄 محاولة RPC بديل لـ {chain_key}: {fallback_url[:50]}...")
            return get_web3(fallback_url)
        except Exception as e:
            log.warning(f"⚠️ فشل RPC بديل: {e}")
            continue
    
    raise ConnectionError(f"❌ تعذر الاتصال بجميع RPCs لـ {chain_key}")

# إنشاء اتصالات Web3
W3_INSTANCES = {}
for key in CHAIN_CONFIGS.keys():
    try:
        W3_INSTANCES[key] = create_web3_with_fallback(key)
        log.info(f"✅ تم الاتصال بـ {key}")
    except Exception as e:
        log.error(f"❌ فشل الاتصال بـ {key}: {e}")

STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# ==================== التخزين الدائم ====================
SUCCESS_FILE = Path("successful_mints.json")
WATCHLIST_FILE = Path("watchlist.json")
DETECTED_MINTS_FILE = Path("detected_mints.json")
STATS_FILE = Path("wallet_stats.json")
RECENT_CONTRACTS_FILE = Path("recent_contracts.json")

successful_mints: Dict[str, Set[str]] = {}
watchlist: Dict[str, Dict] = {}
detected_mints: Dict[str, Dict] = {}
in_flight: Set[str] = set()
rejected_cooldown: Dict[str, float] = {}
_eth_price_cache = {"value": None, "ts": 0}
evaluation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EVALUATIONS)
recent_contracts: Dict[str, Dict[str, Any]] = {}

# ==================== نظام التخزين ====================
class PersistentStorage:
    def __init__(self):
        self.files = {
            "successful": SUCCESS_FILE,
            "watchlist": WATCHLIST_FILE,
            "detected": DETECTED_MINTS_FILE,
            "stats": STATS_FILE,
            "contracts": RECENT_CONTRACTS_FILE,
        }
        self.data = {
            "successful": {},
            "watchlist": {},
            "detected": {},
            "stats": {},
            "contracts": {},
        }
        self._load_all()
    
    def _load_all(self):
        if SUCCESS_FILE.exists():
            try:
                with open(SUCCESS_FILE, 'r') as f:
                    data = json.load(f)
                    self.data["successful"] = {k: set(v) for k, v in data.items()}
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل المينتات الناجحة: {e}")
        
        if WATCHLIST_FILE.exists():
            try:
                with open(WATCHLIST_FILE, 'r') as f:
                    self.data["watchlist"] = json.load(f)
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل قائمة المراقبة: {e}")
        
        if DETECTED_MINTS_FILE.exists():
            try:
                with open(DETECTED_MINTS_FILE, 'r') as f:
                    self.data["detected"] = json.load(f)
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل المينتات المكتشفة: {e}")
        
        if STATS_FILE.exists():
            try:
                with open(STATS_FILE, 'r') as f:
                    self.data["stats"] = json.load(f)
                    for wd in WALLETS_DATA:
                        if wd.wallet in self.data["stats"]:
                            wd.stats.update(self.data["stats"][wd.wallet])
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل الإحصائيات: {e}")
        
        if RECENT_CONTRACTS_FILE.exists():
            try:
                with open(RECENT_CONTRACTS_FILE, 'r') as f:
                    self.data["contracts"] = json.load(f)
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل العقود المكتشفة: {e}")
    
    def save_all(self):
        try:
            with open(SUCCESS_FILE, 'w') as f:
                json.dump(
                    {k: list(v) for k, v in self.data["successful"].items()},
                    f, indent=2
                )
            
            with open(WATCHLIST_FILE, 'w') as f:
                json.dump(self.data["watchlist"], f, indent=2)
            
            with open(DETECTED_MINTS_FILE, 'w') as f:
                json.dump(self.data["detected"], f, indent=2)
            
            stats_data = {}
            for wd in WALLETS_DATA:
                stats_data[wd.wallet] = wd.stats
            with open(STATS_FILE, 'w') as f:
                json.dump(stats_data, f, indent=2)
            
            with open(RECENT_CONTRACTS_FILE, 'w') as f:
                json.dump(self.data["contracts"], f, indent=2)
        except Exception as e:
            log.error(f"❌ خطأ في حفظ البيانات: {e}")

storage = PersistentStorage()
successful_mints = storage.data["successful"]
watchlist = storage.data["watchlist"]
detected_mints = storage.data["detected"]
recent_contracts = storage.data["contracts"]

async def periodic_save():
    while True:
        await asyncio.sleep(SAVE_INTERVAL_SECONDS)
        storage.save_all()

# ==================== نظام الكشف ====================
class MintDetector:
    def __init__(self):
        self.detection_count = 0
        self.detection_sources = {
            "websocket": 0,
            "polling": 0,
            "contract_monitor": 0,
        }
    
    def is_valid_mint_event(self, payload: dict) -> Tuple[bool, Optional[str], Optional[str]]:
        try:
            from_account = payload.get("from_account", {})
            from_address = from_account.get("address", "").lower()
            if from_address != ZERO_ADDRESS:
                return False, None, None
            
            collection = payload.get("collection", {})
            slug = collection.get("slug", "")
            if not slug:
                return False, None, None
            
            item = payload.get("item", {})
            chain = item.get("chain", {})
            stream_chain_name = chain.get("name", "")
            chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
            if chain_key is None:
                return False, None, None
            
            return True, chain_key, slug
            
        except Exception:
            return False, None, None
    
    def record_detection(self, slug: str, source: str = "websocket"):
        self.detection_count += 1
        self.detection_sources[source] = self.detection_sources.get(source, 0) + 1
        log.info(f"🔔 مينت: {slug} (المصدر: {source})")

detector = MintDetector()

# ==================== إدارة الأقفال ====================
class WalletLockManager:
    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._lock_creation: asyncio.Lock = asyncio.Lock()
    
    async def get_lock(self, wallet_address: str) -> asyncio.Lock:
        addr = wallet_address.lower()
        async with self._lock_creation:
            if addr not in self._locks:
                self._locks[addr] = asyncio.Lock()
            return self._locks[addr]

lock_manager = WalletLockManager()

# ==================== إدارة المعاملات المعلقة ====================
class PendingTxManager:
    def __init__(self):
        self._pending: Dict[str, List[str]] = {}
    
    async def add_tx(self, wallet: str, tx_hash: str):
        addr = wallet.lower()
        if addr not in self._pending:
            self._pending[addr] = []
        self._pending[addr].append(tx_hash)
    
    async def remove_tx(self, wallet: str, tx_hash: str):
        addr = wallet.lower()
        if addr in self._pending and tx_hash in self._pending[addr]:
            self._pending[addr].remove(tx_hash)
    
    async def get_pending_count(self, wallet: str) -> int:
        addr = wallet.lower()
        return len(self._pending.get(addr, []))

pending_manager = PendingTxManager()

# ==================== دوال Web3 المساعدة ====================
async def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    """جلب رصيد المحفظة بالدولار مع إعادة محاولة"""
    try:
        if not wallet_address or wallet_address == "0x" or len(wallet_address) < 42:
            return 0.0
        
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        
        for attempt in range(3):
            try:
                balance_wei = await asyncio.wait_for(
                    asyncio.to_thread(w3.eth.get_balance, checksum_wallet),
                    timeout=10
                )
                balance_usd = (balance_wei / 1e18) * eth_price_usd
                return balance_usd
            except asyncio.TimeoutError:
                await asyncio.sleep(1)
            except Exception:
                await asyncio.sleep(1)
        
        return 0.0
        
    except Exception as e:
        log.error(f"❌ [الرصيد] تعذر القراءة: {e}")
        return 0.0

async def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> Optional[int]:
    """جلب السعر من العقد مع معالجة محسنة"""
    try:
        if not nft_contract or nft_contract == "0x" or len(nft_contract) < 42:
            return None
        
        checksum_contract = Web3.to_checksum_address(nft_contract)
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        
        for attempt in range(3):
            try:
                public_drop = await asyncio.wait_for(
                    asyncio.to_thread(
                        seadrop.functions.getPublicDrop(checksum_contract).call
                    ),
                    timeout=10
                )
                if public_drop and len(public_drop) > 0:
                    return int(public_drop[0])
            except asyncio.TimeoutError:
                await asyncio.sleep(1)
            except Exception:
                await asyncio.sleep(1)
        
        return None
        
    except Exception as e:
        log.warning(f"⚠️ خطأ قراءة السعر: {e}")
        return None

async def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000) -> float:
    """تقدير رسوم الغاز"""
    try:
        gas_price = await asyncio.wait_for(
            asyncio.to_thread(w3.eth.gas_price),
            timeout=5
        )
        fee_eth = (gas_price * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception:
        return float("inf")

async def get_fee_recipient(w3: Web3, nft_contract: str) -> Optional[str]:
    """جلب مستلم الرسوم"""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = await asyncio.wait_for(
            asyncio.to_thread(
                seadrop.functions.getAllowedFeeRecipients(
                    Web3.to_checksum_address(nft_contract)
                ).call
            ),
            timeout=10
        )
        if recipients:
            return Web3.to_checksum_address(recipients[0])
        return None
    except Exception:
        return None

def decide_quantity(max_per_wallet: Optional[int], remaining_supply: int, strategy: PurchaseStrategy) -> int:
    """تحديد الكمية حسب الاستراتيجية"""
    if max_per_wallet is None:
        base_qty = 5
    elif max_per_wallet <= 20:
        base_qty = max_per_wallet
    else:
        base_qty = 15
    
    if strategy == PurchaseStrategy.AGGRESSIVE:
        qty = base_qty
    elif strategy == PurchaseStrategy.CONSERVATIVE:
        qty = max(1, base_qty // 2)
    else:
        qty = max(1, int(base_qty * 0.75))
    
    return max(1, min(qty, remaining_supply))

# ==================== دوال الشراء ====================
async def send_transaction_with_retry(
    w3: Web3,
    wallet_data: WalletData,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: Optional[int],
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> PurchaseResult:
    """إرسال معاملة شراء مع إعادة محاولة"""
    strategy_config = STRATEGY_CONFIGS[wallet_data.strategy]
    max_retries = strategy_config["max_retries"]
    
    for attempt in range(max_retries):
        try:
            wallet_data.stats["total_attempts"] += 1
            
            result = await _attempt_purchase(
                w3=w3,
                wallet_data=wallet_data,
                nft_contract=nft_contract,
                price_wei_per_token=price_wei_per_token,
                max_per_wallet=max_per_wallet,
                remaining_supply=remaining_supply,
                eth_price_usd=eth_price_usd,
                max_gas_fee_usd=max_gas_fee_usd,
                strategy_config=strategy_config,
            )
            
            if result.success:
                wallet_data.stats["successful"] += 1
                wallet_data.stats["last_purchase_time"] = time.time()
                wallet_data.stats["total_gas_spent"] += result.gas_fee_usd
                return result
            else:
                wallet_data.stats["failed"] += 1
                
                if result.reason in ["timeout", "connection_error", "nonce_error"]:
                    if attempt < max_retries - 1:
                        delay = min(1 * (2 ** attempt), 10)
                        await asyncio.sleep(delay)
                        continue
                
                return result
                
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
                continue
            
            wallet_data.stats["failed"] += 1
            return PurchaseResult(
                success=False,
                wallet=wallet_data.wallet,
                reason="unexpected_error",
                error=str(e)
            )
    
    return PurchaseResult(
        success=False,
        wallet=wallet_data.wallet,
        reason="retry_exhausted"
    )

async def _attempt_purchase(
    w3: Web3,
    wallet_data: WalletData,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: Optional[int],
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
    strategy_config: Dict[str, Any],
) -> PurchaseResult:
    """محاولة شراء واحدة"""
    checksum_wallet = Web3.to_checksum_address(wallet_data.wallet)
    checksum_contract = Web3.to_checksum_address(nft_contract)
    
    # التحقق من المعاملات المعلقة
    pending_count = await pending_manager.get_pending_count(checksum_wallet)
    if pending_count >= 3:
        return PurchaseResult(
            success=False,
            wallet=checksum_wallet,
            reason="too_many_pending_tx"
        )
    
    # التحقق من الرصيد
    balance_usd = await get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < 0.10:
        return PurchaseResult(
            success=False,
            wallet=checksum_wallet,
            reason="balance_too_low",
            error=f"Balance: ${balance_usd:.2f}"
        )
    
    # تقدير رسوم الغاز
    gas_fee_usd = await estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        return PurchaseResult(
            success=False,
            wallet=checksum_wallet,
            reason="gas_too_high",
            gas_fee_usd=gas_fee_usd,
            error=f"Gas: ${gas_fee_usd:.4f}"
        )
    
    # جلب مستلم الرسوم
    fee_recipient = await get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        return PurchaseResult(
            success=False,
            wallet=checksum_wallet,
            reason="no_fee_recipient"
        )
    
    # تحديد الكمية
    quantity = decide_quantity(max_per_wallet, remaining_supply, wallet_data.strategy)
    total_value = price_wei_per_token * quantity
    
    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = await asyncio.to_thread(w3.eth.get_transaction_count, checksum_wallet, "pending")
        
        # بناء المعاملة
        tx = contract.functions.mintPublic(
            checksum_contract,
            Web3.to_checksum_address(fee_recipient),
            ZERO_ADDRESS,
            quantity,
        ).build_transaction({
            "from": checksum_wallet,
            "value": total_value,
            "nonce": nonce,
            "chainId": w3.eth.chain_id,
            "gasPrice": await asyncio.to_thread(w3.eth.gas_price),
        })
        
        # تقدير الغاز
        estimated_gas = await asyncio.to_thread(w3.eth.estimate_gas, tx)
        tx["gas"] = int(estimated_gas * 1.2)
        
        # التحقق من التكلفة
        gas_cost_wei = tx["gas"] * tx["gasPrice"]
        actual_gas_fee_usd = (gas_cost_wei / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            return PurchaseResult(
                success=False,
                wallet=checksum_wallet,
                reason="gas_too_high",
                gas_fee_usd=actual_gas_fee_usd
            )
        
        # التحقق من الرصيد الكافي
        total_cost_wei = total_value + gas_cost_wei
        wallet_balance_wei = await asyncio.to_thread(w3.eth.get_balance, checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            return PurchaseResult(
                success=False,
                wallet=checksum_wallet,
                reason="insufficient_funds"
            )
        
        # توقيع وإرسال المعاملة
        signed = w3.eth.account.sign_transaction(tx, private_key=wallet_data.private_key)
        tx_hash = await asyncio.to_thread(w3.eth.send_raw_transaction, signed.raw_transaction)
        
        await pending_manager.add_tx(checksum_wallet, tx_hash.hex())
        
        log.info(f"✅ [شراء] TX: {tx_hash.hex()[:10]}... Qty: {quantity}")
        
        return PurchaseResult(
            success=True,
            wallet=checksum_wallet,
            tx_hash=tx_hash.hex(),
            quantity=quantity,
            gas_fee_usd=actual_gas_fee_usd,
            total_value_wei=total_value,
        )
        
    except Exception as e:
        error_msg = str(e).lower()
        if "insufficient funds" in error_msg:
            reason = "insufficient_funds"
        elif "nonce" in error_msg:
            reason = "nonce_error"
        else:
            reason = "tx_error"
        
        return PurchaseResult(
            success=False,
            wallet=checksum_wallet,
            reason=reason,
            error=str(e)
        )

async def purchase_parallel(
    w3: Web3,
    wallets_data: List[WalletData],
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: Optional[int],
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> List[PurchaseResult]:
    """شراء متوازي لجميع المحافظ"""
    tasks = []
    for wallet_data in wallets_data:
        lock = await lock_manager.get_lock(wallet_data.wallet)
        
        async def purchase_with_lock(wd=wallet_data):
            async with lock:
                return await send_transaction_with_retry(
                    w3=w3,
                    wallet_data=wd,
                    nft_contract=nft_contract,
                    price_wei_per_token=price_wei_per_token,
                    max_per_wallet=max_per_wallet,
                    remaining_supply=remaining_supply,
                    eth_price_usd=eth_price_usd,
                    max_gas_fee_usd=max_gas_fee_usd,
                )
        
        tasks.append(purchase_with_lock())
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            processed_results.append(PurchaseResult(
                success=False,
                wallet=wallets_data[i].wallet,
                reason="exception",
                error=str(result)
            ))
        else:
            processed_results.append(result)
    
    return processed_results

# ==================== دوال مساعدة ====================
def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < 300):
        return _eth_price_cache["value"]
    
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8
        )
        price = resp.json()["ethereum"]["usd"]
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except Exception:
        return _eth_price_cache["value"] or 3000.0

async def fetch_drop_detail(slug: str) -> Tuple[Optional[bool], Optional[Dict]]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{DROPS_API_BASE}/{slug}",
                headers={"x-api-key": OPENSEA_API_KEY},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    return True, await resp.json()
                return False, None
    except Exception:
        return None, None

def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def started_today_local(stage: dict) -> bool:
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

def format_wallet_stats(wallet_data: WalletData) -> str:
    stats = wallet_data.stats
    total = stats["total_attempts"]
    success_rate = (stats["successful"] / total * 100) if total > 0 else 0
    
    return (
        f"📊 <b>إحصائيات المحفظة</b>\n"
        f"المحفظة: <code>{wallet_data.wallet[:8]}...</code>\n"
        f"المحاولات: {stats['total_attempts']}\n"
        f"الناجحة: {stats['successful']}\n"
        f"نسبة النجاح: {success_rate:.1f}%\n"
        f"إجمالي الغاز: ${stats['total_gas_spent']:.4f}"
    )

# ==================== تيليجرام ====================
class TelegramManager:
    def __init__(self):
        self.send_queue: asyncio.Queue = asyncio.Queue()
    
    def enqueue(self, bot_token: str, chat_id: str, text: str):
        self.send_queue.put_nowait({
            "bot_token": bot_token,
            "chat_id": chat_id,
            "text": text
        })
    
    def broadcast(self, text: str):
        for w in WALLETS_DATA:
            self.enqueue(w.bot_token, w.chat_id, text)
    
    def send_to_wallet(self, wallet_index: int, text: str):
        if 0 <= wallet_index < len(WALLETS_DATA):
            w = WALLETS_DATA[wallet_index]
            self.enqueue(w.bot_token, w.chat_id, text)
    
    async def sender_loop(self):
        while True:
            msg = await self.send_queue.get()
            try:
                telegram_api = f"https://api.telegram.org/bot{msg['bot_token']}"
                await asyncio.to_thread(
                    requests.post,
                    f"{telegram_api}/sendMessage",
                    data={
                        "chat_id": msg["chat_id"],
                        "text": msg["text"],
                        "parse_mode": "HTML"
                    },
                    timeout=10,
                )
            except Exception as e:
                log.error(f"❌ خطأ إرسال تليجرام: {e}")
            
            self.send_queue.task_done()
            await asyncio.sleep(0.1)
    
    def build_success_message(self, detail: dict, result: dict, chain_key: str) -> str:
        name = detail.get("collection_name") or detail.get("collection_slug")
        url = detail.get("opensea_url", "")
        chain_label = "Robinhood" if chain_key == "robinhood" else "Ethereum"
        w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
        
        twitter = detail.get('twitter_username', '')
        twitter_text = f"\nحساب X: @{twitter}" if twitter else ""
        
        return (
            f"✅ <b>تم الشراء!</b> ({chain_label})\n\n"
            f"المحفظة: <code>{w_short}</code>\n"
            f"المجموعة: <b>{name}</b>{twitter_text}\n"
            f"الكمية: {result['quantity']}\n"
            f"رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
            f"المعاملة: <code>{result['tx_hash'][:10]}...</code>\n"
            f"🔗 {url}"
        )

telegram = TelegramManager()

# ==================== منطق الشراء الرئيسي ====================
async def try_buy_now_multi_wallet(
    slug: str,
    chain_key: str,
    detail: dict
) -> Optional[List[Dict[str, Any]]]:
    """محاولة الشراء لجميع المحافظ"""
    stage = detail.get("active_stage")
    if not stage:
        return None
    
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    if remaining <= 0:
        return None
    
    contract_address = detail.get("contract_address")
    if not contract_address:
        return None
    
    w3 = W3_INSTANCES[chain_key]
    eth_price_usd = get_eth_price_usd()
    
    # قراءة السعر
    price_wei = await get_onchain_public_price_wei(w3, contract_address)
    if price_wei is None:
        try:
            price_wei = int(stage.get("price", "0"))
        except:
            price_wei = 0
    
    if not is_free_or_negligible(price_wei, eth_price_usd):
        return None
    
    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]
    
    already_bought = successful_mints.get(slug, set())
    pending_wallets = [
        wd for wd in WALLETS_DATA
        if wd.wallet not in already_bought
    ]
    
    if not pending_wallets:
        return None
    
    for wd in pending_wallets:
        wd.current_detail = detail
        wd.chain_key = chain_key
    
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
    
    for result in results:
        if result.success:
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(result.wallet)
            
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
    
    storage.save_all()
    return [vars(r) for r in results]

# ==================== التقييم والشراء ====================
async def evaluate_and_buy(slug: str, chain_key: str, source: str = "websocket"):
    """تقييم سريع وشراء - بدون رسائل اكتشاف"""
    async with evaluation_semaphore:
        if (len(successful_mints.get(slug, set())) >= len(WALLETS_DATA) or
            slug in watchlist or slug in in_flight):
            return
        
        in_flight.add(slug)
        try:
            found, detail = await fetch_drop_detail(slug)
            if not found or not detail or not detail.get("is_minting"):
                return
            
            stage = detail.get("active_stage")
            if not stage or not started_today_local(stage):
                return
            
            if slug not in detected_mints:
                detected_mints[slug] = {
                    'detected_at': time.time(),
                    'chain': chain_key,
                    'slug': slug,
                    'name': detail.get('collection_name') or slug,
                    'detected_by': source,
                }
                storage.save_all()
                detector.record_detection(slug, source)
            
            # تتبع العقد
            contract_address = detail.get("contract_address")
            if contract_address:
                recent_contracts[contract_address.lower()] = {
                    "slug": slug,
                    "chain_key": chain_key,
                    "timestamp": time.time(),
                    "detected_by": source,
                }
                if len(recent_contracts) > MONITOR_RECENT_CONTRACTS * 2:
                    sorted_contracts = sorted(
                        recent_contracts.items(),
                        key=lambda x: x[1]["timestamp"]
                    )
                    for addr, _ in sorted_contracts[:MONITOR_RECENT_CONTRACTS // 2]:
                        recent_contracts.pop(addr, None)
                storage.save_all()
            
            # التحقق من السعر
            w3 = W3_INSTANCES[chain_key]
            eth_price_usd = get_eth_price_usd()
            
            if contract_address:
                price_wei = await get_onchain_public_price_wei(w3, contract_address)
                if price_wei is None:
                    try:
                        price_wei = int(stage.get("price", "0"))
                    except:
                        price_wei = 0
                
                if not is_free_or_negligible(price_wei, eth_price_usd):
                    log.info(f"💰 '{slug}' مدفوع - تخطي")
                    return
            
            # التحقق من X
            twitter_username = await asyncio.to_thread(
                get_twitter_username_from_opensea, slug, OPENSEA_API_KEY
            )
            
            if twitter_username:
                detail['twitter_username'] = twitter_username
                log.info(f"🛒 شراء '{slug}' - X: @{twitter_username}")
                results = await try_buy_now_multi_wallet(slug, chain_key, detail)
                
                if results is None or len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": detail,
                        "twitter_username": twitter_username,
                        "detected_by": source,
                    }
                    storage.save_all()
            else:
                log.info(f"⏭️ '{slug}' لا يوجد X - تخطي")
            
        except Exception as e:
            log.error(f"❌ خطأ: {e}")
        finally:
            in_flight.discard(slug)

# ==================== المستوى 2: الفحص الدوري ====================
async def poll_new_drops():
    processed_slugs_polling = set()
    
    while True:
        try:
            await asyncio.sleep(POLL_NEW_DROPS_INTERVAL)
            
            if not BOT_ENABLED:
                continue
            
            drops = await asyncio.to_thread(fetch_recent_drops_fast)
            
            for drop in drops:
                slug = drop.get("slug")
                if not slug or slug in processed_slugs_polling:
                    continue
                
                processed_slugs_polling.add(slug)
                
                chain_key = drop.get("chain", "robinhood")
                if chain_key not in CHAIN_CONFIGS:
                    chain_key = "robinhood"
                
                if slug not in detected_mints:
                    asyncio.create_task(
                        evaluate_and_buy(slug, chain_key, source="polling")
                    )
        
        except Exception as e:
            log.error(f"خطأ في الفحص الدوري: {e}")
            await asyncio.sleep(POLL_NEW_DROPS_INTERVAL)

def fetch_recent_drops_fast() -> List[Dict]:
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}?limit=50&order_by=created_at&order_direction=desc",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("drops", [])
        return []
    except:
        return []

# ==================== المستوى 3: مراقبة العقود ====================
async def monitor_contracts():
    processed_monitor = set()
    
    while True:
        try:
            await asyncio.sleep(CONTRACT_MONITOR_INTERVAL)
            
            if not BOT_ENABLED:
                continue
            
            now = time.time()
            
            recent = [
                (addr, data)
                for addr, data in recent_contracts.items()
                if now - data.get("timestamp", 0) < 600
            ]
            
            for contract_addr, data in recent:
                slug = data.get("slug")
                if not slug:
                    continue
                
                monitor_key = f"{contract_addr}_{slug}"
                if monitor_key in processed_monitor:
                    continue
                
                processed_monitor.add(monitor_key)
                if len(processed_monitor) > 1000:
                    processed_monitor.clear()
                
                chain_key = data.get("chain_key", "robinhood")
                
                try:
                    w3 = W3_INSTANCES[chain_key]
                    price_wei = await asyncio.wait_for(
                        asyncio.to_thread(get_onchain_public_price_wei, w3, contract_addr),
                        timeout=5,
                    )
                    
                    if price_wei is not None:
                        # التحقق من وقت البدء
                        try:
                            seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
                            public_drop = await asyncio.wait_for(
                                asyncio.to_thread(
                                    seadrop.functions.getPublicDrop(
                                        Web3.to_checksum_address(contract_addr)
                                    ).call
                                ),
                                timeout=5
                            )
                            start_time = int(public_drop[1]) if len(public_drop) > 1 else None
                            current_time = int(time.time())
                            
                            if start_time and abs(current_time - start_time) < 120:
                                eth_price_usd = get_eth_price_usd()
                                
                                if is_free_or_negligible(price_wei, eth_price_usd):
                                    found, detail = await fetch_drop_detail(slug)
                                    if found and detail:
                                        asyncio.create_task(
                                            evaluate_and_buy(slug, chain_key, source="contract_monitor")
                                        )
                        except:
                            pass
                except asyncio.TimeoutError:
                    continue
                except Exception:
                    continue
        
        except Exception as e:
            log.error(f"خطأ في مراقبة العقود: {e}")
            await asyncio.sleep(CONTRACT_MONITOR_INTERVAL)

# ==================== حلقة المراقبة ====================
async def watch_loop():
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
                    storage.save_all()
                    continue
                
                stage = fresh_detail.get("active_stage")
                if not stage or not started_today_local(stage):
                    watchlist.pop(slug, None)
                    storage.save_all()
                    continue
                
                if 'twitter_username' not in entry:
                    twitter_username = await asyncio.to_thread(
                        get_twitter_username_from_opensea, slug, OPENSEA_API_KEY
                    )
                    if twitter_username:
                        entry['twitter_username'] = twitter_username
                        fresh_detail['twitter_username'] = twitter_username
                
                results = await try_buy_now_multi_wallet(slug, chain_key, fresh_detail)
                
                if results is None:
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": fresh_detail,
                        "twitter_username": entry.get('twitter_username'),
                        "detected_by": entry.get('detected_by', 'unknown'),
                    }
                    continue
                
                if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                    watchlist.pop(slug, None)
                else:
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": fresh_detail,
                        "twitter_username": entry.get('twitter_username'),
                        "detected_by": entry.get('detected_by', 'unknown'),
                    }
                
                storage.save_all()
                
            except Exception as e:
                log.error(f"❌ خطأ بدورة مراقبة '{slug}': {e}")
            finally:
                in_flight.discard(slug)

# ==================== المستوى 1: WebSocket ====================
async def listen_opensea():
    msg_ref = 0
    
    while True:
        try:
            async with websockets.connect(
                STREAM_URL,
                ping_interval=None,
                open_timeout=15
            ) as ws:
                log.info(f"✅ متصل بـ OpenSea Stream")
                
                join_ref = str(msg_ref)
                await ws.send(json.dumps([
                    join_ref, join_ref, "collection:*", "phx_join", {}
                ]))
                msg_ref += 1
                last_heartbeat = time.time()
                
                while True:
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
                    
                    is_valid, chain_key, slug = detector.is_valid_mint_event(payload)
                    if not is_valid or not slug:
                        continue
                    
                    if slug in detected_mints:
                        detected_mints[slug]['detected_at'] = time.time()
                        continue
                    
                    asyncio.create_task(evaluate_and_buy(slug, chain_key, source="websocket"))
                    
        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"⚠️ انقطع الاتصال: {e}")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"❌ خطأ: {e}")
            await asyncio.sleep(5)

# ==================== واجهة الأوامر ====================
async def command_handler():
    while True:
        await asyncio.sleep(60)
        
        if int(time.time()) % 3600 < 60:
            for i, wd in enumerate(WALLETS_DATA):
                stats_msg = format_wallet_stats(wd)
                telegram.send_to_wallet(i, stats_msg)

# ==================== دالة twitter_checker ====================
def get_twitter_username_from_opensea(slug: str, api_key: str) -> Optional[str]:
    """جلب حساب X من OpenSea"""
    try:
        resp = requests.get(
            f"https://api.opensea.io/api/v2/collections/{slug}",
            headers={"x-api-key": api_key},
            timeout=8
        )
        if resp.status_code == 200:
            data = resp.json()
            # محاولة جلب من external_url
            external_url = data.get("external_url", "")
            if "twitter.com" in external_url:
                username = external_url.split("twitter.com/")[-1].split("/")[0]
                if username:
                    return username
        return None
    except Exception:
        return None

# ==================== مراقبة صحة RPC ====================
async def rpc_health_monitor():
    """مراقبة دورية لصحة RPC"""
    while True:
        await asyncio.sleep(60)
        
        for chain_key, w3 in W3_INSTANCES.items():
            try:
                block = await asyncio.wait_for(
                    asyncio.to_thread(w3.eth.block_number),
                    timeout=5
                )
                log.debug(f"✅ RPC {chain_key} سليم - بلوك: {block}")
            except Exception:
                log.error(f"❌ RPC {chain_key} غير صحي - محاولة إعادة الاتصال")
                try:
                    W3_INSTANCES[chain_key] = create_web3_with_fallback(chain_key)
                    log.info(f"✅ تم إعادة الاتصال بـ {chain_key}")
                except Exception as e:
                    log.error(f"❌ فشل إعادة الاتصال بـ {chain_key}: {e}")

# ==================== التشغيل الرئيسي ====================
async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        return
    
    telegram.broadcast(
        f"✅ <b>تم تشغيل النظام</b>\n\n"
        f"عدد المحافظ: {len(WALLETS_DATA)}\n"
        f"المعيار: وجود X فقط\n"
        f"مستويات الكشف: WebSocket + فحص دوري + مراقبة عقود\n\n"
        f"🚀 جاهز للعمل!"
    )
    
    await asyncio.gather(
        listen_opensea(),
        poll_new_drops(),
        monitor_contracts(),
        watch_loop(),
        telegram.sender_loop(),
        periodic_save(),
        command_handler(),
        rpc_health_monitor(),
        return_exceptions=True
    )

def main():
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("🛑 تم الإيقاف")
            storage.save_all()
            break
        except Exception as e:
            log.critical(f"❌ توقف: {e}")
            storage.save_all()
            time.sleep(5)
            continue
        else:
            break

if __name__ == "__main__":
    main()
