# buyer.py - نسخة محسنة بالكامل

"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
نظام محاولات محسن مع تتبع ذكي وإعادة محاولة تكيفية.
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from enum import Enum
from web3 import Web3
from web3.exceptions import ContractLogicError, ValidationError

log = logging.getLogger("buyer")

# ===================== الثوابت =====================

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

MIN_BALANCE_RESERVE_USD = 0.10
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 15
GAS_LIMIT_SAFETY_MARGIN = 1.2

# ===================== نظام المحاولات المحسن =====================

class AttemptStatus(Enum):
    """حالات محاولة الشراء"""
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"
    GAVE_UP = "gave_up"

class ErrorType(Enum):
    """أنواع الأخطاء"""
    TEMPORARY = "temporary"      # مؤقت - يستحق إعادة المحاولة
    PERMANENT = "permanent"      # دائم - لا يستحق إعادة المحاولة
    UNKNOWN = "unknown"          # غير معروف - نعطي فرصة

@dataclass
class PurchaseAttempt:
    """سجل محاولة شراء متكامل"""
    wallet: str
    slug: str
    chain_key: str
    status: AttemptStatus = AttemptStatus.PENDING
    attempt_count: int = 0
    max_attempts: int = 3
    last_error: Optional[str] = None
    last_error_time: float = 0
    error_type: ErrorType = ErrorType.UNKNOWN
    retry_after: float = 0
    retry_delay_base: float = 3  # ثواني
    retry_delay_max: float = 300  # 5 دقائق
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    success_tx_hash: Optional[str] = None
    success_quantity: int = 0
    
    def can_retry(self) -> bool:
        """التحقق من إمكانية إعادة المحاولة"""
        if self.attempt_count >= self.max_attempts:
            return False
        if self.error_type == ErrorType.PERMANENT:
            return False
        if self.status == AttemptStatus.SUCCESS:
            return False
        if time.time() < self.retry_after:
            return False
        return True
    
    def get_retry_delay(self) -> float:
        """حساب وقت الانتظار التكيفي (خوارزمية التراجع الأسي)"""
        delay = self.retry_delay_base * (2 ** (self.attempt_count - 1))
        return min(delay, self.retry_delay_max)
    
    def increment_attempt(self, error: str = None, error_type: ErrorType = ErrorType.TEMPORARY):
        """زيادة عدد المحاولات وتسجيل الخطأ"""
        self.attempt_count += 1
        self.updated_at = time.time()
        if error:
            self.last_error = error
            self.last_error_time = time.time()
        self.error_type = error_type
        self.retry_after = time.time() + self.get_retry_delay()
        self.status = AttemptStatus.RETRY
    
    def mark_success(self, tx_hash: str = None, quantity: int = 0):
        """تسجيل نجاح المحاولة"""
        self.status = AttemptStatus.SUCCESS
        self.updated_at = time.time()
        if tx_hash:
            self.success_tx_hash = tx_hash
        if quantity:
            self.success_quantity = quantity
    
    def mark_failed(self, error: str, error_type: ErrorType = ErrorType.TEMPORARY):
        """تسجيل فشل المحاولة"""
        self.status = AttemptStatus.FAILED
        self.error_type = error_type
        self.last_error = error
        self.last_error_time = time.time()
        self.updated_at = time.time()
    
    def mark_gave_up(self, reason: str = None):
        """تسجيل التخلي عن المحاولة"""
        self.status = AttemptStatus.GAVE_UP
        if reason:
            self.last_error = reason
            self.last_error_time = time.time()
        self.updated_at = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """تحويل إلى قاموس للتقرير"""
        return {
            "status": self.status.value,
            "attempts": self.attempt_count,
            "max_attempts": self.max_attempts,
            "last_error": self.last_error,
            "error_type": self.error_type.value,
            "can_retry": self.can_retry(),
            "retry_after": max(0, self.retry_after - time.time()),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "success": self.status == AttemptStatus.SUCCESS
        }

class ErrorClassifier:
    """مصنف الأخطاء لتحديد نوع الخطأ"""
    
    # أخطاء دائمة (لا تستحق إعادة المحاولة)
    PERMANENT_PATTERNS = [
        "insufficient funds",
        "insufficient balance", 
        "balance too low",
        "already minted",
        "already bought",
        "sold out",
        "invalid address",
        "invalid contract",
        "contract not found",
        "nonce too low",
        "already known",
        "signature invalid",
        "unauthorized",
        "permission denied",
        "not found",
        "404",
        "410",
    ]
    
    # أخطاء مؤقتة (تستحق إعادة المحاولة)
    TEMPORARY_PATTERNS = [
        "timeout",
        "connection",
        "network", 
        "rate limit",
        "too many requests",
        "429",
        "gas",
        "pending",
        "replacement transaction underpriced",
        "busy",
        "retry",
        "try again",
        "temporarily",
        "internal error",
        "500",
        "502",
        "503",
        "504",
    ]
    
    @classmethod
    def classify(cls, error: Union[str, Exception]) -> ErrorType:
        """تصنيف نوع الخطأ"""
        error_str = str(error).lower() if error else ""
        
        # فحص الأخطاء الدائمة أولاً
        for pattern in cls.PERMANENT_PATTERNS:
            if pattern in error_str:
                return ErrorType.PERMANENT
        
        # فحص الأخطاء المؤقتة
        for pattern in cls.TEMPORARY_PATTERNS:
            if pattern in error_str:
                return ErrorType.TEMPORARY
        
        # أخطاء محددة من web3
        if isinstance(error, ContractLogicError):
            return ErrorType.PERMANENT
        if isinstance(error, ValidationError):
            return ErrorType.PERMANENT
        
        # افتراضي: مؤقت (لإعطاء فرصة)
        return ErrorType.TEMPORARY

# ===================== إدارة سجل المحاولات =====================

class AttemptManager:
    """مدير سجل المحاولات المركزي"""
    
    def __init__(self):
        self._attempts: Dict[str, PurchaseAttempt] = {}
        self._lock = asyncio.Lock()
    
    def _get_key(self, wallet: str, slug: str) -> str:
        return f"{wallet.lower()}_{slug}"
    
    async def get_or_create(self, wallet: str, slug: str, chain_key: str = None) -> PurchaseAttempt:
        """الحصول على سجل محاولة أو إنشاء جديد"""
        async with self._lock:
            key = self._get_key(wallet, slug)
            if key not in self._attempts:
                self._attempts[key] = PurchaseAttempt(
                    wallet=wallet,
                    slug=slug,
                    chain_key=chain_key or "unknown"
                )
            return self._attempts[key]
    
    async def get(self, wallet: str, slug: str) -> Optional[PurchaseAttempt]:
        """الحصول على سجل محاولة"""
        async with self._lock:
            key = self._get_key(wallet, slug)
            return self._attempts.get(key)
    
    async def update(self, attempt: PurchaseAttempt):
        """تحديث سجل محاولة"""
        async with self._lock:
            key = self._get_key(attempt.wallet, attempt.slug)
            self._attempts[key] = attempt
    
    async def should_retry(self, wallet: str, slug: str) -> bool:
        """التحقق من إمكانية إعادة المحاولة"""
        attempt = await self.get(wallet, slug)
        if not attempt:
            return True
        return attempt.can_retry()
    
    async def is_permanent_failure(self, wallet: str, slug: str) -> bool:
        """التحقق من أن الفشل دائم"""
        attempt = await self.get(wallet, slug)
        if not attempt:
            return False
        return attempt.error_type == ErrorType.PERMANENT or attempt.attempt_count >= attempt.max_attempts
    
    async def clear(self, wallet: str = None, slug: str = None):
        """مسح سجل المحاولات"""
        async with self._lock:
            if wallet and slug:
                key = self._get_key(wallet, slug)
                self._attempts.pop(key, None)
            elif wallet:
                keys_to_remove = [k for k in self._attempts.keys() if k.startswith(f"{wallet.lower()}_")]
                for k in keys_to_remove:
                    self._attempts.pop(k, None)
            elif slug:
                keys_to_remove = [k for k in self._attempts.keys() if k.endswith(f"_{slug}")]
                for k in keys_to_remove:
                    self._attempts.pop(k, None)
    
    async def get_summary(self, wallet: str = None) -> Dict[str, Any]:
        """الحصول على ملخص المحاولات"""
        summary = {
            "total_attempts": 0,
            "success": 0,
            "failed": 0,
            "retry": 0,
            "gave_up": 0,
            "pending": 0,
            "by_wallet": {}
        }
        
        async with self._lock:
            for key, attempt in self._attempts.items():
                if wallet and not key.startswith(f"{wallet.lower()}_"):
                    continue
                
                summary["total_attempts"] += 1
                status = attempt.status.value
                summary[status] = summary.get(status, 0) + 1
                
                wallet_key = attempt.wallet
                if wallet_key not in summary["by_wallet"]:
                    summary["by_wallet"][wallet_key] = {
                        "attempts": 0,
                        "success": 0,
                        "failed": 0,
                        "slug": attempt.slug
                    }
                summary["by_wallet"][wallet_key]["attempts"] += 1
                if attempt.status == AttemptStatus.SUCCESS:
                    summary["by_wallet"][wallet_key]["success"] += 1
                elif attempt.status in [AttemptStatus.FAILED, AttemptStatus.GAVE_UP]:
                    summary["by_wallet"][wallet_key]["failed"] += 1
        
        return summary

# ===================== المتغيرات العالمية =====================

attempt_manager = AttemptManager()

# قفل خاص لكل محفظة لمنع تضارب المعاملات
wallet_locks = {}

def get_wallet_lock(wallet_address: str) -> asyncio.Lock:
    addr = wallet_address.lower()
    if addr not in wallet_locks:
        wallet_locks[addr] = asyncio.Lock()
    return wallet_locks[addr]

# ===================== دوال المساعدة =====================

def get_web3(rpc_url: str) -> Web3:
    return Web3(Web3.HTTPProvider(rpc_url))

def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة للمحفظة {wallet_address[:8]}...: {e}")
        return 0.0

def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000) -> float:
    try:
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"[الغاز] تعذر التقدير: {e}")
        return float("inf")

def get_fee_recipient(w3: Web3, nft_contract: str) -> Optional[str]:
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        if not recipients:
            return None
        return Web3.to_checksum_address(recipients[0])
    except Exception as e:
        log.error(f"[عنوان الرسوم] خطأ استعلام: {e}")
        return None

def decide_quantity(max_per_wallet: Optional[int], remaining_supply: int) -> int:
    if max_per_wallet is None:
        qty = 5
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    return max(1, min(qty, remaining_supply))

def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> Optional[int]:
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return int(public_drop[0])
    except Exception as e:
        log.warning(f"[سعر on-chain] تعذر القراءة: {e}")
        return None

# ===================== دالة الشراء الأساسية (محسنة) =====================

async def attempt_purchase_single_wallet(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: Optional[int],
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
    slug: str = None,
    chain_key: str = None,
) -> Dict[str, Any]:
    """
    محاولة الشراء بمحفظة واحدة - نسخة محسنة مع تتبع المحاولات وإعادة المحاولة الذكية
    """
    # التحقق من صحة العناوين
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {
            "success": False, 
            "wallet": wallet_address, 
            "reason": "invalid_address", 
            "error": str(e)
        }
    
    # التحقق من حالة المحاولات السابقة
    if slug:
        attempt = await attempt_manager.get_or_create(wallet_address, slug, chain_key)
        if not attempt.can_retry():
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "max_retries_exceeded",
                "attempts": attempt.attempt_count,
                "max_attempts": attempt.max_attempts,
                "last_error": attempt.last_error,
                "error_type": attempt.error_type.value
            }
    
    # 1. التحقق من الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        error_msg = f"الرصيد منخفض جداً: ${balance_usd:.4f} (الحد الأدنى: ${MIN_BALANCE_RESERVE_USD})"
        if slug:
            attempt = await attempt_manager.get_or_create(wallet_address, slug, chain_key)
            attempt.mark_failed(error_msg, ErrorType.PERMANENT)
            await attempt_manager.update(attempt)
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "balance_too_low",
            "balance_usd": balance_usd,
            "error": error_msg
        }
    
    # 2. التحقق من رسوم الغاز
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        error_msg = f"رسوم الغاز مرتفعة جداً: ${gas_fee_usd:.4f} (الحد الأقصى: ${max_gas_fee_usd})"
        if slug:
            attempt = await attempt_manager.get_or_create(wallet_address, slug, chain_key)
            attempt.mark_failed(error_msg, ErrorType.TEMPORARY)
            await attempt_manager.update(attempt)
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "gas_too_high",
            "gas_fee_usd": gas_fee_usd,
            "error": error_msg
        }
    
    # 3. الحصول على مستلم الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        error_msg = "لا يوجد مستلم رسوم متاح"
        if slug:
            attempt = await attempt_manager.get_or_create(wallet_address, slug, chain_key)
            attempt.mark_failed(error_msg, ErrorType.PERMANENT)
            await attempt_manager.update(attempt)
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "no_fee_recipient",
            "error": error_msg
        }
    
    # 4. تحديد الكمية
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity
    
    # 5. بناء وإرسال المعاملة
    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
        
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
        })
        
        # 6. تقدير الغاز
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
        except Exception as e:
            error_msg = f"فشل محاكاة المعاملة: {str(e)}"
            error_type = ErrorClassifier.classify(e)
            if slug:
                attempt = await attempt_manager.get_or_create(wallet_address, slug, chain_key)
                attempt.increment_attempt(error_msg, error_type)
                await attempt_manager.update(attempt)
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "simulation_failed",
                "error": str(e),
                "error_type": error_type.value
            }
        
        # 7. التحقق من رسوم الغاز الفعلية
        actual_gas_fee_usd = (tx["gas"] * w3.eth.gas_price / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            error_msg = f"رسوم الغاز الفعلية مرتفعة: ${actual_gas_fee_usd:.4f} (الحد: ${max_gas_fee_usd})"
            if slug:
                attempt = await attempt_manager.get_or_create(wallet_address, slug, chain_key)
                attempt.mark_failed(error_msg, ErrorType.TEMPORARY)
                await attempt_manager.update(attempt)
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "gas_too_high",
                "gas_fee_usd": actual_gas_fee_usd,
                "error": error_msg
            }
        
        # 8. التحقق من التكلفة الإجمالية
        total_cost_wei = total_value + (tx["gas"] * w3.eth.gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            error_msg = f"الرصيد لا يكفي: ${(wallet_balance_wei / 1e18) * eth_price_usd:.4f} (المطلوب: ${(total_cost_wei / 1e18) * eth_price_usd:.4f})"
            if slug:
                attempt = await attempt_manager.get_or_create(wallet_address, slug, chain_key)
                attempt.mark_failed(error_msg, ErrorType.PERMANENT)
                await attempt_manager.update(attempt)
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "insufficient_funds_for_total_cost",
                "error": error_msg
            }
        
        # 9. توقيع وإرسال المعاملة
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        tx_hash_hex = tx_hash.hex()
        
        # 10. تسجيل النجاح
        if slug:
            attempt = await attempt_manager.get_or_create(wallet_address, slug, chain_key)
            attempt.mark_success(tx_hash_hex, quantity)
            await attempt_manager.update(attempt)
        
        log.info(f"[شراء ناجح - {checksum_wallet[:8]}] {tx_hash_hex} — كمية: {quantity}")
        
        return {
            "success": True,
            "wallet": checksum_wallet,
            "tx_hash": tx_hash_hex,
            "quantity": quantity,
            "gas_fee_usd": actual_gas_fee_usd,
            "total_value_wei": total_value,
            "attempts": attempt.attempt_count + 1 if slug else 0
        }
        
    except Exception as e:
        error_msg = str(e)
        error_type = ErrorClassifier.classify(e)
        log.error(f"[خطأ إرسال للمحفظة {checksum_wallet[:8]}] {error_msg}")
        
        if slug:
            attempt = await attempt_manager.get_or_create(wallet_address, slug, chain_key)
            attempt.increment_attempt(error_msg, error_type)
            await attempt_manager.update(attempt)
            
            # التحقق من تجاوز الحد الأقصى
            if attempt.attempt_count >= attempt.max_attempts:
                attempt.mark_gave_up(f"تجاوز الحد الأقصى للمحاولات ({attempt.max_attempts})")
                await attempt_manager.update(attempt)
        
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "tx_error",
            "error": error_msg,
            "error_type": error_type.value,
            "attempts": attempt.attempt_count if slug else 0
        }

# ===================== دوال مساعدة للاستخدام الخارجي =====================

async def get_attempt_summary(wallet: str, slug: str) -> Dict[str, Any]:
    """الحصول على ملخص محاولات الشراء"""
    attempt = await attempt_manager.get(wallet, slug)
    if not attempt:
        return {"status": "no_attempt", "attempts": 0}
    return attempt.to_dict()

async def get_global_summary() -> Dict[str, Any]:
    """الحصول على الملخص العالمي"""
    return await attempt_manager.get_summary()

async def clear_attempts(wallet: str = None, slug: str = None):
    """مسح سجل المحاولات"""
    await attempt_manager.clear(wallet, slug)

async def should_retry_purchase(wallet: str, slug: str) -> bool:
    """التحقق من إمكانية إعادة المحاولة"""
    return await attempt_manager.should_retry(wallet, slug)

async def is_permanent_failure(wallet: str, slug: str) -> bool:
    """التحقق من فشل دائم"""
    return await attempt_manager.is_permanent_failure(wallet, slug)
