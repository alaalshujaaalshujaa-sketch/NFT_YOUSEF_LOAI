"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
"""

import asyncio
import logging
import time
from typing import Optional, Dict, Any, Tuple
from collections import defaultdict

from web3 import Web3
from web3.exceptions import TransactionNotFound, TimeExhausted

log = logging.getLogger("buyer")

# ---------------------------------------------------------------------------
# الثوابت
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# إدارة الأقفال
# ---------------------------------------------------------------------------

class LockManager:
    """إدارة أقفال المحافظ لمنع تضارب المعاملات"""
    
    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._ref_counts: Dict[str, int] = defaultdict(int)
        self._cleanup_counter = 0
        self._cleanup_threshold = 100
    
    def get_lock(self, wallet_address: str) -> asyncio.Lock:
        """الحصول على قفل لمحفظة معينة"""
        addr = wallet_address.lower()
        self._ref_counts[addr] += 1
        
        if addr not in self._locks:
            self._locks[addr] = asyncio.Lock()
        
        # تنظيف دوري
        self._cleanup_counter += 1
        if self._cleanup_counter >= self._cleanup_threshold:
            self._cleanup()
            self._cleanup_counter = 0
        
        return self._locks[addr]
    
    def release_lock(self, wallet_address: str):
        """تحرير قفل محفظة"""
        addr = wallet_address.lower()
        self._ref_counts[addr] -= 1
        
        if self._ref_counts[addr] <= 0:
            self._locks.pop(addr, None)
            self._ref_counts.pop(addr, None)
    
    def _cleanup(self):
        """تنظيف الأقفال غير المستخدمة"""
        unused = [
            addr for addr, count in self._ref_counts.items()
            if count <= 0
        ]
        for addr in unused:
            self._locks.pop(addr, None)
            self._ref_counts.pop(addr, None)

# ---------------------------------------------------------------------------
# دوال Web3 الأساسية
# ---------------------------------------------------------------------------

def get_web3(rpc_url: str) -> Web3:
    """إنشاء Web3 instance"""
    return Web3(Web3.HTTPProvider(rpc_url))

def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    """جلب رصيد المحفظة بالدولار"""
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة للمحفظة {wallet_address[:8]}...: {e}")
        return 0.0

def estimate_gas_fee_usd(
    w3: Web3,
    eth_price_usd: float,
    gas_units: int = 150_000,
    use_eip1559: bool = True,
) -> float:
    """تقدير رسوم الغاز بالدولار"""
    try:
        if use_eip1559:
            # محاولة استخدام EIP-1559
            try:
                latest_block = w3.eth.get_block('latest')
                base_fee = latest_block.get('baseFeePerGas', None)
                
                if base_fee:
                    priority_fee = w3.eth.max_priority_fee
                    max_fee = int(base_fee * 1.5 + priority_fee)
                    fee_eth = (max_fee * gas_units) / 1e18
                    return fee_eth * eth_price_usd
            except:
                pass
        
        # Fallback للغاز العادي
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
        
    except Exception as e:
        log.warning(f"[الغاز] تعذر التقدير: {e}")
        return float("inf")

def get_fee_recipient(w3: Web3, nft_contract: str) -> Optional[str]:
    """جلب عنوان مستلم الرسوم"""
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

def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> Optional[int]:
    """جلب سعر المينت من السلسلة"""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return int(public_drop[0])
    except Exception as e:
        log.warning(f"[سعر on-chain] تعذر القراءة: {e}")
        return None

# ---------------------------------------------------------------------------
# منطق الكمية
# ---------------------------------------------------------------------------

def decide_quantity(
    max_per_wallet: Optional[int],
    remaining_supply: int,
    strategy: str = "moderate",
) -> int:
    """تحديد كمية الشراء بناءً على الاستراتيجية"""
    
    strategies = {
        "conservative": lambda: 1,  # شراء 1 فقط دائماً
        "moderate": lambda: min(5, max_per_wallet or 5),  # شراء حتى 5
        "aggressive": lambda: max_per_wallet if max_per_wallet and max_per_wallet <= FEW_THRESHOLD else LIMITED_BUY_QTY,
        "max": lambda: max_per_wallet or remaining_supply,  # شراء الحد الأقصى
    }
    
    qty = strategies.get(strategy, strategies["moderate"])()
    return max(1, min(qty, remaining_supply))

# ---------------------------------------------------------------------------
# بناء المعاملة
# ---------------------------------------------------------------------------

def build_transaction(
    w3: Web3,
    contract_address: str,
    wallet_address: str,
    fee_recipient: str,
    quantity: int,
    total_value: int,
    nonce: int,
    use_eip1559: bool = True,
) -> dict:
    """بناء معاملة الشراء مع دعم EIP-1559"""
    
    contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
    
    tx_params = {
        "from": wallet_address,
        "value": total_value,
        "nonce": nonce,
        "chainId": w3.eth.chain_id,
    }
    
    # إضافة gas price
    if use_eip1559:
        try:
            latest_block = w3.eth.get_block('latest')
            base_fee = latest_block.get('baseFeePerGas', None)
            
            if base_fee:
                priority_fee = w3.eth.max_priority_fee
                max_fee = int(base_fee * 1.5 + priority_fee)
                tx_params["maxFeePerGas"] = max_fee
                tx_params["maxPriorityFeePerGas"] = priority_fee
        except:
            pass
    
    # إذا لم يتم استخدام EIP-1559 أو فشل
    if "maxFeePerGas" not in tx_params:
        gas_price = int(w3.eth.gas_price * 1.2)  # 20% buffer
        tx_params["gasPrice"] = gas_price
    
    tx = contract.functions.mintPublic(
        Web3.to_checksum_address(contract_address),
        Web3.to_checksum_address(fee_recipient),
        ZERO_ADDRESS,
        quantity,
    ).build_transaction(tx_params)
    
    return tx

# ---------------------------------------------------------------------------
# تحليل أخطاء المعاملة
# ---------------------------------------------------------------------------

def analyze_transaction_error(error_msg: str) -> str:
    """تحليل رسالة الخطأ لتحديد السبب"""
    
    error_lower = error_msg.lower()
    
    if "execution reverted" in error_lower:
        if "mintnotopen" in error_lower or "not open" in error_lower:
            return "mint_not_open"
        elif "soldout" in error_lower or "sold out" in error_lower:
            return "sold_out"
        elif "insufficient" in error_lower:
            return "insufficient_funds"
        elif "max" in error_lower and "wallet" in error_lower:
            return "max_per_wallet_exceeded"
        else:
            return "execution_reverted"
    elif "nonce too low" in error_lower:
        return "nonce_too_low"
    elif "replacement transaction underpriced" in error_lower:
        return "replacement_underpriced"
    elif "insufficient funds" in error_lower:
        return "insufficient_funds"
    else:
        return "unknown_error"

# ---------------------------------------------------------------------------
# الشراء مع إعادة المحاولة
# ---------------------------------------------------------------------------

async def purchase_with_retry(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: Optional[int],
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
    max_retries: int = 3,
    retry_delay_base: float = 1.0,
) -> dict:
    """محاولة الشراء مع إعادة المحاولة للأخطاء المؤقتة"""
    
    permanent_failures = {
        "invalid_address",
        "balance_too_low",
        "no_fee_recipient",
        "sold_out",
        "mint_not_open",
        "max_per_wallet_exceeded",
    }
    
    last_result = None
    
    for attempt in range(max_retries):
        result = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            w3=w3,
            private_key=private_key,
            wallet_address=wallet_address,
            nft_contract=nft_contract,
            price_wei_per_token=price_wei_per_token,
            max_per_wallet=max_per_wallet,
            remaining_supply=remaining_supply,
            eth_price_usd=eth_price_usd,
            max_gas_fee_usd=max_gas_fee_usd,
        )
        
        last_result = result
        
        # نجاح
        if result.get("success"):
            return result
        
        # فشل دائم - لا تعيد المحاولة
        if result.get("reason") in permanent_failures:
            return result
        
        # إعادة المحاولة مع backoff
        if attempt < max_retries - 1:
            delay = retry_delay_base * (2 ** attempt)  # exponential backoff
            log.info(
                f"إعادة المحاولة ({attempt + 1}/{max_retries}) "
                f"للمحفظة {wallet_address[:8]}... بعد {delay:.1f} ثانية"
            )
            await asyncio.sleep(delay)
    
    return last_result or {"success": False, "wallet": wallet_address, "reason": "max_retries_exceeded"}

# ---------------------------------------------------------------------------
# محاولة الشراء لمحفظة واحدة
# ---------------------------------------------------------------------------

def attempt_purchase_single_wallet(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: Optional[int],
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> dict:
    """محاولة الشراء بمحفظة واحدة محددة"""
    
    # التحقق من صحة العناوين
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {
            "success": False,
            "wallet": wallet_address,
            "reason": "invalid_address",
            "error": str(e),
        }
    
    # فحص الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "balance_too_low",
            "balance_usd": balance_usd,
        }
    
    # فحص رسوم الغاز
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "gas_too_high",
            "gas_fee_usd": gas_fee_usd,
        }
    
    # جلب عنوان الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "no_fee_recipient",
        }
    
    # تحديد الكمية
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity
    
    try:
        # جلب nonce
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
        
        # بناء المعاملة
        tx = build_transaction(
            w3=w3,
            contract_address=checksum_contract,
            wallet_address=checksum_wallet,
            fee_recipient=fee_recipient,
            quantity=quantity,
            total_value=total_value,
            nonce=nonce,
        )
        
        # تقدير الغاز مع تحليل الأخطاء
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
        except Exception as e:
            error_reason = analyze_transaction_error(str(e))
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": error_reason,
                "error": str(e),
            }
        
        # التحقق من رسوم الغاز الفعلية
        actual_gas_fee_usd = (tx["gas"] * w3.eth.gas_price / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "gas_too_high",
                "gas_fee_usd": actual_gas_fee_usd,
            }
        
        # التحقق من كفاية الرصيد
        total_cost_wei = total_value + (tx["gas"] * w3.eth.gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "insufficient_funds_for_total_cost",
            }
        
        # توقيع وإرسال المعاملة
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        
        log.info(f"[شراء ناجح - {checksum_wallet[:8]}] {tx_hash.hex()} — كمية: {quantity}")
        
        return {
            "success": True,
            "wallet": checksum_wallet,
            "tx_hash": tx_hash.hex(),
            "quantity": quantity,
            "gas_fee_usd": actual_gas_fee_usd,
            "total_value_wei": total_value,
        }
    
    except Exception as e:
        error_reason = analyze_transaction_error(str(e))
        log.error(f"[خطأ إرسال للمحفظة {checksum_wallet[:8]}] {e}")
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": error_reason,
            "error": str(e),
        }
