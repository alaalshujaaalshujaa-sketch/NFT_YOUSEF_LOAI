"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
"""

import asyncio
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass

from web3 import Web3
from web3.exceptions import ContractLogicError, TimeExhausted

log = logging.getLogger("buyer")

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

# ==================== الإعدادات ====================
MIN_BALANCE_RESERVE_USD = 0.10
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 15
GAS_LIMIT_SAFETY_MARGIN = 1.2
MAX_RETRY_DELAY = 10
RETRY_BACKOFF_FACTOR = 1.5

# ==================== هيكلة البيانات ====================
@dataclass
class WalletData:
    """بيانات المحفظة الواحدة"""
    wallet: str
    private_key: str
    bot_token: str
    chat_id: str
    current_detail: dict = None
    chain_key: str = ""

# ==================== الأقفال ====================
wallet_locks: Dict[str, asyncio.Lock] = {}

def get_wallet_lock(wallet_address: str) -> asyncio.Lock:
    """الحصول على قفل المحفظة لتجنب تضارب المعاملات"""
    addr = wallet_address.lower()
    if addr not in wallet_locks:
        wallet_locks[addr] = asyncio.Lock()
    return wallet_locks[addr]

# ==================== دوال Web3 ====================
def get_web3(rpc_url: str) -> Web3:
    """إنشاء اتصال Web3"""
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise ConnectionError(f"❌ تعذر الاتصال بـ {rpc_url}")
    return w3

def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    """جلب رصيد المحفظة بالدولار"""
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"❌ [الرصيد] تعذر القراءة للمحفظة {wallet_address[:8]}...: {e}")
        return 0.0

def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000) -> float:
    """تقدير رسوم الغاز بالدولار"""
    try:
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"⚠️ [الغاز] تعذر التقدير: {e}")
        return float("inf")

def get_fee_recipient(w3: Web3, nft_contract: str) -> Optional[str]:
    """جلب مستلم الرسوم من العقد"""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        if not recipients:
            return None
        return Web3.to_checksum_address(recipients[0])
    except Exception as e:
        log.error(f"❌ [عنوان الرسوم] خطأ استعلام: {e}")
        return None

def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> Optional[int]:
    """جلب السعر العام من العقد"""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return int(public_drop[0])
    except Exception as e:
        log.warning(f"⚠️ [سعر on-chain] تعذر القراءة: {e}")
        return None

def decide_quantity(max_per_wallet: Optional[int], remaining_supply: int) -> int:
    """تحديد الكمية المناسبة للشراء"""
    if max_per_wallet is None:
        qty = 5
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    return max(1, min(qty, remaining_supply))

# ==================== دالة الشراء الرئيسية (متوافقة مع main.py) ====================

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
) -> Dict[str, Any]:
    """
    محاولة الشراء بمحفظة واحدة (متوافقة مع main.py القديم)
    هذه الدالة تعمل بشكل متزامن (synchronous) كما هو متوقع في main.py
    """
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address", "error": str(e)}

    # 1. التحقق من الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "balance_too_low",
            "balance_usd": balance_usd
        }

    # 2. تقدير رسوم الغاز
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "gas_too_high",
            "gas_fee_usd": gas_fee_usd
        }

    # 3. جلب مستلم الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "no_fee_recipient"
        }

    # 4. تحديد الكمية
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity

    try:
        # 5. بناء المعاملة
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
        except ContractLogicError as e:
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "contract_reverted",
                "error": str(e)
            }
        except Exception as e:
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "estimation_failed",
                "error": str(e)
            }

        # 7. التحقق من تكلفة الغاز
        gas_price = w3.eth.gas_price
        actual_gas_fee_usd = (tx["gas"] * gas_price / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "gas_too_high",
                "gas_fee_usd": actual_gas_fee_usd
            }

        # 8. التحقق من الرصيد الكافي
        total_cost_wei = total_value + (tx["gas"] * gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "insufficient_funds_for_total_cost"
            }

        # 9. توقيع وإرسال المعاملة
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)

        log.info(f"✅ [شراء ناجح - {checksum_wallet[:8]}] {tx_hash.hex()} — كمية: {quantity}")
        
        return {
            "success": True,
            "wallet": checksum_wallet,
            "tx_hash": tx_hash.hex(),
            "quantity": quantity,
            "gas_fee_usd": actual_gas_fee_usd,
            "total_value_wei": total_value,
        }

    except Exception as e:
        error_msg = str(e)
        log.error(f"❌ [خطأ إرسال للمحفظة {checksum_wallet[:8]}] {error_msg}")
        
        # تصنيف الخطأ
        if "nonce" in error_msg.lower():
            return {"success": False, "wallet": checksum_wallet, "reason": "nonce_error", "error": error_msg}
        elif "insufficient funds" in error_msg.lower():
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds", "error": error_msg}
        else:
            return {"success": False, "wallet": checksum_wallet, "reason": "tx_error", "error": error_msg}

# ==================== دالة الشراء غير المتزامنة (للإصدارات الجديدة) ====================

async def send_transaction_with_retry(
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
) -> Dict[str, Any]:
    """
    محاولة الشراء بمحفظة واحدة مع إعادة المحاولة التلقائية (نسخة غير متزامنة)
    """
    for attempt in range(max_retries):
        try:
            # استخدام الدالة المتزامنة مع to_thread
            result = await asyncio.to_thread(
                attempt_purchase_single_wallet,
                w3, private_key, wallet_address, nft_contract,
                price_wei_per_token, max_per_wallet, remaining_supply,
                eth_price_usd, max_gas_fee_usd
            )
            return result
        except Exception as e:
            error_msg = str(e).lower()
            
            # حالات قابلة لإعادة المحاولة
            retryable_errors = [
                "timeout", "connection", "network", "nonce",
                "replacement transaction underpriced",
                "already known", "underpriced"
            ]
            
            if any(err in error_msg for err in retryable_errors):
                if attempt < max_retries - 1:
                    delay = min(RETRY_BACKOFF_FACTOR ** attempt, MAX_RETRY_DELAY)
                    log.warning(f"🔄 محاولة {attempt + 1}/{max_retries} فشلت: {e}. إعادة المحاولة بعد {delay:.1f} ثوانٍ")
                    await asyncio.sleep(delay)
                    continue
                else:
                    log.error(f"❌ فشلت جميع المحاولات ({max_retries}) للمحفظة {wallet_address[:8]}")
                    return {
                        "success": False,
                        "wallet": wallet_address,
                        "reason": "retry_exhausted",
                        "error": str(e)
                    }
            else:
                # أخطاء غير قابلة لإعادة المحاولة
                log.error(f"❌ فشل نهائي للمحفظة {wallet_address[:8]}: {e}")
                return {
                    "success": False,
                    "wallet": wallet_address,
                    "reason": "permanent_error",
                    "error": str(e)
                }
    
    return {"success": False, "wallet": wallet_address, "reason": "unknown_error"}
