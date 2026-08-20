"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
يدعم EIP-1559، إعادة المحاولة، وإدارة الأخطاء المتقدمة.
"""

import asyncio
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass

from web3 import Web3
from web3.exceptions import TransactionNotFound, ContractLogicError, TimeExhausted

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

def build_eip1559_transaction(w3: Web3, tx_dict: dict) -> dict:
    """بناء معاملة باستخدام EIP-1559"""
    try:
        # جلب البيانات الحديثة
        latest_block = w3.eth.get_block('pending')
        base_fee = latest_block.get('baseFeePerGas', w3.eth.gas_price // 2)
        
        # تقدير رسوم الأولوية المثلى
        try:
            max_priority = w3.eth.max_priority_fee
        except:
            max_priority = w3.eth.gas_price // 10  # 10% من السعر كبديل
        
        # حساب الرسوم القصوى (2x قاعدة + الأولوية)
        max_fee = int(base_fee * 2 + max_priority)
        
        # تحديث المعاملة
        eip1559_tx = {
            **tx_dict,
            'maxFeePerGas': max_fee,
            'maxPriorityFeePerGas': max_priority,
            'type': 2,  # EIP-1559
        }
        
        # إزالة الحقول القديمة إذا وجدت
        eip1559_tx.pop('gasPrice', None)
        
        return eip1559_tx
    except Exception as e:
        log.warning(f"⚠️ تعذر بناء EIP-1559، استخدام الطريقة القديمة: {e}")
        return tx_dict

# ==================== إرسال المعاملة مع إعادة المحاولة ====================
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
    محاولة الشراء بمحفظة واحدة مع إعادة المحاولة التلقائية
    """
    checksum_wallet = Web3.to_checksum_address(wallet_address)
    checksum_contract = Web3.to_checksum_address(nft_contract)
    
    for attempt in range(max_retries):
        try:
            return await _attempt_purchase(
                w3, private_key, checksum_wallet, checksum_contract,
                price_wei_per_token, max_per_wallet, remaining_supply,
                eth_price_usd, max_gas_fee_usd
            )
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
                    log.error(f"❌ فشلت جميع المحاولات ({max_retries}) للمحفظة {checksum_wallet[:8]}")
                    return {
                        "success": False,
                        "wallet": checksum_wallet,
                        "reason": "retry_exhausted",
                        "error": str(e)
                    }
            else:
                # أخطاء غير قابلة لإعادة المحاولة
                log.error(f"❌ فشل نهائي للمحفظة {checksum_wallet[:8]}: {e}")
                return {
                    "success": False,
                    "wallet": checksum_wallet,
                    "reason": "permanent_error",
                    "error": str(e)
                }
    
    return {"success": False, "wallet": checksum_wallet, "reason": "unknown_error"}

async def _attempt_purchase(
    w3: Web3,
    private_key: str,
    checksum_wallet: str,
    checksum_contract: str,
    price_wei_per_token: int,
    max_per_wallet: Optional[int],
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> Dict[str, Any]:
    """محاولة الشراء الفعلية"""
    
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

    # 5. بناء المعاملة
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

        # 7. تطبيق EIP-1559
        tx = build_eip1559_transaction(w3, tx)

        # 8. التحقق النهائي من التكلفة
        actual_gas_fee_usd = (tx["gas"] * tx.get("maxFeePerGas", w3.eth.gas_price) / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "gas_too_high",
                "gas_fee_usd": actual_gas_fee_usd
            }

        total_cost_wei = total_value + (tx["gas"] * tx.get("maxFeePerGas", w3.eth.gas_price))
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "insufficient_funds"
            }

        # 9. توقيع وإرسال المعاملة
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        
        # إرسال مع متابعة الوقت
        tx_hash = await asyncio.to_thread(
            w3.eth.send_raw_transaction,
            signed.raw_transaction
        )
        
        log.info(f"✅ [شراء - {checksum_wallet[:8]}] {tx_hash.hex()} — كمية: {quantity}")

        # 10. انتظار التأكيد (اختياري)
        try:
            receipt = await asyncio.to_thread(
                w3.eth.wait_for_transaction_receipt,
                tx_hash,
                timeout=60
            )
            if receipt.status != 1:
                return {
                    "success": False,
                    "wallet": checksum_wallet,
                    "reason": "transaction_failed",
                    "tx_hash": tx_hash.hex()
                }
        except TimeExhausted:
            log.warning(f"⚠️ لم يتم تأكيد المعاملة {tx_hash.hex()} خلال 60 ثانية")
            # نعتبرها ناجحة مؤقتاً، سنتحقق لاحقاً

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
        elif "replacement transaction underpriced" in error_msg.lower():
            return {"success": False, "wallet": checksum_wallet, "reason": "underpriced", "error": error_msg}
        else:
            return {"success": False, "wallet": checksum_wallet, "reason": "tx_error", "error": error_msg}
