"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
مع تحسينات في معالجة الأخطاء وإعادة المحاولة.
"""

import asyncio
import logging
import time
from web3 import Web3
from web3.exceptions import TransactionNotFound, ContractLogicError
from web3.middleware import ExtraDataToPOAMiddleware

log = logging.getLogger("buyer")

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

# إعدادات محسنة
MAX_GAS_RETRIES = 3
GAS_RETRY_DELAY = 2
NONCE_RETRY_DELAY = 1

# قفل خاص لكل محفظة
wallet_locks = {}

def get_wallet_lock(wallet_address: str) -> asyncio.Lock:
    addr = wallet_address.lower()
    if addr not in wallet_locks:
        wallet_locks[addr] = asyncio.Lock()
    return wallet_locks[addr]

def get_web3(rpc_url: str) -> Web3:
    """إنشاء كائن Web3 مع دعم POA"""
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    # إضافة middleware لـ POA (مطلوب لـ Robinhood Chain)
    try:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    except Exception:
        # قد يكون بالفعل مضافاً
        pass
    return w3

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

def get_fee_recipient(w3: Web3, nft_contract: str) -> str | None:
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

def decide_quantity(max_per_wallet: int | None, remaining_supply: int) -> int:
    if max_per_wallet is None:
        qty = 5
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    return max(1, min(qty, remaining_supply))

def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> int | None:
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return int(public_drop[0])
    except Exception as e:
        log.warning(f"[سعر on-chain] تعذر القراءة: {e}")
        return None

def get_nonce_safe(w3: Web3, wallet_address: str, max_retries: int = 5) -> int:
    """جلب الـ nonce بشكل آمن مع إعادة محاولة"""
    for attempt in range(max_retries):
        try:
            nonce = w3.eth.get_transaction_count(
                Web3.to_checksum_address(wallet_address),
                "pending"
            )
            return nonce
        except Exception as e:
            log.warning(f"محاولة {attempt+1} لجلب nonce فشلت: {e}")
            if attempt < max_retries - 1:
                time.sleep(NONCE_RETRY_DELAY)
    raise Exception(f"فشل جلب nonce بعد {max_retries} محاولات")

def estimate_gas_with_retry(w3: Web3, tx: dict, max_retries: int = 3) -> int:
    """تقدير الغاز مع إعادة محاولة"""
    for attempt in range(max_retries):
        try:
            estimated = w3.eth.estimate_gas(tx)
            return int(estimated * GAS_LIMIT_SAFETY_MARGIN)
        except ContractLogicError as e:
            log.error(f"خطأ منطق العقد: {e}")
            raise
        except Exception as e:
            log.warning(f"محاولة {attempt+1} لتقدير الغاز فشلت: {e}")
            if attempt < max_retries - 1:
                time.sleep(GAS_RETRY_DELAY)
    raise Exception(f"فشل تقدير الغاز بعد {max_retries} محاولات")

def attempt_purchase_single_wallet(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: int | None,
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> dict:
    """نسخة محسنة من attempt_purchase_single_wallet مع إعادة محاولة"""
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address", "error": str(e)}
    
    # فحص الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low", "balance_usd": balance_usd}
    
    # تقدير الغاز
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high", "gas_fee_usd": gas_fee_usd}
    
    # الحصول على عنوان الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        return {"success": False, "wallet": checksum_wallet, "reason": "no_fee_recipient"}
    
    # تحديد الكمية
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity
    
    # جلب nonce بشكل آمن
    try:
        nonce = get_nonce_safe(w3, checksum_wallet)
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "nonce_failed", "error": str(e)}
    
    # بناء المعاملة
    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
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
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "build_tx_failed", "error": str(e)}
    
    # تقدير الغاز مع إعادة محاولة
    try:
        tx["gas"] = estimate_gas_with_retry(w3, tx)
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_estimation_failed", "error": str(e)}
    
    # التحقق من تكلفة الغاز الفعلية
    try:
        gas_price = w3.eth.gas_price
        actual_gas_fee_usd = (tx["gas"] * gas_price / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high_actual", "gas_fee_usd": actual_gas_fee_usd}
        
        total_cost_wei = total_value + (tx["gas"] * gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds_for_total_cost"}
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "pre_send_check_failed", "error": str(e)}
    
    # توقيع وإرسال المعاملة مع إعادة محاولة
    for attempt in range(MAX_GAS_RETRIES):
        try:
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
            error_str = str(e)
            log.warning(f"محاولة {attempt+1} فشلت للمحفظة {checksum_wallet[:8]}: {error_str}")
            
            # معالجة أخطاء محددة
            if "nonce" in error_str.lower() and attempt < MAX_GAS_RETRIES - 1:
                try:
                    nonce = get_nonce_safe(w3, checksum_wallet)
                    tx["nonce"] = nonce
                    time.sleep(GAS_RETRY_DELAY)
                    continue
                except:
                    pass
            elif "gas" in error_str.lower() and attempt < MAX_GAS_RETRIES - 1:
                try:
                    tx["gas"] = estimate_gas_with_retry(w3, tx)
                    time.sleep(GAS_RETRY_DELAY)
                    continue
                except:
                    pass
            elif "insufficient funds" in error_str.lower():
                return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds", "error": error_str}
            elif attempt == MAX_GAS_RETRIES - 1:
                return {"success": False, "wallet": checksum_wallet, "reason": "max_retries_exceeded", "error": error_str}
    
    return {"success": False, "wallet": checksum_wallet, "reason": "max_retries_exceeded"}
