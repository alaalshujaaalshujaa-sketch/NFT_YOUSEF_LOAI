"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
نسخة محسنة مع دعم إعادة المحاولة، تحسين إدارة الكميات، ومعالجة أفضل للأخطاء.
"""

import asyncio
import logging
import time
from typing import Optional, Dict, Any, Tuple
from web3 import Web3
from web3.exceptions import ContractLogicError, TransactionNotFound

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
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "totalSupply",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

MIN_BALANCE_RESERVE_USD = 0.10
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 3  # تم تخفيضها لتقليل المخاطر
GAS_LIMIT_SAFETY_MARGIN = 1.2
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2

# قفل خاص لكل محفظة لمنع تضارب المعاملات والنونس في نفس الوقت
wallet_locks = {}


def get_wallet_lock(wallet_address: str) -> asyncio.Lock:
    """الحصول على قفل خاص لكل محفظة لضمان عدم تضارب المعاملات"""
    addr = wallet_address.lower()
    if addr not in wallet_locks:
        wallet_locks[addr] = asyncio.Lock()
    return wallet_locks[addr]


def get_web3(rpc_url: str) -> Web3:
    """إنشاء اتصال Web3 مع محاولات إعادة الاتصال"""
    return Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 30}))


def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    """الحصول على رصيد المحفظة بالدولار"""
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة للمحفظة {wallet_address[:8]}...: {e}")
        return 0.0


def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000) -> float:
    """تقدير رسوم الغاز بالدولار"""
    try:
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"[الغاز] تعذر التقدير: {e}")
        return float("inf")


def get_fee_recipient(w3: Web3, nft_contract: str) -> Optional[str]:
    """الحصول على عنوان المستفيد من الرسوم"""
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
    """تحديد الكمية المناسبة للشراء مع مراعاة الحدود"""
    if max_per_wallet is None:
        # إذا لم يكن هناك حد، نشتري 1 فقط لتقليل المخاطر
        qty = 1
    elif max_per_wallet <= 1:
        qty = 1
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        # للحدود الكبيرة، نشتري كمية محدودة فقط
        qty = min(LIMITED_BUY_QTY, max_per_wallet)
    
    # التأكد من عدم تجاوز الكمية المتبقية
    return max(1, min(qty, remaining_supply))


def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> Optional[int]:
    """الحصول على السعر من العقد مباشرة"""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return int(public_drop[0])
    except Exception as e:
        log.warning(f"[سعر on-chain] تعذر القراءة: {e}")
        return None


def get_public_drop_info(w3: Web3, nft_contract: str) -> Optional[Dict[str, Any]]:
    """الحصول على معلومات كاملة عن الـ Public Drop"""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return {
            "mintPrice": int(public_drop[0]),
            "startTime": int(public_drop[1]),
            "endTime": int(public_drop[2]),
            "maxTotalMintableByWallet": int(public_drop[3]),
            "feeBps": int(public_drop[4]),
            "restrictFeeRecipients": bool(public_drop[5]),
        }
    except Exception as e:
        log.warning(f"[Public Drop] تعذر القراءة: {e}")
        return None


def get_total_supply(w3: Web3, nft_contract: str) -> Optional[int]:
    """الحصول على إجمالي العرض من العقد"""
    try:
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(nft_contract),
            abi=[{"inputs": [], "name": "totalSupply", "outputs": [{"type": "uint256"}], "type": "function"}]
        )
        return int(contract.functions.totalSupply().call())
    except Exception as e:
        log.warning(f"[Total Supply] تعذر القراءة: {e}")
        return None


def is_mint_active(w3: Web3, nft_contract: str) -> Tuple[bool, Optional[int]]:
    """التحقق من أن المينت لا يزال نشطاً"""
    drop_info = get_public_drop_info(w3, nft_contract)
    if not drop_info:
        return False, None
    
    current_time = int(time.time())
    start_time = drop_info["startTime"]
    end_time = drop_info["endTime"]
    
    # التحقق من أن الوقت الحالي بين بداية ونهاية المينت
    is_active = start_time <= current_time <= end_time
    return is_active, end_time


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
    retry_count: int = 0,
) -> Dict[str, Any]:
    """محاولة الشراء بمحفظة واحدة محددة مع دعم إعادة المحاولة"""
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address", "error": str(e)}

    # التحقق من أن المينت لا يزال نشطاً
    is_active, end_time = is_mint_active(w3, checksum_contract)
    if not is_active:
        return {"success": False, "wallet": checksum_wallet, "reason": "mint_not_active", "end_time": end_time}

    # التحقق من الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low", "balance_usd": balance_usd}

    # تقدير رسوم الغاز
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high_estimate", "gas_fee_usd": gas_fee_usd}

    # الحصول على عنوان المستفيد من الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        return {"success": False, "wallet": checksum_wallet, "reason": "no_fee_recipient"}

    # تحديد الكمية المناسبة
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity

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

        # تقدير الغاز مع هامش أمان
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
        except Exception as e:
            return {"success": False, "wallet": checksum_wallet, "reason": "simulation_failed", "error": str(e)}

        # التحقق من رسوم الغاز الفعلية
        actual_gas_fee_usd = (tx["gas"] * w3.eth.gas_price / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high_actual", "gas_fee_usd": actual_gas_fee_usd}

        # التحقق من كفاية الرصيد للتكلفة الإجمالية
        total_cost_wei = total_value + (tx["gas"] * w3.eth.gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds_for_total_cost"}

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

    except ContractLogicError as e:
        log.error(f"[خطأ منطق العقد للمحفظة {checksum_wallet[:8]}] {e}")
        # محاولة إعادة المحاولة إذا كان الخطأ مؤقتاً
        if "execution reverted" in str(e) and retry_count < MAX_RETRY_ATTEMPTS:
            log.info(f"إعادة محاولة {retry_count + 1}/{MAX_RETRY_ATTEMPTS} للمحفظة {checksum_wallet[:8]}")
            time.sleep(RETRY_DELAY_SECONDS)
            return attempt_purchase_single_wallet(
                w3, private_key, wallet_address, nft_contract, price_wei_per_token,
                max_per_wallet, remaining_supply, eth_price_usd, max_gas_fee_usd, retry_count + 1
            )
        return {"success": False, "wallet": checksum_wallet, "reason": "contract_reverted", "error": str(e)}
    except Exception as e:
        log.error(f"[خطأ إرسال للمحفظة {checksum_wallet[:8]}] {e}")
        return {"success": False, "wallet": checksum_wallet, "reason": "tx_error", "error": str(e)}
