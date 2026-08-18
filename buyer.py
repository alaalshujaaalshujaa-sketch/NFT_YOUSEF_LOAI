"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
دمج الكود الأصلي مع التحسينات:
- حد أقصى للغاز 5 سنتات
- حد السعر المجاني 0.01 دولار
- إعادة المحاولة التلقائية
- تخزين الحالة في قاعدة بيانات
"""

import asyncio
import logging
import time
from typing import Optional, Dict, Any, Tuple
from web3 import Web3
from web3.exceptions import ContractLogicError

log = logging.getLogger("buyer")

# ======================== الثوابت (من الكود الأصلي) ========================

SEADROP_ADDRESS = Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5")
ZERO_ADDRESS = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

# الحد الأقصى لرسوم الغاز (5 سنتات)
MAX_GAS_FEE_USD = 0.05

# حد السعر المجاني (0.01 دولار - كما في الكود الأصلي)
FREE_PRICE_THRESHOLD_USD = 0.01

# إعدادات الشراء (من الكود الأصلي)
MIN_BALANCE_RESERVE_USD = 0.10
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 3  # تم تخفيضها من 15 إلى 3
GAS_LIMIT_SAFETY_MARGIN = 1.2
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2

# ======================== ABI (من الكود الأصلي) ========================

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

# ======================== الأقفال (من الكود الأصلي) ========================

wallet_locks = {}


def get_wallet_lock(wallet_address: str) -> asyncio.Lock:
    """الحصول على قفل خاص لكل محفظة لضمان عدم تضارب المعاملات"""
    addr = wallet_address.lower()
    if addr not in wallet_locks:
        wallet_locks[addr] = asyncio.Lock()
    return wallet_locks[addr]


# ======================== دوال Web3 (من الكود الأصلي مع تحسينات) ========================

def get_web3(rpc_url: str) -> Web3:
    """إنشاء اتصال Web3"""
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
    
    is_active = start_time <= current_time <= end_time
    return is_active, end_time


def calculate_max_gas_units(eth_price_usd: float) -> int:
    """
    حساب الحد الأقصى لوحدات الغاز المسموح بها
    بحيث لا تتجاوز رسوم الغاز 5 سنتات
    """
    try:
        max_gas_in_eth = MAX_GAS_FEE_USD / eth_price_usd
        max_gas_units = int((max_gas_in_eth * 1e18) / 1)
        return int(max_gas_units * 0.8)
    except Exception as e:
        log.warning(f"[حساب الغاز] خطأ: {e}")
        return 150_000


def decide_quantity(max_per_wallet: Optional[int], remaining_supply: int) -> int:
    """تحديد الكمية المناسبة للشراء مع مراعاة الحدود"""
    if max_per_wallet is None:
        qty = 1
    elif max_per_wallet <= 1:
        qty = 1
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = min(LIMITED_BUY_QTY, max_per_wallet)
    
    return max(1, min(qty, remaining_supply))


def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    """
    التحقق من أن السعر مجاني أو لا يُذكر
    الحد الأدنى: 0.01 دولار (كما في الكود الأصلي)
    """
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD


# ======================== دالة الشراء الرئيسية (من الكود الأصلي مع تحسينات) ========================

def attempt_purchase_single_wallet(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: Optional[int],
    remaining_supply: int,
    eth_price_usd: float,
    retry_count: int = 0,
) -> Dict[str, Any]:
    """
    محاولة الشراء بمحفظة واحدة - مأخوذة من الكود الأصلي مع تحسينات الغاز
    """
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address", "error": str(e)}

    # 1. التحقق من نشاط المينت
    is_active, end_time = is_mint_active(w3, checksum_contract)
    if not is_active:
        return {"success": False, "wallet": checksum_wallet, "reason": "mint_not_active", "end_time": end_time}

    # 2. التحقق من الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low", "balance_usd": balance_usd}

    # 3. حساب الحد الأقصى للغاز
    gas_price_wei = w3.eth.gas_price
    max_gas_units = calculate_max_gas_units(eth_price_usd)
    gas_fee_per_unit_usd = (gas_price_wei / 1e18) * eth_price_usd
    estimated_max_gas_fee = gas_fee_per_unit_usd * max_gas_units

    # التحقق من أن رسوم الغاز لا تتجاوز 5 سنتات
    if estimated_max_gas_fee > MAX_GAS_FEE_USD:
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "gas_price_too_high",
            "gas_fee_usd": estimated_max_gas_fee,
            "max_allowed": MAX_GAS_FEE_USD
        }

    # 4. الحصول على عنوان المستفيد من الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        return {"success": False, "wallet": checksum_wallet, "reason": "no_fee_recipient"}

    # 5. تحديد الكمية
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity

    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")

        # بناء المعاملة (من الكود الأصلي)
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

        # 6. تقدير الغاز مع هامش أمان
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(min(estimated_gas * GAS_LIMIT_SAFETY_MARGIN, max_gas_units))
        except Exception as e:
            return {"success": False, "wallet": checksum_wallet, "reason": "simulation_failed", "error": str(e)}

        # 7. التحقق النهائي من رسوم الغاز
        actual_gas_fee_usd = (tx["gas"] * w3.eth.gas_price / 1e18) * eth_price_usd
        if actual_gas_fee_usd > MAX_GAS_FEE_USD:
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "gas_exceeds_limit",
                "gas_fee_usd": actual_gas_fee_usd,
                "max_allowed": MAX_GAS_FEE_USD,
                "gas_units": tx["gas"]
            }

        # 8. التحقق من كفاية الرصيد
        total_cost_wei = total_value + (tx["gas"] * w3.eth.gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds"}

        # 9. توقيع وإرسال المعاملة (من الكود الأصلي)
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)

        log.info(f"[✅ شراء ناجح] {checksum_wallet[:8]} | الكمية: {quantity} | الغاز: ${actual_gas_fee_usd:.4f}")
        
        return {
            "success": True,
            "wallet": checksum_wallet,
            "tx_hash": tx_hash.hex(),
            "quantity": quantity,
            "gas_fee_usd": actual_gas_fee_usd,
            "total_value_wei": total_value,
            "gas_units": tx["gas"],
            "max_gas_allowed": MAX_GAS_FEE_USD
        }

    except ContractLogicError as e:
        # إعادة المحاولة إذا كان الخطأ مؤقتاً
        if "execution reverted" in str(e) and retry_count < MAX_RETRY_ATTEMPTS:
            log.info(f"[🔄 إعادة محاولة {retry_count + 1}/{MAX_RETRY_ATTEMPTS}] {checksum_wallet[:8]}")
            time.sleep(RETRY_DELAY_SECONDS)
            return attempt_purchase_single_wallet(
                w3, private_key, wallet_address, nft_contract, price_wei_per_token,
                max_per_wallet, remaining_supply, eth_price_usd, retry_count + 1
            )
        return {"success": False, "wallet": checksum_wallet, "reason": "contract_reverted", "error": str(e)}
    
    except Exception as e:
        log.error(f"[❌ خطأ] {checksum_wallet[:8]}: {e}")
        return {"success": False, "wallet": checksum_wallet, "reason": "tx_error", "error": str(e)}
