"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
محسّن للسرعة مع caching و batch processing.
"""

import asyncio
import logging
import time
from typing import Optional, Dict, Tuple

from web3 import Web3
from web3.middleware import geth_poa_middleware

log = logging.getLogger("buyer-fast")

# ============ Constants ============
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

# ============ Settings ============
MIN_BALANCE_RESERVE_USD = 0.10
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 15
GAS_LIMIT_SAFETY_MARGIN = 1.2
GAS_PRIORITY_INCREASE = 10**9  # 1 Gwei زيادة

# ============ Caches ============
_contract_cache: Dict[str, Tuple[Web3.eth.Contract, float]] = {}
_gas_price_cache: Dict[str, Any] = {"price": None, "ts": 0}
_nonce_cache: Dict[str, Tuple[int, int]] = {}
_nonce_cache_lock = asyncio.Lock()
_price_cache: Dict[str, Tuple[int, float]] = {}

# ============ Locks ============
wallet_locks: Dict[str, asyncio.Lock] = {}

# ============ Web3 Setup ============
def get_web3(rpc_url: str) -> Web3:
    """إنشاء اتصال Web3 محسّن"""
    w3 = Web3(Web3.HTTPProvider(
        rpc_url,
        request_kwargs={'timeout': 5}
    ))
    # إضافة middleware لتسريع المعاملات
    try:
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception:
        pass
    return w3

def get_wallet_lock(wallet_address: str) -> asyncio.Lock:
    """الحصول على قفل المحفظة"""
    addr = wallet_address.lower()
    if addr not in wallet_locks:
        wallet_locks[addr] = asyncio.Lock()
    return wallet_locks[addr]

# ============ Cached Contract ============
def get_cached_contract(w3: Web3, address: str, abi: list) -> Web3.eth.Contract:
    """الحصول على نسخة العقد من cache"""
    key = f"{address}_{id(w3)}"
    now = time.time()
    
    if key in _contract_cache:
        contract, timestamp = _contract_cache[key]
        if now - timestamp < 60:  # دقيقة واحدة
            return contract
    
    contract = w3.eth.contract(address=address, abi=abi)
    _contract_cache[key] = (contract, now)
    return contract

# ============ Cached Gas Price ============
def get_cached_gas_price(w3: Web3) -> int:
    """الحصول على سعر الغاز من cache"""
    now = time.time()
    if _gas_price_cache["price"] and (now - _gas_price_cache["ts"] < 2):
        return _gas_price_cache["price"]
    
    try:
        price = w3.eth.gas_price
        _gas_price_cache["price"] = price
        _gas_price_cache["ts"] = now
        return price
    except:
        return _gas_price_cache["price"] or 50 * 10**9

# ============ Nonce Management ============
async def get_cached_nonce(w3: Web3, wallet: str) -> int:
    """الحصول على nonce مع تتبع المعاملات المعلقة"""
    key = f"{wallet}_{id(w3)}"
    
    async with _nonce_cache_lock:
        if key in _nonce_cache:
            nonce, pending_count = _nonce_cache[key]
            if pending_count == 0:
                try:
                    new_nonce = w3.eth.get_transaction_count(wallet, "pending")
                    if new_nonce > nonce:
                        _nonce_cache[key] = (new_nonce, 0)
                        return new_nonce
                except:
                    pass
            return nonce
        else:
            nonce = w3.eth.get_transaction_count(wallet, "pending")
            _nonce_cache[key] = (nonce, 0)
            return nonce

def increment_nonce(wallet: str):
    """زيادة nonce بعد إرسال معاملة"""
    key = wallet
    if key in _nonce_cache:
        nonce, pending = _nonce_cache[key]
        _nonce_cache[key] = (nonce + 1, pending + 1)

def confirm_nonce(wallet: str, nonce: int):
    """تأكيد اكتمال المعاملة"""
    key = wallet
    if key in _nonce_cache:
        cached_nonce, pending = _nonce_cache[key]
        if nonce == cached_nonce - 1:
            _nonce_cache[key] = (cached_nonce, max(0, pending - 1))

# ============ Balance ============
def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    """جلب رصيد المحفظة"""
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة للمحفظة {wallet_address[:8]}...: {e}")
        return 0.0

# ============ Gas Estimation ============
def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000) -> float:
    """تقدير رسوم الغاز"""
    try:
        gas_price_wei = get_cached_gas_price(w3)
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"[الغاز] تعذر التقدير: {e}")
        return float("inf")

# ============ Fee Recipient ============
def get_fee_recipient(w3: Web3, nft_contract: str) -> Optional[str]:
    """الحصول على مستلم الرسوم"""
    try:
        seadrop = get_cached_contract(w3, SEADROP_ADDRESS, SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        if not recipients:
            return None
        return Web3.to_checksum_address(recipients[0])
    except Exception as e:
        log.error(f"[عنوان الرسوم] خطأ استعلام: {e}")
        return None

# ============ Quantity Decision ============
def decide_quantity(max_per_wallet: Optional[int], remaining_supply: int) -> int:
    """تحديد الكمية المناسبة للشراء"""
    if max_per_wallet is None:
        qty = 5
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    return max(1, min(qty, remaining_supply))

# ============ Onchain Price ============
def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> Optional[int]:
    """جلب السعر من onchain مع cache"""
    try:
        # استخدام cache
        key = f"{nft_contract}_{id(w3)}"
        now = time.time()
        if key in _price_cache:
            price, timestamp = _price_cache[key]
            if now - timestamp < 10:  # 10 ثواني
                return price
        
        seadrop = get_cached_contract(w3, SEADROP_ADDRESS, SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        price = int(public_drop[0])
        _price_cache[key] = (price, now)
        return price
    except Exception as e:
        log.warning(f"[سعر on-chain] تعذر القراءة: {e}")
        return None

# ============ Main Purchase Function ============
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
    """محاولة الشراء بمحفظة واحدة محددة - محسّن للسرعة"""
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address", "error": str(e)}

    # 1. التحقق من الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low", "balance_usd": balance_usd}

    # 2. تقدير الغاز
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high", "gas_fee_usd": gas_fee_usd}

    # 3. الحصول على مستلم الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        return {"success": False, "wallet": checksum_wallet, "reason": "no_fee_recipient"}

    # 4. تحديد الكمية
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity

    try:
        # 5. بناء المعاملة
        contract = get_cached_contract(w3, SEADROP_ADDRESS, SEADROP_ABI)
        
        # الحصول على nonce من cache
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
        
        # بناء المعاملة مع إعدادات السرعة
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
            "type": 2,  # EIP-1559
        })

        # 6. تقدير الغاز مع margin
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
        except Exception as e:
            return {"success": False, "wallet": checksum_wallet, "reason": "simulation_failed", "error": str(e)}

        # 7. التحقق من رسوم الغاز النهائية
        gas_price = get_cached_gas_price(w3)
        actual_gas_fee_usd = (tx["gas"] * gas_price / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high", "gas_fee_usd": actual_gas_fee_usd}

        # 8. التحقق من الرصيد الكافي للتكلفة الكلية
        total_cost_wei = total_value + (tx["gas"] * gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds_for_total_cost"}

        # 9. إضافة أولوية للغاز لتسريع المعاملة
        tx["maxPriorityFeePerGas"] = gas_price + GAS_PRIORITY_INCREASE
        tx["maxFeePerGas"] = gas_price * 2

        # 10. توقيع وإرسال المعاملة
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
        log.error(f"[خطأ إرسال للمحفظة {checksum_wallet[:8]}] {e}")
        return {"success": False, "wallet": checksum_wallet, "reason": "tx_error", "error": str(e)}
