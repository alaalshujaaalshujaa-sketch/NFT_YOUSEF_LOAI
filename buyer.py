"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
محسّن للسرعة مع caching.
"""

import asyncio
import logging
import time
from typing import Optional

from web3 import Web3

# محاولة استيراد geth_poa_middleware من مواقع مختلفة (للتوافق مع الإصدارات)
try:
    from web3.middleware import geth_poa_middleware
except ImportError:
    try:
        from web3.geth import geth_poa_middleware
    except ImportError:
        try:
            from web3.middleware.geth_poa import geth_poa_middleware
        except ImportError:
            geth_poa_middleware = None

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

# ============ Caches ============
_contract_cache = {}
_gas_price_cache = {"price": None, "ts": 0}
_price_cache = {}
wallet_locks = {}

# ============ Web3 Setup ============
def get_web3(rpc_url: str) -> Web3:
    """إنشاء اتصال Web3"""
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 5}))
    if geth_poa_middleware is not None:
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

# ============ Cached Functions ============
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

def get_cached_contract(w3: Web3, address: str, abi: list):
    """الحصول على نسخة العقد من cache"""
    key = f"{address}_{id(w3)}"
    now = time.time()
    if key in _contract_cache:
        contract, timestamp = _contract_cache[key]
        if now - timestamp < 60:
            return contract
    contract = w3.eth.contract(address=address, abi=abi)
    _contract_cache[key] = (contract, now)
    return contract

# ============ Helper Functions ============
def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    """جلب رصيد المحفظة"""
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception:
        return 0.0

def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000) -> float:
    """تقدير رسوم الغاز"""
    try:
        gas_price_wei = get_cached_gas_price(w3)
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception:
        return float("inf")

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
    except Exception:
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

def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> Optional[int]:
    """جلب السعر من onchain مع cache"""
    try:
        key = f"{nft_contract}_{id(w3)}"
        now = time.time()
        if key in _price_cache:
            price, timestamp = _price_cache[key]
            if now - timestamp < 10:
                return price
        
        seadrop = get_cached_contract(w3, SEADROP_ADDRESS, SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        price = int(public_drop[0])
        _price_cache[key] = (price, now)
        return price
    except Exception:
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
    """محاولة الشراء بمحفظة واحدة محددة"""
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
            return {"success": False, "wallet": checksum_wallet, "reason": "simulation_failed", "error": str(e)}

        # 7. التحقق من رسوم الغاز
        gas_price = get_cached_gas_price(w3)
        actual_gas_fee_usd = (tx["gas"] * gas_price / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high", "gas_fee_usd": actual_gas_fee_usd}

        # 8. التحقق من الرصيد
        total_cost_wei = total_value + (tx["gas"] * gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds_for_total_cost"}

        # 9. توقيع وإرسال المعاملة
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
