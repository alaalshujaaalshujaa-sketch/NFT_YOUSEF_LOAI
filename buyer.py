"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop - نسخة فائقة السرعة
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple
from web3 import Web3
from web3.exceptions import ContractLogicError
from web3.middleware import ExtraDataToPOAMiddleware

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

# ==================== إعدادات السرعة ====================
MIN_BALANCE_RESERVE_USD = 0.05  # تم التخفيض
FEW_THRESHOLD = 10  # تم التخفيض
LIMITED_BUY_QTY = 20  # تم الزيادة
GAS_LIMIT_SAFETY_MARGIN = 1.1  # تم التخفيض
MAX_RETRY_ATTEMPTS = 2  # تم التخفيض
RETRY_DELAY_SECONDS = 1  # تم التخفيض

# ==================== المتغيرات العامة ====================
wallet_locks = {}
_eth_price_cache = {"value": None, "ts": 0}
_ETH_PRICE_CACHE_DURATION = 30  # تم التخفيض

def get_wallet_lock(wallet_address: str) -> asyncio.Lock:
    addr = wallet_address.lower()
    if addr not in wallet_locks:
        wallet_locks[addr] = asyncio.Lock()
    return wallet_locks[addr]

def get_web3(rpc_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    try:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    except (ValueError, TypeError):
        pass
    return w3

# ==================== جلب الأسعار السريع ====================
async def get_eth_price_usd_async() -> float:
    """جلب سعر ETH بشكل غير متزامن مع تخزين مؤقت"""
    import aiohttp
    
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < _ETH_PRICE_CACHE_DURATION):
        return _eth_price_cache["value"]
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
                timeout=aiohttp.ClientTimeout(total=3)
            ) as resp:
                data = await resp.json()
                price = data["ethereum"]["usd"]
                _eth_price_cache["value"] = price
                _eth_price_cache["ts"] = now
                return price
    except Exception as e:
        log.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0

def get_eth_price_usd_sync() -> float:
    """نسخة متزامنة لجلب السعر"""
    import requests
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < _ETH_PRICE_CACHE_DURATION):
        return _eth_price_cache["value"]
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=3
        )
        price = resp.json()["ethereum"]["usd"]
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except:
        return _eth_price_cache["value"] or 3000.0

# ==================== فحص الرصيد المتوازي ====================
async def get_wallet_balance_async(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    """فحص رصيد المحفظة بشكل غير متزامن"""
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = await asyncio.to_thread(w3.eth.get_balance, checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة للمحفظة {wallet_address[:8]}...: {e}")
        return 0.0

async def check_wallets_balance_parallel(w3: Web3, wallets: List[str], eth_price_usd: float) -> Dict[str, float]:
    """فحص رصيد جميع المحافظ بالتوازي"""
    tasks = [get_wallet_balance_async(w3, wallet, eth_price_usd) for wallet in wallets]
    results = await asyncio.gather(*tasks)
    return {wallet: balance for wallet, balance in zip(wallets, results)}

# ==================== الوظائف الأساسية ====================
def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 100_000) -> float:
    """تقدير رسوم الغاز بشكل أسرع"""
    try:
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception:
        return float("inf")

def get_fee_recipient(w3: Web3, nft_contract: str):
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        if not recipients:
            return None
        return Web3.to_checksum_address(recipients[0])
    except Exception as e:
        log.error(f"[عنوان الرسوم] خطأ: {e}")
        return None

def decide_quantity(max_per_wallet, remaining_supply: int) -> int:
    """تحديد الكمية المثلى للشراء"""
    if max_per_wallet is None:
        qty = 5
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    return max(1, min(qty, remaining_supply))

def get_onchain_public_price_wei(w3: Web3, nft_contract: str):
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return int(public_drop[0])
    except Exception:
        return None

def analyze_error(error: Exception) -> dict:
    error_str = str(error).lower()
    
    reasons = {
        "insufficient_funds": ["insufficient funds", "insufficient balance", "not enough funds"],
        "gas_issue": ["gas", "out of gas", "gas limit"],
        "nonce_issue": ["nonce", "transaction with the same nonce"],
        "contract_reverted": ["execution reverted", "revert", "vm exception"],
        "sold_out": ["sold out", "max supply", "no tokens left"],
        "already_minted": ["already minted", "already claimed", "max per wallet"],
    }
    
    for reason, keywords in reasons.items():
        for keyword in keywords:
            if keyword in error_str:
                return {"reason": reason, "message": error_str}
    
    return {"reason": "unknown_error", "message": error_str}

# ==================== عملية الشراء الأساسية ====================
async def attempt_purchase_single_wallet(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet,
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> dict:
    """محاولة شراء NFT من محفظة واحدة - نسخة محسنة"""
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address", "error": str(e)}

    # فحص الرصيد
    balance_usd = await get_wallet_balance_async(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low", "balance_usd": balance_usd}

    # تقدير الغاز
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high", "gas_fee_usd": gas_fee_usd}

    # جلب مستلم الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        fee_recipient = ZERO_ADDRESS

    # تحديد الكمية
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity
    
    log.info(f"💰 شراء {quantity} من {checksum_contract[:8]}... للمحفظة {checksum_wallet[:8]}...")

    # بناء المعاملة
    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = await asyncio.to_thread(w3.eth.get_transaction_count, checksum_wallet, "pending")
        
        tx = contract.functions.mintPublic(
            checksum_contract,
            fee_recipient,
            ZERO_ADDRESS,
            quantity,
        ).build_transaction({
            "from": checksum_wallet,
            "value": total_value,
            "nonce": nonce,
            "chainId": await asyncio.to_thread(w3.eth.chain_id),
        })
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "build_tx_failed", "error": str(e)}

    # تقدير الغاز مع هامش أمان
    try:
        estimated_gas = await asyncio.to_thread(w3.eth.estimate_gas, tx)
        tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
    except ContractLogicError as e:
        error_analysis = analyze_error(e)
        return {"success": False, "wallet": checksum_wallet, "reason": f"contract_reverted_{error_analysis['reason']}", "error": str(e)}
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_estimation_failed", "error": str(e)}

    # التحقق من التكلفة النهائية
    try:
        gas_price = await asyncio.to_thread(w3.eth.gas_price)
        actual_gas_fee_usd = (tx["gas"] * gas_price / 1e18) * eth_price_usd
        
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high_actual", "gas_fee_usd": actual_gas_fee_usd}
        
        total_cost_wei = total_value + (tx["gas"] * gas_price)
        wallet_balance_wei = await asyncio.to_thread(w3.eth.get_balance, checksum_wallet)
        
        if wallet_balance_wei < total_cost_wei:
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds_for_total_cost"}
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "pre_send_check_failed", "error": str(e)}

    # إرسال المعاملة مع إعادة المحاولة
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            signed = await asyncio.to_thread(w3.eth.account.sign_transaction, tx, private_key=private_key)
            tx_hash = await asyncio.to_thread(w3.eth.send_raw_transaction, signed.raw_transaction)
            
            log.info(f"✅ شراء ناجح للمحفظة {checksum_wallet[:8]}...: {tx_hash.hex()} — كمية: {quantity}")
            return {
                "success": True,
                "wallet": checksum_wallet,
                "tx_hash": tx_hash.hex(),
                "quantity": quantity,
                "gas_fee_usd": actual_gas_fee_usd,
                "total_value_wei": total_value,
                "attempt": attempt + 1
            }
            
        except ContractLogicError as e:
            error_analysis = analyze_error(e)
            if error_analysis['reason'] in ['sold_out', 'already_minted']:
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis['reason'], "error": str(e)}
            if attempt == MAX_RETRY_ATTEMPTS - 1:
                return {"success": False, "wallet": checksum_wallet, "reason": f"contract_reverted_{error_analysis['reason']}", "error": str(e)}
            await asyncio.sleep(RETRY_DELAY_SECONDS * (attempt + 0.5))
            
        except Exception as e:
            error_analysis = analyze_error(e)
            if attempt == MAX_RETRY_ATTEMPTS - 1:
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis['reason'], "error": str(e)}
            await asyncio.sleep(RETRY_DELAY_SECONDS * (attempt + 0.5))
    
    return {"success": False, "wallet": checksum_wallet, "reason": "max_retries_exceeded"}
