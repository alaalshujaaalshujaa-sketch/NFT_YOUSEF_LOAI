"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
نسخة فائقة السرعة - استخدام قيمة ثابتة للغاز.
"""

import asyncio
import logging
import time
from web3 import Web3
from web3.exceptions import ContractLogicError
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

# ✅ قيمة ثابتة للغاز (أسرع من التقدير)
FIXED_GAS_LIMIT = 300000
MAX_RETRY_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 1

wallet_locks = {}

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

def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة: {e}")
        return 0.0

def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float) -> float:
    try:
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * FIXED_GAS_LIMIT) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"[الغاز] تعذر التقدير: {e}")
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
    except Exception as e:
        log.warning(f"[سعر on-chain] تعذر القراءة: {e}")
        return None

def analyze_error(error: Exception) -> dict:
    error_str = str(error).lower()
    
    reasons = {
        "insufficient_funds": ["insufficient funds", "insufficient balance"],
        "sold_out": ["sold out", "max supply"],
        "already_minted": ["already minted", "already claimed"],
    }
    
    for reason, keywords in reasons.items():
        for keyword in keywords:
            if keyword in error_str:
                return {"reason": reason}
    
    return {"reason": "unknown"}

def attempt_purchase_single_wallet(
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
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address"}

    # فحص الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low"}

    # تقدير الغاز
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high"}

    # الحصول على عنوان الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        fee_recipient = ZERO_ADDRESS

    # تحديد الكمية
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity

    # بناء المعاملة مع غاز ثابت
    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
        
        tx = contract.functions.mintPublic(
            checksum_contract,
            fee_recipient,
            ZERO_ADDRESS,
            quantity,
        ).build_transaction({
            "from": checksum_wallet,
            "value": total_value,
            "nonce": nonce,
            "chainId": w3.eth.chain_id,
            "gas": FIXED_GAS_LIMIT,  # ✅ قيمة ثابتة
        })
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "build_tx_failed"}

    # التحقق من التكلفة
    try:
        gas_price = w3.eth.gas_price
        actual_gas_fee_usd = (FIXED_GAS_LIMIT * gas_price / 1e18) * eth_price_usd
        
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high"}
        
        total_cost_wei = total_value + (FIXED_GAS_LIMIT * gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        
        if wallet_balance_wei < total_cost_wei:
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds"}
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "pre_send_failed"}

    # إرسال المعاملة
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            
            log.info(f"✅ شراء ناجح: {tx_hash.hex()[:8]}...")
            return {
                "success": True,
                "wallet": checksum_wallet,
                "tx_hash": tx_hash.hex(),
                "quantity": quantity,
                "gas_fee_usd": actual_gas_fee_usd,
                "total_value_wei": total_value,
            }
            
        except ContractLogicError as e:
            error_analysis = analyze_error(e)
            if error_analysis['reason'] in ['sold_out', 'already_minted']:
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis['reason']}
            
            if attempt == MAX_RETRY_ATTEMPTS - 1:
                return {"success": False, "wallet": checksum_wallet, "reason": "contract_error"}
            
            time.sleep(RETRY_DELAY_SECONDS)
            
        except Exception as e:
            if attempt == MAX_RETRY_ATTEMPTS - 1:
                return {"success": False, "wallet": checksum_wallet, "reason": "tx_error"}
            
            time.sleep(RETRY_DELAY_SECONDS)
    
    return {"success": False, "wallet": checksum_wallet, "reason": "max_retries"}
