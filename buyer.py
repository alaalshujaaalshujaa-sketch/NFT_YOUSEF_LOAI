"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
مع تحسينات متقدمة للأداء والاستقرار:
- تخزين مؤقت لسعر الغاز
- حدود دنيا وعليا لسعر الغاز
- إعادة محاولة لتقدير الغاز
- تخزين مؤقت لسعر NFT
- تحسين معالجة الأخطاء
"""

import asyncio
import logging
import time
from web3 import Web3

log = logging.getLogger("buyer")

# ============================================
# 🔥 الثوابت
# ============================================

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

# ============================================
# 🔥 إدارة سعر الغاز - محسّن بالكامل
# ============================================

# تخزين مؤقت لسعر الغاز
_GAS_CACHE = {"value": None, "timestamp": 0}
_GAS_CACHE_TTL = 5  # 5 ثواني

# حدود سعر الغاز
GAS_MIN_GWEI = 10   # 10 Gwei (حد أدنى)
GAS_MAX_GWEI = 100  # 100 Gwei (حد أقصى)
GAS_DEFAULT_GWEI = 30  # 30 Gwei (قيمة احتياطية)

def get_gas_price_cached(w3: Web3) -> int:
    """
    جلب سعر الغاز مع تخزين مؤقت وتطبيق حدود دنيا وعليا
    
    Args:
        w3: كائن Web3
    
    Returns:
        int: سعر الغاز بالـ Wei
    """
    
    # التحقق من الكاش
    if _GAS_CACHE["value"] and (time.time() - _GAS_CACHE["timestamp"] < _GAS_CACHE_TTL):
        return _GAS_CACHE["value"]
    
    try:
        # جلب سعر الغاز من الشبكة
        gas_price_wei = w3.eth.gas_price
        gas_price_gwei = gas_price_wei / 1e9
        
        # تطبيق الحدود الدنيا والعليا
        if gas_price_gwei < GAS_MIN_GWEI:
            log.debug(f"رفع سعر الغاز من {gas_price_gwei:.2f} إلى {GAS_MIN_GWEI} Gwei")
            gas_price_wei = int(GAS_MIN_GWEI * 10**9)
        elif gas_price_gwei > GAS_MAX_GWEI:
            log.warning(f"سعر الغاز مرتفع جداً: {gas_price_gwei:.2f} Gwei (الحد الأقصى: {GAS_MAX_GWEI})")
            gas_price_wei = int(GAS_MAX_GWEI * 10**9)
        
        # تخزين في الكاش
        _GAS_CACHE["value"] = gas_price_wei
        _GAS_CACHE["timestamp"] = time.time()
        
        log.debug(f"💰 سعر الغاز: {gas_price_wei/1e9:.2f} Gwei")
        return gas_price_wei
        
    except Exception as e:
        log.warning(f"خطأ في جلب سعر الغاز: {e}")
        
        # استخدام القيمة المخبأة إذا كانت موجودة
        if _GAS_CACHE["value"]:
            log.debug(f"⚠️ استخدام سعر غاز مخبأ: {_GAS_CACHE['value']/1e9:.2f} Gwei")
            return _GAS_CACHE["value"]
        
        # القيمة الاحتياطية
        log.warning(f"⚠️ استخدام قيمة احتياطية: {GAS_DEFAULT_GWEI} Gwei")
        return int(GAS_DEFAULT_GWEI * 10**9)

def estimate_gas_with_retry(
    w3: Web3, 
    tx: dict, 
    max_retries: int = 3,
    multiplier: float = 1.2
) -> int:
    """
    تقدير الغاز مع إعادة محاولة وتحديث تلقائي لسعر الغاز
    
    Args:
        w3: كائن Web3
        tx: معاملة البناء
        max_retries: عدد المحاولات القصوى
        multiplier: مضاعف هامش الأمان
    
    Returns:
        int: كمية الغاز المقدرة مع هامش أمان
    """
    
    for attempt in range(max_retries):
        try:
            # تحديث سعر الغاز
            gas_price = get_gas_price_cached(w3)
            tx["gasPrice"] = gas_price
            
            # محاولة تقدير الغاز
            estimated = w3.eth.estimate_gas(tx)
            
            # إضافة هامش أمان
            gas_with_margin = int(estimated * multiplier)
            
            log.debug(f"تقدير الغاز: {estimated} → {gas_with_margin} (هامش: {(multiplier-1)*100:.0f}%)")
            return gas_with_margin
            
        except Exception as e:
            if attempt == max_retries - 1:
                log.warning(f"فشل تقدير الغاز بعد {max_retries} محاولات: {e}")
                return 200_000  # قيمة احتياطية
            
            # تأخير قبل إعادة المحاولة
            wait_time = 0.5 * (attempt + 1)
            log.debug(f"محاولة تقدير الغاز {attempt + 1} فشلت، إعادة بعد {wait_time:.1f}s")
            time.sleep(wait_time)
            
            # زيادة سعر الغاز تدريجياً للمحاولة التالية
            current_gas = tx.get("gasPrice", GAS_DEFAULT_GWEI * 10**9)
            tx["gasPrice"] = int(current_gas * 1.1)  # زيادة 10%
    
    return 200_000  # قيمة احتياطية

def get_safe_gas_price(w3: Web3, priority: str = "normal") -> int:
    """
    جلب سعر غاز آمن حسب الأولوية
    
    Args:
        w3: كائن Web3
        priority: 'slow', 'normal', 'fast'
    
    Returns:
        int: سعر الغاز بالـ Wei
    """
    
    base_price = get_gas_price_cached(w3)
    
    if priority == "fast":
        return int(base_price * 1.3)
    elif priority == "slow":
        return int(base_price * 0.8)
    else:  # normal
        return base_price

def estimate_transaction_cost(
    w3: Web3,
    tx: dict,
    eth_price_usd: float
) -> dict:
    """
    تقدير التكلفة الكاملة للمعاملة
    
    Args:
        w3: كائن Web3
        tx: معاملة البناء
        eth_price_usd: سعر ETH بالدولار
    
    Returns:
        dict: تفاصيل التكلفة
    """
    
    # جلب سعر الغاز
    gas_price = get_gas_price_cached(w3)
    
    # تقدير الغاز
    try:
        gas_used = w3.eth.estimate_gas(tx)
        gas_used = int(gas_used * GAS_LIMIT_SAFETY_MARGIN)
    except:
        gas_used = 200_000
    
    # حساب التكلفة
    gas_cost_eth = (gas_price * gas_used) / 1e18
    gas_cost_usd = gas_cost_eth * eth_price_usd
    
    # حساب التكلفة الإجمالية (مع قيمة المعاملة)
    total_value_eth = tx.get("value", 0) / 1e18
    total_cost_eth = total_value_eth + gas_cost_eth
    total_cost_usd = total_cost_eth * eth_price_usd
    
    return {
        "gas_price_gwei": gas_price / 1e9,
        "gas_used": gas_used,
        "gas_cost_eth": gas_cost_eth,
        "gas_cost_usd": gas_cost_usd,
        "total_cost_eth": total_cost_eth,
        "total_cost_usd": total_cost_usd
    }

# ============================================
# 🔥 تخزين مؤقت لسعر NFT (on-chain price)
# ============================================

_PRICE_CACHE = {}
_PRICE_CACHE_TTL = 10  # 10 ثواني

def get_cached_price(contract_address: str) -> int | None:
    """جلب سعر NFT من الكاش"""
    key = contract_address.lower()
    if key in _PRICE_CACHE:
        price, timestamp = _PRICE_CACHE[key]
        if time.time() - timestamp < _PRICE_CACHE_TTL:
            return price
        del _PRICE_CACHE[key]
    return None

def set_cached_price(contract_address: str, price: int):
    """تخزين سعر NFT في الكاش"""
    _PRICE_CACHE[contract_address.lower()] = (price, time.time())

# ============================================
# 🔥 إعادة محاولة للـ RPC Calls
# ============================================

def rpc_call_with_retry(func, *args, max_retries=3, delay=1, **kwargs):
    """
    تنفيذ استدعاء RPC مع إعادة محاولة وتأخير متزايد
    
    Args:
        func: الدالة المراد تنفيذها
        *args: وسائط الدالة
        max_retries: عدد المحاولات القصوى
        delay: التأخير الأولي بين المحاولات
        **kwargs: وسائط الدالة المسماة
    
    Returns:
        نتيجة الدالة أو None في حالة الفشل
    """
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                log.debug(f"فشل RPC بعد {max_retries} محاولات: {e}")
                return None
            wait_time = delay * (attempt + 1)
            log.debug(f"محاولة RPC {attempt + 1} فشلت، إعادة بعد {wait_time:.1f}s")
            time.sleep(wait_time)
    return None

# ============================================
# 🔥 قفل المحافظ (لمنع تضارب Nonce)
# ============================================

wallet_locks = {}

def get_wallet_lock(wallet_address: str) -> asyncio.Lock:
    """الحصول على قفل للمحفظة لتجنب تضارب Nonce"""
    addr = wallet_address.lower()
    if addr not in wallet_locks:
        wallet_locks[addr] = asyncio.Lock()
    return wallet_locks[addr]

# ============================================
# 🔥 الدوال الأساسية
# ============================================

def get_web3(rpc_url: str) -> Web3:
    """إنشاء كائن Web3"""
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

def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000) -> float:
    """تقدير رسوم الغاز بالدولار باستخدام سعر الغاز المحسّن"""
    try:
        gas_price_wei = get_gas_price_cached(w3)
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"[الغاز] تعذر التقدير: {e}")
        return float("inf")

def get_fee_recipient(w3: Web3, nft_contract: str) -> str | None:
    """جلب عنوان مستلم الرسوم من العقد"""
    try:
        def _call():
            seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
            recipients = seadrop.functions.getAllowedFeeRecipients(
                Web3.to_checksum_address(nft_contract)
            ).call()
            if not recipients:
                return None
            return Web3.to_checksum_address(recipients[0])
        
        return rpc_call_with_retry(_call, max_retries=3, delay=1)
    except Exception as e:
        log.error(f"[عنوان الرسوم] خطأ استعلام: {e}")
        return None

def decide_quantity(max_per_wallet: int | None, remaining_supply: int) -> int:
    """
    تحديد الكمية المناسبة للشراء
    
    Args:
        max_per_wallet: الحد الأقصى لكل محفظة
        remaining_supply: الكمية المتبقية
    
    Returns:
        int: الكمية المقرر شراؤها
    """
    if remaining_supply <= 0:
        log.warning(f"⚠️ لا توجد كمية متبقية للشراء (remaining: {remaining_supply})")
        return 0
    
    if max_per_wallet is None:
        qty = 5
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    
    final_qty = max(1, min(qty, remaining_supply))
    
    if final_qty != qty:
        log.debug(f"تعديل الكمية من {qty} إلى {final_qty} (المتبقي: {remaining_supply})")
    
    return final_qty

def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> int | None:
    """
    جلب السعر من العقد مع تخزين مؤقت
    
    Args:
        w3: كائن Web3
        nft_contract: عنوان عقد NFT
    
    Returns:
        int | None: السعر بالـ Wei أو None في حالة الفشل
    """
    
    # التحقق من الكاش
    cached = get_cached_price(nft_contract)
    if cached is not None:
        log.debug(f"✅ سعر NFT من الكاش: {cached} Wei")
        return cached
    
    try:
        def _call():
            seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
            public_drop = seadrop.functions.getPublicDrop(
                Web3.to_checksum_address(nft_contract)
            ).call()
            return int(public_drop[0])
        
        price = rpc_call_with_retry(_call, max_retries=3, delay=1)
        
        if price is not None:
            set_cached_price(nft_contract, price)
            log.debug(f"💰 سعر NFT من العقد: {price} Wei")
        else:
            log.warning(f"[سعر on-chain] تعذر القراءة: {nft_contract[:8]}")
        
        return price
        
    except Exception as e:
        log.warning(f"[سعر on-chain] تعذر القراءة: {e}")
        return None

def check_balance_sufficient(w3: Web3, wallet_address: str, total_cost_wei: int) -> tuple[bool, str]:
    """
    التحقق من كفاية الرصيد مع احتياطي للغاز
    
    Args:
        w3: كائن Web3
        wallet_address: عنوان المحفظة
        total_cost_wei: التكلفة الإجمالية بالـ Wei
    
    Returns:
        tuple[bool, str]: (كافي, رسالة)
    """
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        
        # احتياطي للغاز (50k gas)
        gas_price = get_gas_price_cached(w3)
        reserve_wei = 50_000 * gas_price
        
        required = total_cost_wei + reserve_wei
        
        if balance_wei < required:
            balance_eth = balance_wei / 1e18
            required_eth = required / 1e18
            return False, f"الرصيد {balance_eth:.6f} ETH < المطلوب {required_eth:.6f} ETH"
        
        return True, f"الرصيد كافٍ: {balance_wei/1e18:.6f} ETH"
        
    except Exception as e:
        return False, f"خطأ في قراءة الرصيد: {e}"

def handle_purchase_error(e: Exception, wallet: str) -> dict:
    """
    معالجة أخطاء الشراء مع رسائل مفهومة
    
    Args:
        e: الاستثناء
        wallet: عنوان المحفظة
    
    Returns:
        dict: نتيجة مع رسالة خطأ مفهومة
    """
    error_str = str(e).lower()
    
    if "insufficient funds" in error_str or "insufficient balance" in error_str:
        return {
            "success": False, 
            "wallet": wallet, 
            "reason": "insufficient_funds",
            "error": "الرصيد غير كافٍ للشراء"
        }
    elif "gas" in error_str and ("too low" in error_str or "below" in error_str):
        return {
            "success": False, 
            "wallet": wallet, 
            "reason": "gas_too_low",
            "error": "سعر الغاز منخفض جداً"
        }
    elif "nonce" in error_str:
        return {
            "success": False, 
            "wallet": wallet, 
            "reason": "nonce_error",
            "error": "خطأ في Nonce، حاول مرة أخرى"
        }
    elif "timeout" in error_str or "timed out" in error_str:
        return {
            "success": False, 
            "wallet": wallet, 
            "reason": "timeout",
            "error": "انتهت مهلة الاتصال"
        }
    elif "execution reverted" in error_str:
        return {
            "success": False, 
            "wallet": wallet, 
            "reason": "execution_reverted",
            "error": "فشل تنفيذ العقد (ربما انتهى المينت)"
        }
    else:
        return {
            "success": False, 
            "wallet": wallet, 
            "reason": "unknown_error",
            "error": str(e)[:100]
        }

# ============================================
# 🔥 دالة الشراء الرئيسية - المحسّنة بالكامل
# ============================================

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
    """
    محاولة الشراء بمحفظة واحدة محددة مع جميع التحسينات
    
    Args:
        w3: كائن Web3
        private_key: المفتاح الخاص للمحفظة
        wallet_address: عنوان المحفظة
        nft_contract: عنوان عقد NFT
        price_wei_per_token: السعر لكل NFT بالـ Wei
        max_per_wallet: الحد الأقصى لكل محفظة
        remaining_supply: الكمية المتبقية
        eth_price_usd: سعر ETH بالدولار
        max_gas_fee_usd: الحد الأقصى لرسوم الغاز بالدولار
    
    Returns:
        dict: نتيجة العملية
    """
    
    # ============================================
    # 1. التحقق من صحة العناوين
    # ============================================
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {
            "success": False, 
            "wallet": wallet_address, 
            "reason": "invalid_address", 
            "error": str(e)
        }

    # ============================================
    # 2. التحقق من الرصيد
    # ============================================
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {
            "success": False, 
            "wallet": checksum_wallet, 
            "reason": "balance_too_low", 
            "balance_usd": balance_usd
        }

    # ============================================
    # 3. تقدير رسوم الغاز
    # ============================================
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        return {
            "success": False, 
            "wallet": checksum_wallet, 
            "reason": "gas_too_high", 
            "gas_fee_usd": gas_fee_usd
        }

    # ============================================
    # 4. جلب عنوان مستلم الرسوم
    # ============================================
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        return {
            "success": False, 
            "wallet": checksum_wallet, 
            "reason": "no_fee_recipient"
        }

    # ============================================
    # 5. تحديد الكمية
    # ============================================
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    if quantity <= 0:
        return {
            "success": False, 
            "wallet": checksum_wallet, 
            "reason": "no_quantity", 
            "remaining": remaining_supply
        }

    total_value = price_wei_per_token * quantity

    # ============================================
    # 6. بناء المعاملة وتقدير الغاز
    # ============================================
    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")

        # بناء المعاملة
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

        # تقدير الغاز مع إعادة محاولة
        estimated_gas = estimate_gas_with_retry(w3, tx, max_retries=3, multiplier=GAS_LIMIT_SAFETY_MARGIN)
        tx["gas"] = estimated_gas

        # ============================================
        # 7. التحقق من رسوم الغاز بعد التقدير
        # ============================================
        gas_price = get_gas_price_cached(w3)
        actual_gas_fee_usd = (tx["gas"] * gas_price / 1e18) * eth_price_usd
        
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {
                "success": False, 
                "wallet": checksum_wallet, 
                "reason": "gas_too_high", 
                "gas_fee_usd": actual_gas_fee_usd
            }

        # ============================================
        # 8. التحقق النهائي من الرصيد
        # ============================================
        total_cost_wei = total_value + (tx["gas"] * gas_price)
        is_sufficient, msg = check_balance_sufficient(w3, checksum_wallet, total_cost_wei)
        if not is_sufficient:
            return {
                "success": False, 
                "wallet": checksum_wallet, 
                "reason": "insufficient_funds", 
                "detail": msg
            }

        # ============================================
        # 9. توقيع وإرسال المعاملة
        # ============================================
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)

        log.info(f"[شراء ناجح - {checksum_wallet[:8]}] {tx_hash.hex()[:16]}... كمية: {quantity}")
        
        return {
            "success": True,
            "wallet": checksum_wallet,
            "tx_hash": tx_hash.hex(),
            "quantity": quantity,
            "gas_fee_usd": actual_gas_fee_usd,
            "total_value_wei": total_value,
            "gas_price_gwei": gas_price / 1e9,
            "gas_used": tx["gas"],
        }

    except Exception as e:
        log.error(f"[خطأ إرسال للمحفظة {checksum_wallet[:8]}] {e}")
        return handle_purchase_error(e, checksum_wallet)
