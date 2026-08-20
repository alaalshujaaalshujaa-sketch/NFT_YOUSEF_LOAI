"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
يدعم EIP-1559، التجميع، إعادة المحاولة الذكية، وإدارة الأخطاء المتقدمة.
"""

import asyncio
import json
import logging
import time
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field
from enum import Enum
from decimal import Decimal

from web3 import Web3
from web3.exceptions import TransactionNotFound, ContractLogicError, TimeExhausted

# ✅ إصلاح استيراد geth_poa_middleware للإصدارات المختلفة
try:
    from web3.middleware import geth_poa_middleware
except ImportError:
    # web3 v6+ 
    from web3.middleware.geth_poa import geth_poa_middleware

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

# ==================== إعدادات متقدمة ====================
MIN_BALANCE_RESERVE_USD = 0.10
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 15
GAS_LIMIT_SAFETY_MARGIN = 1.2
MAX_RETRY_DELAY = 10
RETRY_BACKOFF_FACTOR = 1.5
MAX_PENDING_TX = 3
GAS_PRICE_MULTIPLIER = 1.1

# ==================== استراتيجيات الشراء ====================
class PurchaseStrategy(Enum):
    AGGRESSIVE = "aggressive"
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"

STRATEGY_CONFIGS = {
    PurchaseStrategy.AGGRESSIVE: {
        "max_gas_multiplier": 2.0,
        "retry_delay": 1,
        "max_retries": 5,
        "priority_fee_multiplier": 1.5,
    },
    PurchaseStrategy.CONSERVATIVE: {
        "max_gas_multiplier": 1.2,
        "retry_delay": 5,
        "max_retries": 2,
        "priority_fee_multiplier": 1.0,
    },
    PurchaseStrategy.BALANCED: {
        "max_gas_multiplier": 1.5,
        "retry_delay": 3,
        "max_retries": 3,
        "priority_fee_multiplier": 1.2,
    },
}

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
    strategy: PurchaseStrategy = PurchaseStrategy.BALANCED
    stats: Dict[str, Any] = field(default_factory=lambda: {
        "total_attempts": 0,
        "successful": 0,
        "failed": 0,
        "total_gas_spent": 0.0,
        "last_purchase_time": None,
    })
    pending_tx_count: int = 0

@dataclass
class PurchaseResult:
    """نتيجة عملية الشراء"""
    success: bool
    wallet: str
    reason: str = ""
    tx_hash: str = ""
    quantity: int = 0
    gas_fee_usd: float = 0.0
    total_value_wei: int = 0
    error: str = ""
    timestamp: float = field(default_factory=time.time)

# ==================== إدارة الأقفال ====================
class WalletLockManager:
    """مدير أقفال المحافظ"""
    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._lock_creation: asyncio.Lock = asyncio.Lock()
    
    async def get_lock(self, wallet_address: str) -> asyncio.Lock:
        addr = wallet_address.lower()
        async with self._lock_creation:
            if addr not in self._locks:
                self._locks[addr] = asyncio.Lock()
            return self._locks[addr]

lock_manager = WalletLockManager()

# ==================== إدارة المعاملات المعلقة ====================
class PendingTxManager:
    """مدير المعاملات المعلقة"""
    def __init__(self):
        self._pending: Dict[str, List[str]] = {}
    
    async def add_tx(self, wallet: str, tx_hash: str):
        addr = wallet.lower()
        if addr not in self._pending:
            self._pending[addr] = []
        self._pending[addr].append(tx_hash)
    
    async def remove_tx(self, wallet: str, tx_hash: str):
        addr = wallet.lower()
        if addr in self._pending and tx_hash in self._pending[addr]:
            self._pending[addr].remove(tx_hash)
    
    async def get_pending_count(self, wallet: str) -> int:
        addr = wallet.lower()
        return len(self._pending.get(addr, []))

pending_manager = PendingTxManager()

# ==================== دوال Web3 المحسنة ====================
def get_web3(rpc_url: str) -> Web3:
    """إنشاء اتصال Web3 مع middleware"""
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 30}))
    
    # ✅ إضافة middleware لسلاسل PoA
    try:
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception as e:
        log.warning(f"⚠️ تعذر إضافة PoA middleware: {e}")
    
    if not w3.is_connected():
        raise ConnectionError(f"❌ تعذر الاتصال بـ {rpc_url}")
    
    try:
        latest_block = w3.eth.get_block('latest')
        if 'baseFeePerGas' in latest_block:
            log.info(f"✅ السلسلة تدعم EIP-1559")
        else:
            log.warning(f"⚠️ السلسلة لا تدعم EIP-1559، سيتم استخدام Legacy")
    except:
        pass
    
    return w3

async def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = await asyncio.to_thread(w3.eth.get_balance, checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"❌ [الرصيد] تعذر القراءة للمحفظة {wallet_address[:8]}...: {e}")
        return 0.0

async def estimate_gas_fee_usd(
    w3: Web3, 
    eth_price_usd: float, 
    gas_units: int = 150_000,
    priority_fee_multiplier: float = 1.0
) -> float:
    try:
        latest_block = await asyncio.to_thread(w3.eth.get_block, 'pending')
        
        if 'baseFeePerGas' in latest_block:
            base_fee = latest_block['baseFeePerGas']
            priority_fee = await asyncio.to_thread(w3.eth.max_priority_fee)
            priority_fee = int(priority_fee * priority_fee_multiplier)
            total_fee_per_gas = base_fee + priority_fee
        else:
            gas_price = await asyncio.to_thread(w3.eth.gas_price)
            total_fee_per_gas = int(gas_price * GAS_PRICE_MULTIPLIER)
        
        fee_eth = (total_fee_per_gas * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"⚠️ [الغاز] تعذر التقدير: {e}")
        return float("inf")

async def get_optimal_gas_params(
    w3: Web3,
    priority_fee_multiplier: float = 1.0,
    max_gas_multiplier: float = 1.5
) -> Dict[str, Any]:
    try:
        latest_block = await asyncio.to_thread(w3.eth.get_block, 'pending')
        
        if 'baseFeePerGas' in latest_block:
            base_fee = latest_block['baseFeePerGas']
            priority_fee = await asyncio.to_thread(w3.eth.max_priority_fee)
            priority_fee = int(priority_fee * priority_fee_multiplier)
            max_fee = int(base_fee * max_gas_multiplier + priority_fee)
            
            return {
                'type': 2,
                'maxFeePerGas': max_fee,
                'maxPriorityFeePerGas': priority_fee,
            }
        else:
            gas_price = await asyncio.to_thread(w3.eth.gas_price)
            return {
                'type': 0,
                'gasPrice': int(gas_price * GAS_PRICE_MULTIPLIER),
            }
    except Exception as e:
        log.warning(f"⚠️ تعذر الحصول على معاملات الغاز: {e}")
        gas_price = await asyncio.to_thread(w3.eth.gas_price)
        return {'type': 0, 'gasPrice': gas_price}

async def get_fee_recipient(w3: Web3, nft_contract: str) -> Optional[str]:
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = await asyncio.to_thread(
            seadrop.functions.getAllowedFeeRecipients(
                Web3.to_checksum_address(nft_contract)
            ).call
        )
        if not recipients:
            return None
        return Web3.to_checksum_address(recipients[0])
    except Exception as e:
        log.error(f"❌ [عنوان الرسوم] خطأ استعلام: {e}")
        return None

async def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> Optional[int]:
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = await asyncio.to_thread(
            seadrop.functions.getPublicDrop(
                Web3.to_checksum_address(nft_contract)
            ).call
        )
        return int(public_drop[0])
    except Exception as e:
        log.warning(f"⚠️ [سعر on-chain] تعذر القراءة: {e}")
        return None

def decide_quantity(max_per_wallet: Optional[int], remaining_supply: int, strategy: PurchaseStrategy) -> int:
    if max_per_wallet is None:
        base_qty = 5
    elif max_per_wallet <= FEW_THRESHOLD:
        base_qty = max_per_wallet
    else:
        base_qty = LIMITED_BUY_QTY
    
    if strategy == PurchaseStrategy.AGGRESSIVE:
        qty = base_qty
    elif strategy == PurchaseStrategy.CONSERVATIVE:
        qty = max(1, base_qty // 2)
    else:
        qty = max(1, int(base_qty * 0.75))
    
    return max(1, min(qty, remaining_supply))

# ==================== الشراء المحسن ====================
async def send_transaction_with_retry(
    w3: Web3,
    wallet_data: WalletData,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: Optional[int],
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> PurchaseResult:
    strategy_config = STRATEGY_CONFIGS[wallet_data.strategy]
    max_retries = strategy_config["max_retries"]
    
    for attempt in range(max_retries):
        try:
            wallet_data.stats["total_attempts"] += 1
            
            result = await _attempt_purchase(
                w3=w3,
                wallet_data=wallet_data,
                nft_contract=nft_contract,
                price_wei_per_token=price_wei_per_token,
                max_per_wallet=max_per_wallet,
                remaining_supply=remaining_supply,
                eth_price_usd=eth_price_usd,
                max_gas_fee_usd=max_gas_fee_usd,
                strategy_config=strategy_config,
            )
            
            if result.success:
                wallet_data.stats["successful"] += 1
                wallet_data.stats["last_purchase_time"] = time.time()
                wallet_data.stats["total_gas_spent"] += result.gas_fee_usd
                return result
            else:
                wallet_data.stats["failed"] += 1
                
                if result.reason in ["timeout", "connection_error", "nonce_error", "underpriced"]:
                    if attempt < max_retries - 1:
                        delay = min(
                            strategy_config["retry_delay"] * (RETRY_BACKOFF_FACTOR ** attempt),
                            MAX_RETRY_DELAY
                        )
                        log.warning(
                            f"🔄 المحفظة {wallet_data.wallet[:8]} - محاولة {attempt + 1}/{max_retries} "
                            f"فشلت ({result.reason}). إعادة بعد {delay:.1f} ثوانٍ"
                        )
                        await asyncio.sleep(delay)
                        continue
                
                return result
                
        except Exception as e:
            log.error(f"❌ المحفظة {wallet_data.wallet[:8]} - خطأ غير متوقع: {e}")
            
            if attempt < max_retries - 1:
                delay = min(RETRY_BACKOFF_FACTOR ** attempt, MAX_RETRY_DELAY)
                await asyncio.sleep(delay)
                continue
            
            wallet_data.stats["failed"] += 1
            return PurchaseResult(
                success=False,
                wallet=wallet_data.wallet,
                reason="unexpected_error",
                error=str(e)
            )
    
    return PurchaseResult(
        success=False,
        wallet=wallet_data.wallet,
        reason="retry_exhausted"
    )

async def _attempt_purchase(
    w3: Web3,
    wallet_data: WalletData,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: Optional[int],
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
    strategy_config: Dict[str, Any],
) -> PurchaseResult:
    checksum_wallet = Web3.to_checksum_address(wallet_data.wallet)
    checksum_contract = Web3.to_checksum_address(nft_contract)
    
    pending_count = await pending_manager.get_pending_count(checksum_wallet)
    if pending_count >= MAX_PENDING_TX:
        return PurchaseResult(
            success=False,
            wallet=checksum_wallet,
            reason="too_many_pending_tx"
        )
    
    balance_usd = await get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return PurchaseResult(
            success=False,
            wallet=checksum_wallet,
            reason="balance_too_low",
            error=f"Balance: ${balance_usd:.2f}"
        )
    
    gas_fee_usd = await estimate_gas_fee_usd(
        w3, 
        eth_price_usd,
        priority_fee_multiplier=strategy_config["priority_fee_multiplier"]
    )
    if gas_fee_usd > max_gas_fee_usd:
        return PurchaseResult(
            success=False,
            wallet=checksum_wallet,
            reason="gas_too_high",
            gas_fee_usd=gas_fee_usd,
            error=f"Gas: ${gas_fee_usd:.4f} > Max: ${max_gas_fee_usd:.4f}"
        )
    
    fee_recipient = await get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        return PurchaseResult(
            success=False,
            wallet=checksum_wallet,
            reason="no_fee_recipient"
        )
    
    quantity = decide_quantity(max_per_wallet, remaining_supply, wallet_data.strategy)
    total_value = price_wei_per_token * quantity
    
    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = await asyncio.to_thread(
            w3.eth.get_transaction_count,
            checksum_wallet,
            "pending"
        )
        
        gas_params = await get_optimal_gas_params(
            w3,
            priority_fee_multiplier=strategy_config["priority_fee_multiplier"],
            max_gas_multiplier=strategy_config["max_gas_multiplier"]
        )
        
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
            **gas_params,
        })
        
        try:
            estimated_gas = await asyncio.to_thread(w3.eth.estimate_gas, tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
        except ContractLogicError as e:
            return PurchaseResult(
                success=False,
                wallet=checksum_wallet,
                reason="contract_reverted",
                error=str(e)
            )
        except Exception as e:
            return PurchaseResult(
                success=False,
                wallet=checksum_wallet,
                reason="estimation_failed",
                error=str(e)
            )
        
        if gas_params.get('type') == 2:
            gas_cost_wei = tx["gas"] * gas_params["maxFeePerGas"]
        else:
            gas_cost_wei = tx["gas"] * gas_params["gasPrice"]
        
        actual_gas_fee_usd = (gas_cost_wei / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            return PurchaseResult(
                success=False,
                wallet=checksum_wallet,
                reason="gas_too_high",
                gas_fee_usd=actual_gas_fee_usd,
                error=f"Actual gas: ${actual_gas_fee_usd:.4f}"
            )
        
        total_cost_wei = total_value + gas_cost_wei
        wallet_balance_wei = await asyncio.to_thread(w3.eth.get_balance, checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            return PurchaseResult(
                success=False,
                wallet=checksum_wallet,
                reason="insufficient_funds",
                error=f"Need: {total_cost_wei/1e18:.6f} ETH, Have: {wallet_balance_wei/1e18:.6f} ETH"
            )
        
        signed = w3.eth.account.sign_transaction(tx, private_key=wallet_data.private_key)
        tx_hash = await asyncio.to_thread(
            w3.eth.send_raw_transaction,
            signed.raw_transaction
        )
        
        await pending_manager.add_tx(checksum_wallet, tx_hash.hex())
        wallet_data.pending_tx_count += 1
        
        log.info(
            f"✅ [شراء - {checksum_wallet[:8]}] "
            f"TX: {tx_hash.hex()[:10]}... "
            f"Qty: {quantity} "
            f"Gas: ${actual_gas_fee_usd:.4f}"
        )
        
        try:
            receipt = await asyncio.wait_for(
                asyncio.to_thread(
                    w3.eth.wait_for_transaction_receipt,
                    tx_hash,
                    timeout=120
                ),
                timeout=130
            )
            
            await pending_manager.remove_tx(checksum_wallet, tx_hash.hex())
            wallet_data.pending_tx_count -= 1
            
            if receipt.status != 1:
                return PurchaseResult(
                    success=False,
                    wallet=checksum_wallet,
                    reason="transaction_failed",
                    tx_hash=tx_hash.hex(),
                    error="Transaction reverted"
                )
        except asyncio.TimeoutError:
            log.warning(f"⚠️ لم يتم تأكيد المعاملة {tx_hash.hex()[:10]}... خلال 120 ثانية")
            asyncio.create_task(_cleanup_pending_tx(w3, checksum_wallet, tx_hash.hex()))
        
        return PurchaseResult(
            success=True,
            wallet=checksum_wallet,
            tx_hash=tx_hash.hex(),
            quantity=quantity,
            gas_fee_usd=actual_gas_fee_usd,
            total_value_wei=total_value,
        )
        
    except Exception as e:
        error_msg = str(e)
        log.error(f"❌ [خطأ إرسال للمحفظة {checksum_wallet[:8]}] {error_msg}")
        
        error_lower = error_msg.lower()
        if "nonce" in error_lower:
            reason = "nonce_error"
        elif "insufficient funds" in error_lower:
            reason = "insufficient_funds"
        elif "replacement transaction underpriced" in error_lower:
            reason = "underpriced"
        elif "timeout" in error_lower:
            reason = "timeout"
        elif "connection" in error_lower:
            reason = "connection_error"
        else:
            reason = "tx_error"
        
        return PurchaseResult(
            success=False,
            wallet=checksum_wallet,
            reason=reason,
            error=error_msg
        )

async def _cleanup_pending_tx(w3: Web3, wallet: str, tx_hash: str):
    await asyncio.sleep(300)
    try:
        receipt = await asyncio.to_thread(w3.eth.get_transaction_receipt, tx_hash)
        if receipt:
            await pending_manager.remove_tx(wallet, tx_hash)
            log.info(f"✅ تم تأكيد المعاملة المتأخرة: {tx_hash[:10]}...")
    except:
        pass

# ==================== الشراء المتوازي المحسن ====================
async def purchase_parallel(
    w3: Web3,
    wallets_data: List[WalletData],
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: Optional[int],
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> List[PurchaseResult]:
    tasks = []
    for wallet_data in wallets_data:
        lock = await lock_manager.get_lock(wallet_data.wallet)
        
        async def purchase_with_lock(wd=wallet_data):
            async with lock:
                return await send_transaction_with_retry(
                    w3=w3,
                    wallet_data=wd,
                    nft_contract=nft_contract,
                    price_wei_per_token=price_wei_per_token,
                    max_per_wallet=max_per_wallet,
                    remaining_supply=remaining_supply,
                    eth_price_usd=eth_price_usd,
                    max_gas_fee_usd=max_gas_fee_usd,
                )
        
        tasks.append(purchase_with_lock())
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            processed_results.append(PurchaseResult(
                success=False,
                wallet=wallets_data[i].wallet,
                reason="exception",
                error=str(result)
            ))
        else:
            processed_results.append(result)
    
    return processed_results

# ==================== أدوات تحليلية ====================
def get_wallet_stats(wallet_data: WalletData) -> Dict[str, Any]:
    stats = wallet_data.stats.copy()
    total = stats["total_attempts"]
    if total > 0:
        stats["success_rate"] = (stats["successful"] / total) * 100
    else:
        stats["success_rate"] = 0.0
    return stats

def format_wallet_stats(wallet_data: WalletData) -> str:
    stats = get_wallet_stats(wallet_data)
    return (
        f"📊 <b>إحصائيات المحفظة</b>\n"
        f"المحفظة: <code>{wallet_data.wallet[:8]}...</code>\n"
        f"المحاولات: {stats['total_attempts']}\n"
        f"الناجحة: {stats['successful']}\n"
        f"الفاشلة: {stats['failed']}\n"
        f"نسبة النجاح: {stats['success_rate']:.1f}%\n"
        f"إجمالي الغاز: ${stats['total_gas_spent']:.4f}\n"
        f"آخر شراء: {stats['last_purchase_time'] or 'لا يوجد'}"
    )
