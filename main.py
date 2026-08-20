"""
النظام الكامل المحسن — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها.
المعيار الوحيد: وجود حساب X (Twitter)
بدون رسائل اكتشاف - فقط رسائل الشراء
"""


import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Set, List, Any, Tuple
from dataclasses import dataclass, field

import aiohttp
import requests
import websockets
from dotenv import load_dotenv
from web3 import Web3

from buyer import (
    WalletData,
    PurchaseStrategy,
    purchase_parallel,
    get_web3,
    get_onchain_public_price_wei,
    get_wallet_stats,
    format_wallet_stats,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

# ==================== الإعدادات الأساسية ====================
OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]
TELEGRAM_BOT_TOKENS = [t.strip() for t in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if t.strip()]
TELEGRAM_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

if not (len(PRIVATE_KEYS) == len(WALLETS) == len(TELEGRAM_BOT_TOKENS) == len(TELEGRAM_CHAT_IDS)):
    raise ValueError("أعداد المفاتيح، المحافظ، توكنات البوتات، و Chat IDs غير متطابقة في ملف .env!")

WALLETS_DATA: List[WalletData] = []
for i in range(len(WALLETS)):
    if i < 3:
        strategy = PurchaseStrategy.AGGRESSIVE
    elif i < 7:
        strategy = PurchaseStrategy.BALANCED
    else:
        strategy = PurchaseStrategy.CONSERVATIVE
    
    WALLETS_DATA.append(WalletData(
        wallet=WALLETS[i],
        private_key=PRIVATE_KEYS[i],
        bot_token=TELEGRAM_BOT_TOKENS[i],
        chat_id=TELEGRAM_CHAT_IDS[i],
        strategy=strategy,
    ))

ALCHEMY_API_KEY_ROBINHOOD = os.environ["ALCHEMY_API_KEY"]
ALCHEMY_API_KEY_ETHEREUM = os.environ["ALCHEMY_API_KEY_ETHEREUM"]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

# ==================== إعدادات الأداء ====================
HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 15
REJECTION_COOLDOWN_SECONDS = 120
SAVE_INTERVAL_SECONDS = 60
MAX_CONCURRENT_EVALUATIONS = 10  # زيادة للسرعة

POLL_NEW_DROPS_INTERVAL = int(os.environ.get("POLL_NEW_DROPS_INTERVAL", "60"))
MONITOR_RECENT_CONTRACTS = int(os.environ.get("MONITOR_RECENT_CONTRACTS", "30"))
CONTRACT_MONITOR_INTERVAL = int(os.environ.get("CONTRACT_MONITOR_INTERVAL", "15"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

# ==================== إعدادات السلاسل ====================
CHAIN_CONFIGS = {
    "robinhood": {
        "stream_chain_name": "robinhood",
        "rpc_url": f"https://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ROBINHOOD}",
        "max_gas_fee_usd": 0.05,
        "chain_id": 6900,
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ETHEREUM}",
        "max_gas_fee_usd": 0.50,
        "chain_id": 1,
    },
}

W3_INSTANCES = {}
for key, cfg in CHAIN_CONFIGS.items():
    try:
        W3_INSTANCES[key] = get_web3(cfg["rpc_url"])
        log.info(f"✅ تم الاتصال بـ {key}")
    except Exception as e:
        log.error(f"❌ فشل الاتصال بـ {key}: {e}")
        raise

STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# ==================== التخزين الدائم ====================
SUCCESS_FILE = Path("successful_mints.json")
WATCHLIST_FILE = Path("watchlist.json")
DETECTED_MINTS_FILE = Path("detected_mints.json")
STATS_FILE = Path("wallet_stats.json")
RECENT_CONTRACTS_FILE = Path("recent_contracts.json")

successful_mints: Dict[str, Set[str]] = {}
watchlist: Dict[str, Dict] = {}
detected_mints: Dict[str, Dict] = {}
in_flight: Set[str] = set()
rejected_cooldown: Dict[str, float] = {}
_eth_price_cache = {"value": None, "ts": 0}
evaluation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EVALUATIONS)
recent_contracts: Dict[str, Dict[str, Any]] = {}

# ==================== التخزين ====================
class PersistentStorage:
    def __init__(self):
        self.files = {
            "successful": SUCCESS_FILE,
            "watchlist": WATCHLIST_FILE,
            "detected": DETECTED_MINTS_FILE,
            "stats": STATS_FILE,
            "contracts": RECENT_CONTRACTS_FILE,
        }
        self.data = {
            "successful": {},
            "watchlist": {},
            "detected": {},
            "stats": {},
            "contracts": {},
        }
        self._load_all()
    
    def _load_all(self):
        if SUCCESS_FILE.exists():
            try:
                with open(SUCCESS_FILE, 'r') as f:
                    data = json.load(f)
                    self.data["successful"] = {k: set(v) for k, v in data.items()}
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل المينتات الناجحة: {e}")
        
        if WATCHLIST_FILE.exists():
            try:
                with open(WATCHLIST_FILE, 'r') as f:
                    self.data["watchlist"] = json.load(f)
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل قائمة المراقبة: {e}")
        
        if DETECTED_MINTS_FILE.exists():
            try:
                with open(DETECTED_MINTS_FILE, 'r') as f:
                    self.data["detected"] = json.load(f)
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل المينتات المكتشفة: {e}")
        
        if STATS_FILE.exists():
            try:
                with open(STATS_FILE, 'r') as f:
                    self.data["stats"] = json.load(f)
                    for wd in WALLETS_DATA:
                        if wd.wallet in self.data["stats"]:
                            wd.stats.update(self.data["stats"][wd.wallet])
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل الإحصائيات: {e}")
        
        if RECENT_CONTRACTS_FILE.exists():
            try:
                with open(RECENT_CONTRACTS_FILE, 'r') as f:
                    self.data["contracts"] = json.load(f)
            except Exception as e:
                log.warning(f"⚠️ تعذر تحميل العقود المكتشفة: {e}")
    
    def save_all(self):
        try:
            with open(SUCCESS_FILE, 'w') as f:
                json.dump(
                    {k: list(v) for k, v in self.data["successful"].items()},
                    f, indent=2
                )
            
            with open(WATCHLIST_FILE, 'w') as f:
                json.dump(self.data["watchlist"], f, indent=2)
            
            with open(DETECTED_MINTS_FILE, 'w') as f:
                json.dump(self.data["detected"], f, indent=2)
            
            stats_data = {}
            for wd in WALLETS_DATA:
                stats_data[wd.wallet] = wd.stats
            with open(STATS_FILE, 'w') as f:
                json.dump(stats_data, f, indent=2)
            
            with open(RECENT_CONTRACTS_FILE, 'w') as f:
                json.dump(self.data["contracts"], f, indent=2)
            
            log.debug("💾 تم حفظ البيانات")
        except Exception as e:
            log.error(f"❌ خطأ في حفظ البيانات: {e}")

storage = PersistentStorage()
successful_mints = storage.data["successful"]
watchlist = storage.data["watchlist"]
detected_mints = storage.data["detected"]
recent_contracts = storage.data["contracts"]

async def periodic_save():
    while True:
        await asyncio.sleep(SAVE_INTERVAL_SECONDS)
        storage.save_all()

# ==================== نظام الكشف ====================
class MintDetector:
    def __init__(self):
        self.detection_count = 0
        self.detection_sources = {
            "websocket": 0,
            "polling": 0,
            "contract_monitor": 0,
        }
    
    def is_valid_mint_event(self, payload: dict) -> Tuple[bool, Optional[str], Optional[str]]:
        try:
            from_account = payload.get("from_account", {})
            from_address = from_account.get("address", "").lower()
            if from_address != ZERO_ADDRESS:
                return False, None, None
            
            collection = payload.get("collection", {})
            slug = collection.get("slug", "")
            if not slug:
                return False, None, None
            
            item = payload.get("item", {})
            chain = item.get("chain", {})
            stream_chain_name = chain.get("name", "")
            chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
            if chain_key is None:
                return False, None, None
            
            return True, chain_key, slug
            
        except Exception as e:
            return False, None, None
    
    def record_detection(self, slug: str, source: str = "websocket"):
        self.detection_count += 1
        self.detection_sources[source] = self.detection_sources.get(source, 0) + 1
        log.info(f"🔔 مينت: {slug} (المصدر: {source})")

detector = MintDetector()

# ==================== ✅ تقييم مبسط (معيار واحد: X) ====================
class MintEvaluator:
    def __init__(self):
        self.evaluation_history: Dict[str, Dict] = {}
    
    def evaluate_quality(self, slug: str, detail: dict) -> Dict[str, Any]:
        twitter = detail.get('twitter_username')
        
        quality = {
            'has_twitter': bool(twitter),
            'twitter_username': twitter,
            'recommendation': 'buy' if twitter else 'skip',
        }
        
        self.evaluation_history[slug] = quality
        return quality

evaluator = MintEvaluator()

# ==================== ✅ تيليجرام - فقط رسائل الشراء ====================
class TelegramManager:
    def __init__(self):
        self.send_queue: asyncio.Queue = asyncio.Queue()
    
    def enqueue(self, bot_token: str, chat_id: str, text: str):
        self.send_queue.put_nowait({
            "bot_token": bot_token,
            "chat_id": chat_id,
            "text": text
        })
    
    def broadcast(self, text: str):
        for w in WALLETS_DATA:
            self.enqueue(w.bot_token, w.chat_id, text)
    
    def send_to_wallet(self, wallet_index: int, text: str):
        if 0 <= wallet_index < len(WALLETS_DATA):
            w = WALLETS_DATA[wallet_index]
            self.enqueue(w.bot_token, w.chat_id, text)
    
    async def sender_loop(self):
        while True:
            msg = await self.send_queue.get()
            try:
                telegram_api = f"https://api.telegram.org/bot{msg['bot_token']}"
                await asyncio.to_thread(
                    requests.post,
                    f"{telegram_api}/sendMessage",
                    data={
                        "chat_id": msg["chat_id"],
                        "text": msg["text"],
                        "parse_mode": "HTML"
                    },
                    timeout=10,
                )
            except Exception as e:
                log.error(f"❌ خطأ إرسال تليجرام: {e}")
            
            self.send_queue.task_done()
            await asyncio.sleep(0.1)
    
    # ✅ فقط رسالة شراء ناجح
    def build_success_message(self, detail: dict, result: dict, chain_key: str) -> str:
        name = detail.get("collection_name") or detail.get("collection_slug")
        url = detail.get("opensea_url", "")
        chain_label = "Robinhood" if chain_key == "robinhood" else "Ethereum"
        w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
        
        twitter = detail.get('twitter_username', '')
        twitter_text = f"\nحساب X: @{twitter}" if twitter else ""
        
        return (
            f"✅ <b>تم الشراء!</b> ({chain_label})\n\n"
            f"المحفظة: <code>{w_short}</code>\n"
            f"المجموعة: <b>{name}</b>{twitter_text}\n"
            f"الكمية: {result['quantity']}\n"
            f"رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
            f"المعاملة: <code>{result['tx_hash'][:10]}...</code>\n"
            f"🔗 {url}"
        )

telegram = TelegramManager()

# ==================== دوال مساعدة ====================
def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < 300):
        return _eth_price_cache["value"]
    
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8,
        )
        price = resp.json()["ethereum"]["usd"]
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except Exception as e:
        return _eth_price_cache["value"] or 3000.0

async def fetch_drop_detail(slug: str) -> Tuple[Optional[bool], Optional[Dict]]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{DROPS_API_BASE}/{slug}",
                headers={"x-api-key": OPENSEA_API_KEY},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    return True, await resp.json()
                return False, None
    except Exception as e:
        return None, None

def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def started_today_local(stage: dict) -> bool:
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

# ==================== الشراء ====================
async def try_buy_now_multi_wallet(
    slug: str,
    chain_key: str,
    detail: dict
) -> Optional[List[Dict[str, Any]]]:
    stage = detail.get("active_stage")
    if not stage:
        return None
    
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    if remaining <= 0:
        return None
    
    contract_address = detail.get("contract_address")
    if not contract_address:
        return None
    
    w3 = W3_INSTANCES[chain_key]
    eth_price_usd = get_eth_price_usd()
    
    onchain_price = await get_onchain_public_price_wei(w3, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
    
    if not is_free_or_negligible(price_wei, eth_price_usd):
        return None
    
    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]
    
    already_bought = successful_mints.get(slug, set())
    pending_wallets = [
        wd for wd in WALLETS_DATA
        if wd.wallet not in already_bought
    ]
    
    if not pending_wallets:
        return None
    
    for wd in pending_wallets:
        wd.current_detail = detail
        wd.chain_key = chain_key
    
    results = await purchase_parallel(
        w3=w3,
        wallets_data=pending_wallets,
        nft_contract=contract_address,
        price_wei_per_token=price_wei,
        max_per_wallet=max_per_wallet,
        remaining_supply=remaining,
        eth_price_usd=eth_price_usd,
        max_gas_fee_usd=max_gas_fee_usd,
    )
    
    for result in results:
        if result.success:
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(result.wallet)
            
            wallet_index = next(
                i for i, wd in enumerate(WALLETS_DATA)
                if wd.wallet == result.wallet
            )
            # ✅ فقط رسالة شراء ناجح
            msg = telegram.build_success_message(detail, {
                'wallet': result.wallet,
                'quantity': result.quantity,
                'gas_fee_usd': result.gas_fee_usd,
                'tx_hash': result.tx_hash,
            }, chain_key)
            telegram.send_to_wallet(wallet_index, msg)
    
    storage.save_all()
    return [vars(r) for r in results]

# ==================== ✅ التقييم والشراء (بدون رسائل) ====================
async def evaluate_and_buy(slug: str, chain_key: str, source: str = "websocket"):
    """تقييم سريع وشراء - بدون أي رسائل"""
    async with evaluation_semaphore:
        if (len(successful_mints.get(slug, set())) >= len(WALLETS_DATA) or
            slug in watchlist or slug in in_flight):
            return
        
        in_flight.add(slug)
        try:
            found, detail = await fetch_drop_detail(slug)
            if not found or not detail or not detail.get("is_minting"):
                return
            
            stage = detail.get("active_stage")
            if not stage or not started_today_local(stage):
                return
            
            if slug not in detected_mints:
                detected_mints[slug] = {
                    'detected_at': time.time(),
                    'chain': chain_key,
                    'slug': slug,
                    'name': detail.get('collection_name') or slug,
                    'detected_by': source,
                }
                storage.save_all()
                detector.record_detection(slug, source)
            
            # تتبع العقد للمستوى 3
            contract_address = detail.get("contract_address")
            if contract_address:
                recent_contracts[contract_address.lower()] = {
                    "slug": slug,
                    "chain_key": chain_key,
                    "timestamp": time.time(),
                    "detected_by": source,
                }
                if len(recent_contracts) > MONITOR_RECENT_CONTRACTS * 2:
                    sorted_contracts = sorted(
                        recent_contracts.items(),
                        key=lambda x: x[1]["timestamp"]
                    )
                    for addr, _ in sorted_contracts[:MONITOR_RECENT_CONTRACTS // 2]:
                        recent_contracts.pop(addr, None)
                storage.save_all()
            
            # ✅ التحقق من السعر (مجاني فقط)
            w3 = W3_INSTANCES[chain_key]
            eth_price_usd = get_eth_price_usd()
            
            if contract_address:
                onchain_price = await get_onchain_public_price_wei(w3, contract_address)
                price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
                
                if not is_free_or_negligible(price_wei, eth_price_usd):
                    return
            
            # ✅ الشرط الوحيد: وجود X
            twitter_username = await asyncio.to_thread(
                get_twitter_username_from_opensea, slug, OPENSEA_API_KEY
            )
            
            # ✅ إذا كان يوجد X → شراء فوري (بدون رسائل)
            if twitter_username:
                detail['twitter_username'] = twitter_username
                log.info(f"🛒 شراء '{slug}' - X: @{twitter_username}")
                results = await try_buy_now_multi_wallet(slug, chain_key, detail)
                
                if results is None or len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": detail,
                        "twitter_username": twitter_username,
                        "detected_by": source,
                    }
                    storage.save_all()
            else:
                log.info(f"⏭️ '{slug}' لا يوجد X - تخطي")
            
        except Exception as e:
            log.error(f"❌ خطأ: {e}")
        finally:
            in_flight.discard(slug)

# ==================== 🟢 المستوى 2: الفحص الدوري ====================
async def poll_new_drops():
    processed_slugs_polling = set()
    
    while True:
        try:
            await asyncio.sleep(POLL_NEW_DROPS_INTERVAL)
            
            if not BOT_ENABLED:
                continue
            
            drops = await asyncio.to_thread(fetch_recent_drops_fast)
            
            for drop in drops:
                slug = drop.get("slug")
                if not slug or slug in processed_slugs_polling:
                    continue
                
                processed_slugs_polling.add(slug)
                
                chain_key = drop.get("chain", "robinhood")
                if chain_key not in CHAIN_CONFIGS:
                    chain_key = "robinhood"
                
                if slug not in detected_mints:
                    asyncio.create_task(
                        evaluate_and_buy(slug, chain_key, source="polling")
                    )
        
        except Exception as e:
            log.error(f"خطأ في الفحص الدوري: {e}")
            await asyncio.sleep(POLL_NEW_DROPS_INTERVAL)

def fetch_recent_drops_fast() -> List[Dict]:
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}?limit=50&order_by=created_at&order_direction=desc",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("drops", [])
        return []
    except:
        return []

# ==================== 🟣 المستوى 3: مراقبة العقود ====================
async def monitor_contracts():
    processed_monitor = set()
    
    while True:
        try:
            await asyncio.sleep(CONTRACT_MONITOR_INTERVAL)
            
            if not BOT_ENABLED:
                continue
            
            now = time.time()
            
            recent = [
                (addr, data)
                for addr, data in recent_contracts.items()
                if now - data.get("timestamp", 0) < 600
            ]
            
            for contract_addr, data in recent:
                slug = data.get("slug")
                if not slug:
                    continue
                
                monitor_key = f"{contract_addr}_{slug}"
                if monitor_key in processed_monitor:
                    continue
                
                processed_monitor.add(monitor_key)
                if len(processed_monitor) > 1000:
                    processed_monitor.clear()
                
                chain_key = data.get("chain_key", "robinhood")
                
                try:
                    w3 = W3_INSTANCES[chain_key]
                    public_drop = await asyncio.wait_for(
                        asyncio.to_thread(get_onchain_public_price_wei, w3, contract_addr),
                        timeout=5,
                    )
                    
                    if public_drop is not None:
                        start_time, end_time = await asyncio.to_thread(
                            get_mint_times, w3, contract_addr
                        )
                        current_time = int(time.time())
                        
                        if start_time and abs(current_time - start_time) < 120:
                            log.info(f"🔍 عقد '{slug}' بدأ للتو")
                            eth_price_usd = get_eth_price_usd()
                            
                            if is_free_or_negligible(public_drop, eth_price_usd):
                                found, detail = await fetch_drop_detail(slug)
                                if found and detail:
                                    asyncio.create_task(
                                        evaluate_and_buy(slug, chain_key, source="contract_monitor")
                                    )
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    log.debug(f"خطأ في فحص العقد {contract_addr[:8]}: {e}")
        
        except Exception as e:
            log.error(f"خطأ في مراقبة العقود: {e}")
            await asyncio.sleep(CONTRACT_MONITOR_INTERVAL)

def get_mint_times(w3: Web3, nft_contract: str) -> Tuple[Optional[int], Optional[int]]:
    try:
        from buyer import SEADROP_ADDRESS, SEADROP_ABI
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return int(public_drop[1]), int(public_drop[2])
    except:
        return None, None

# ==================== حلقة المراقبة ====================
async def watch_loop():
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        if not watchlist:
            continue
        
        for slug in list(watchlist.keys()):
            if slug in in_flight or len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                watchlist.pop(slug, None)
                storage.save_all()
                continue
            
            entry = watchlist.get(slug)
            if not entry:
                continue
            
            in_flight.add(slug)
            try:
                chain_key = entry["chain_key"]
                found, fresh_detail = await fetch_drop_detail(slug)
                
                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    watchlist.pop(slug, None)
                    storage.save_all()
                    continue
                
                stage = fresh_detail.get("active_stage")
                if not stage or not started_today_local(stage):
                    watchlist.pop(slug, None)
                    storage.save_all()
                    continue
                
                if 'twitter_username' not in entry:
                    twitter_username = await asyncio.to_thread(
                        get_twitter_username_from_opensea, slug, OPENSEA_API_KEY
                    )
                    if twitter_username:
                        entry['twitter_username'] = twitter_username
                        fresh_detail['twitter_username'] = twitter_username
                
                results = await try_buy_now_multi_wallet(slug, chain_key, fresh_detail)
                
                if results is None:
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": fresh_detail,
                        "twitter_username": entry.get('twitter_username'),
                        "detected_by": entry.get('detected_by', 'unknown'),
                    }
                    continue
                
                if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                    watchlist.pop(slug, None)
                else:
                    watchlist[slug] = {
                        "chain_key": chain_key,
                        "detail": fresh_detail,
                        "twitter_username": entry.get('twitter_username'),
                        "detected_by": entry.get('detected_by', 'unknown'),
                    }
                
                storage.save_all()
                
            except Exception as e:
                log.error(f"❌ خطأ بدورة مراقبة '{slug}': {e}")
            finally:
                in_flight.discard(slug)

# ==================== 🔵 WebSocket ====================
async def listen_opensea():
    msg_ref = 0
    
    while True:
        try:
            async with websockets.connect(
                STREAM_URL,
                ping_interval=None,
                open_timeout=15
            ) as ws:
                log.info(f"✅ متصل بـ OpenSea Stream")
                
                join_ref = str(msg_ref)
                await ws.send(json.dumps([
                    join_ref, join_ref, "collection:*", "phx_join", {}
                ]))
                msg_ref += 1
                last_heartbeat = time.time()
                
                while True:
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([
                            None, hb_ref, "phoenix", "heartbeat", {}
                        ]))
                        msg_ref += 1
                        last_heartbeat = time.time()
                    
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        continue
                    
                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    
                    if isinstance(parsed, list) and len(parsed) == 5:
                        _jref, _ref, _topic, event_name, payload_wrapper = parsed
                    else:
                        continue
                    
                    if event_name != "item_transferred":
                        continue
                    
                    payload = (payload_wrapper or {}).get("payload") or {}
                    
                    is_valid, chain_key, slug = detector.is_valid_mint_event(payload)
                    if not is_valid or not slug:
                        continue
                    
                    if slug in detected_mints:
                        detected_mints[slug]['detected_at'] = time.time()
                        continue
                    
                    asyncio.create_task(evaluate_and_buy(slug, chain_key, source="websocket"))
                    
        except Exception as e:
            log.warning(f"⚠️ انقطع الاتصال: {e}")
            await asyncio.sleep(3)

# ==================== واجهة الأوامر ====================
async def command_handler():
    while True:
        await asyncio.sleep(60)
        
        if int(time.time()) % 3600 < 60:
            for i, wd in enumerate(WALLETS_DATA):
                stats_msg = format_wallet_stats(wd)
                telegram.send_to_wallet(i, stats_msg)

# ==================== التشغيل ====================
async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        return
    
    telegram.broadcast(
        f"✅ <b>تم تشغيل النظام</b>\n\n"
        f"عدد المحافظ: {len(WALLETS_DATA)}\n"
        f"المعيار: وجود X فقط\n"
        f"مستويات الكشف: WebSocket + فحص دوري + مراقبة عقود"
    )
    
    await asyncio.gather(
        listen_opensea(),
        poll_new_drops(),
        monitor_contracts(),
        watch_loop(),
        telegram.sender_loop(),
        periodic_save(),
        command_handler(),
        return_exceptions=True
    )

def main():
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("🛑 تم الإيقاف")
            storage.save_all()
            break
        except Exception as e:
            log.critical(f"❌ توقف: {e}")
            storage.save_all()
            time.sleep(5)
            continue
        else:
            break

if __name__ == "__main__":
    main()
