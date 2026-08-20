"""
النظام الكامل — سرعة قصوى لاكتشاف وشراء المينتات:
  - WebSocket + فحص دوري + مراقبة العقود
  - 3 مستويات للكشف لضمان عدم تفويت أي مينت
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional, Dict, Any, List, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum

import requests
import websockets
from dotenv import load_dotenv
from web3 import Web3

from buyer import (
    get_web3,
    get_onchain_public_price_wei,
    get_mint_times,
    LockManager,
    purchase_with_retry,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

# ---------------------------------------------------------------------------
# الإعدادات
# ---------------------------------------------------------------------------

class MintStatus(Enum):
    NOT_STARTED = "not_started"
    ACTIVE = "active"
    ENDED = "ended"
    UNKNOWN = "unknown"

@dataclass
class ChainConfig:
    stream_chain_name: str
    rpc_url: str
    max_gas_fee_usd: float

@dataclass
class WalletConfig:
    wallet: str
    private_key: str
    bot_token: str
    chat_id: str
    chain_key: str = ""
    current_detail: Dict[str, Any] = field(default_factory=dict)

@dataclass
class PendingMint:
    """مينت قادم تم فحصه مسبقاً"""
    slug: str
    chain_key: str
    contract_address: str
    start_time: int
    end_time: Optional[int]
    twitter_username: Optional[str]
    collection_name: str
    opensea_url: str
    added_at: float
    detected_by: str = "websocket"  # "websocket", "polling", "contract_monitor"

class Config:
    def __init__(self):
        self.opensea_api_key = self._get_env("OPENSEA_API_KEY", required=True)
        self.bot_enabled = self._get_env("BOT_ENABLED", "false").lower() == "true"
        
        self.alchemy_api_key_robinhood = self._get_env("ALCHEMY_API_KEY", required=True)
        self.alchemy_api_key_ethereum = self._get_env("ALCHEMY_API_KEY_ETHEREUM", required=True)
        
        self.chains = {
            "robinhood": ChainConfig(
                stream_chain_name="robinhood",
                rpc_url=f"https://robinhood-mainnet.g.alchemy.com/v2/{self.alchemy_api_key_robinhood}",
                max_gas_fee_usd=float(self._get_env("MAX_GAS_FEE_ROBINHOOD", "0.05")),
            ),
            "ethereum": ChainConfig(
                stream_chain_name="ethereum",
                rpc_url=f"https://eth-mainnet.g.alchemy.com/v2/{self.alchemy_api_key_ethereum}",
                max_gas_fee_usd=float(self._get_env("MAX_GAS_FEE_ETHEREUM", "0.50")),
            ),
        }
        
        self.wallets = self._load_wallets()
        
        self.heartbeat_interval = int(self._get_env("HEARTBEAT_INTERVAL", "20"))
        self.recv_timeout = int(self._get_env("RECV_TIMEOUT", "5"))
        self.free_price_threshold = float(self._get_env("FREE_PRICE_THRESHOLD", "0.01"))
        
        # إشعار قبل 12 ساعة
        self.notify_before_start = 43200
        
        # ✅ إعدادات جديدة للكشف الإضافي
        self.poll_new_drops_interval = int(self._get_env("POLL_NEW_DROPS_INTERVAL", "60"))  # دقيقة واحدة
        self.monitor_recent_contracts = int(self._get_env("MONITOR_RECENT_CONTRACTS", "20"))  # 20 عقد حديث
        
    def _get_env(self, key: str, default: str = "", required: bool = False) -> str:
        value = os.environ.get(key, default).strip()
        if required and not value:
            raise ValueError(f"المتغير {key} مطلوب!")
        return value
    
    def _load_wallets(self) -> List[WalletConfig]:
        private_keys = [k.strip() for k in self._get_env("PRIVATE_KEYS", required=True).split(",") if k.strip()]
        wallets = [w.strip() for w in self._get_env("WALLETS", required=True).split(",") if w.strip()]
        bot_tokens = [t.strip() for t in self._get_env("TELEGRAM_BOT_TOKENS", required=True).split(",") if t.strip()]
        chat_ids = [c.strip() for c in self._get_env("TELEGRAM_CHAT_IDS", required=True).split(",") if c.strip()]
        
        if not (len(private_keys) == len(wallets) == len(bot_tokens) == len(chat_ids)):
            raise ValueError("أعداد غير متطابقة!")
        
        return [
            WalletConfig(
                wallet=wallets[i],
                private_key=private_keys[i],
                bot_token=bot_tokens[i],
                chat_id=chat_ids[i],
            )
            for i in range(len(wallets))
        ]

config = Config()

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={config.opensea_api_key}&vsn=2.0.0"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

w3_instances = {key: get_web3(cfg.rpc_url) for key, cfg in config.chains.items()}
stream_name_to_chain = {cfg.stream_chain_name: key for key, cfg in config.chains.items()}

lock_manager = LockManager()

# ✅ تتبع مبسط وسريع
pending_mints: Dict[str, PendingMint] = {}
successful_mints: Dict[str, set] = {}
processed_slugs: Set[str] = set()
in_flight: Set[str] = set()

# ✅ تتبع العقود التي تم اكتشافها مؤخراً
recent_contracts: Dict[str, Dict[str, Any]] = {}  # contract_address -> {slug, chain_key, timestamp}

_eth_price_cache = {"value": None, "ts": 0, "ttl": 300}

# ---------------------------------------------------------------------------
# دوال مساعدة سريعة
# ---------------------------------------------------------------------------

def get_eth_price_usd() -> float:
    """جلب سعر ETH مع cache سريع"""
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < _eth_price_cache["ttl"]):
        return _eth_price_cache["value"]
    
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=5,
        )
        price = float(resp.json()["ethereum"]["usd"])
        _eth_price_cache.update({"value": price, "ts": now})
        return price
    except:
        return _eth_price_cache["value"] or 3000.0

def calculate_price_usd(price_wei: int, eth_price_usd: float) -> float:
    return (price_wei / 1e18) * eth_price_usd

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    return calculate_price_usd(price_wei, eth_price_usd) < config.free_price_threshold

def format_time(wait_seconds: int) -> str:
    hours = wait_seconds // 3600
    minutes = (wait_seconds % 3600) // 60
    secs = wait_seconds % 60
    
    if hours > 0:
        return f"{hours} ساعة و {minutes} دقيقة" if minutes > 0 else f"{hours} ساعة"
    elif minutes > 0:
        return f"{minutes} دقيقة"
    else:
        return f"{secs} ثانية"

# ---------------------------------------------------------------------------
# تيليجرام سريع
# ---------------------------------------------------------------------------

class TelegramManager:
    def __init__(self):
        self.send_queue: asyncio.Queue = asyncio.Queue()
        self._sender_task: Optional[asyncio.Task] = None
    
    async def start(self):
        self._sender_task = asyncio.create_task(self._sender_loop())
    
    async def stop(self):
        if self._sender_task:
            self._sender_task.cancel()
    
    def enqueue(self, bot_token: str, chat_id: str, text: str):
        try:
            self.send_queue.put_nowait({"bot_token": bot_token, "chat_id": chat_id, "text": text})
        except:
            pass
    
    def broadcast(self, text: str, wallets: List[WalletConfig]):
        for wallet in wallets:
            self.enqueue(wallet.bot_token, wallet.chat_id, text)
    
    async def _sender_loop(self):
        while True:
            try:
                msg = await self.send_queue.get()
                telegram_api = f"https://api.telegram.org/bot{msg['bot_token']}"
                await asyncio.to_thread(
                    requests.post,
                    f"{telegram_api}/sendMessage",
                    data={"chat_id": msg["chat_id"], "text": msg["text"], "parse_mode": "HTML", "disable_web_page_preview": False},
                    timeout=5,
                )
                self.send_queue.task_done()
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                break
            except:
                await asyncio.sleep(0.5)

telegram_manager = TelegramManager()

# ---------------------------------------------------------------------------
# رسائل سريعة
# ---------------------------------------------------------------------------

def build_start_notification(collection_name: str, opensea_url: str, wait_seconds: int, detected_by: str = "") -> str:
    time_str = format_time(wait_seconds)
    detection_info = f" (كشف: {detected_by})" if detected_by else ""
    msg = f"🔔 <b>مينت قادم!</b>{detection_info}\n\nالمجموعة: <b>{collection_name}</b>\nيبدأ خلال: {time_str}"
    if opensea_url:
        msg += f"\n\n🔗 <a href='{opensea_url}'>OpenSea</a>"
    return msg

def build_success_message(wallet_short: str, collection_name: str, opensea_url: str, chain_label: str, quantity: int) -> str:
    msg = f"✅ <b>تم الشراء!</b> ({chain_label})\n\nالمحفظة: <code>{wallet_short}</code>\nالمجموعة: <b>{collection_name}</b>\nالكمية: {quantity}"
    if opensea_url:
        msg += f"\n\n🔗 <a href='{opensea_url}'>OpenSea</a>"
    return msg

# ---------------------------------------------------------------------------
# ✅ شراء سريع
# ---------------------------------------------------------------------------

async def buy_immediately(
    slug: str,
    chain_key: str,
    contract_address: str,
    price_wei: int,
    collection_name: str,
    opensea_url: str,
):
    """شراء فوري لجميع المحافظ"""
    
    w3 = w3_instances[chain_key]
    eth_price_usd = get_eth_price_usd()
    max_gas_fee_usd = config.chains[chain_key].max_gas_fee_usd
    
    already_bought = successful_mints.get(slug, set())
    pending_wallets = [w for w in config.wallets if w.wallet.lower() not in already_bought]
    
    if not pending_wallets:
        return
    
    tasks = []
    for wallet in pending_wallets:
        wallet.chain_key = chain_key
        
        lock = lock_manager.get_lock(wallet.wallet)
        
        async def buy_one(w=wallet, l=lock):
            async with l:
                try:
                    if w.wallet.lower() in successful_mints.get(slug, set()):
                        return
                    
                    result = await purchase_with_retry(
                        w3=w3,
                        private_key=w.private_key,
                        wallet_address=w.wallet,
                        nft_contract=contract_address,
                        price_wei_per_token=price_wei,
                        max_per_wallet=None,
                        remaining_supply=1000,
                        eth_price_usd=eth_price_usd,
                        max_gas_fee_usd=max_gas_fee_usd,
                        max_retries=2,
                        retry_delay_base=0.5,
                    )
                    
                    if result.get("success"):
                        if slug not in successful_mints:
                            successful_mints[slug] = set()
                        successful_mints[slug].add(w.wallet.lower())
                        
                        wallet_short = f"{w.wallet[:6]}...{w.wallet[-4:]}"
                        message = build_success_message(
                            wallet_short,
                            collection_name,
                            opensea_url,
                            "Robinhood" if chain_key == "robinhood" else "Ethereum",
                            result.get("quantity", 0),
                        )
                        telegram_manager.enqueue(w.bot_token, w.chat_id, message)
                except Exception as e:
                    log.error(f"خطأ شراء {w.wallet[:8]}: {e}")
                finally:
                    lock_manager.release_lock(w.wallet)
        
        tasks.append(buy_one())
    
    await asyncio.gather(*tasks, return_exceptions=True)

# ---------------------------------------------------------------------------
# ✅ فحص تويتر سريع مع cache
# ---------------------------------------------------------------------------

_twitter_cache: Dict[str, Tuple[float, Optional[str]]] = {}
_twitter_cache_ttl = 3600

async def check_twitter_fast(slug: str) -> Tuple[bool, Optional[str]]:
    if slug in _twitter_cache:
        ts, username = _twitter_cache[slug]
        if time.time() - ts < _twitter_cache_ttl:
            return bool(username), username
    
    try:
        username = await asyncio.wait_for(
            asyncio.to_thread(get_twitter_username_from_opensea, slug, config.opensea_api_key),
            timeout=8,
        )
        _twitter_cache[slug] = (time.time(), username)
        return bool(username), username
    except:
        return False, None

# ---------------------------------------------------------------------------
# ✅ وظيفة الكشف الأساسية (مستوى 1)
# ---------------------------------------------------------------------------

async def process_new_mint_fast(slug: str, chain_key: str, payload: dict, detected_by: str = "websocket"):
    """معالجة سريعة للمينت الجديد"""
    
    if slug in processed_slugs or slug in in_flight:
        return
    
    in_flight.add(slug)
    processed_slugs.add(slug)
    
    try:
        # ✅ 1. استخراج البيانات من WebSocket مباشرة (0 ثانية)
        collection = payload.get("collection", {}) or {}
        item = payload.get("item", {}) or {}
        
        contract_address = (
            collection.get("contract_address") or
            (item.get("nft", {}) or {}).get("contract") or
            ""
        )
        
        collection_name = collection.get("name") or collection.get("slug") or slug
        opensea_url = collection.get("opensea_url") or f"https://opensea.io/collection/{slug}"
        
        if not contract_address:
            # ✅ Fallback قوي
            found, detail = await asyncio.to_thread(fetch_drop_detail_fast, slug)
            if found and detail:
                contract_address = detail.get("contract_address", "")
                collection_name = detail.get("collection_name") or collection_name
                opensea_url = detail.get("opensea_url") or opensea_url
        
        if not contract_address:
            return
        
        # ✅ تتبع العقد لاكتشافات المستقبلية
        recent_contracts[contract_address.lower()] = {
            "slug": slug,
            "chain_key": chain_key,
            "timestamp": time.time()
        }
        # الحفاظ على عدد محدود
        if len(recent_contracts) > config.monitor_recent_contracts * 2:
            sorted_contracts = sorted(recent_contracts.items(), key=lambda x: x[1]["timestamp"])
            for addr, _ in sorted_contracts[:config.monitor_recent_contracts // 2]:
                recent_contracts.pop(addr, None)
        
        w3 = w3_instances[chain_key]
        
        # ✅ 2. قراءة blockchain واحدة فقط (0.5 ثانية)
        try:
            public_drop = await asyncio.wait_for(
                asyncio.to_thread(get_full_drop_info_fast, w3, contract_address),
                timeout=5,
            )
        except:
            return
        
        if not public_drop:
            return
        
        price_wei = public_drop[0]
        start_time = public_drop[1]
        end_time = public_drop[2]
        
        current_time = int(time.time())
        eth_price_usd = get_eth_price_usd()
        
        # ✅ 3. المينت لم يبدأ - فحص مسبق سريع
        if start_time and current_time < start_time:
            wait_seconds = start_time - current_time
            
            if wait_seconds > config.notify_before_start:
                return
            
            log.info(f"🔔 '{slug}' سيبدأ خلال {wait_seconds} ثانية (كشف: {detected_by})")
            
            has_twitter, twitter_username = await check_twitter_fast(slug)
            
            if not has_twitter:
                log.info(f"⏭️ '{slug}' لا يوجد X")
                return
            
            pending_mints[slug] = PendingMint(
                slug=slug,
                chain_key=chain_key,
                contract_address=contract_address,
                start_time=start_time,
                end_time=end_time,
                twitter_username=twitter_username,
                collection_name=collection_name,
                opensea_url=opensea_url,
                added_at=time.time(),
                detected_by=detected_by,
            )
            
            message = build_start_notification(collection_name, opensea_url, wait_seconds, detected_by)
            telegram_manager.broadcast(message, config.wallets)
            return
        
        # ✅ 4. المينت انتهى
        if end_time and current_time > end_time:
            return
        
        # ✅ 5. المينت نشط - فحص السعر فقط
        if not is_free_or_negligible(price_wei, eth_price_usd):
            return
        
        log.info(f"🆓 '{slug}' مجاني - شراء (كشف: {detected_by})")
        
        has_twitter, twitter_username = await check_twitter_fast(slug)
        
        if not has_twitter:
            log.info(f"⏭️ '{slug}' لا يوجد X")
            return
        
        await buy_immediately(
            slug=slug,
            chain_key=chain_key,
            contract_address=contract_address,
            price_wei=price_wei,
            collection_name=collection_name,
            opensea_url=opensea_url,
        )
    
    except Exception as e:
        log.error(f"خطأ '{slug}': {e}")
    finally:
        in_flight.discard(slug)

def fetch_drop_detail_fast(slug: str) -> Tuple[bool, Optional[Dict]]:
    """Fallback سريع للـ API"""
    try:
        resp = requests.get(
            f"https://api.opensea.io/api/v2/drops/{slug}",
            headers={"x-api-key": config.opensea_api_key},
            timeout=5,
        )
        if resp.status_code == 200:
            return True, resp.json()
        return False, None
    except:
        return False, None

def get_full_drop_info_fast(w3: Web3, contract_address: str):
    """قراءة واحدة تجلب كل المعلومات"""
    try:
        from buyer import SEADROP_ADDRESS, SEADROP_ABI
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(contract_address)
        ).call()
        return public_drop
    except:
        return None

# ---------------------------------------------------------------------------
# ✅ المستوى 2: الفحص الدوري للمينتات الجديدة
# ---------------------------------------------------------------------------

async def poll_new_drops():
    """فحص دوري للمينتات الجديدة عبر OpenSea API"""
    processed_slugs_polling = set()
    
    while True:
        try:
            await asyncio.sleep(config.poll_new_drops_interval)
            
            if not config.bot_enabled:
                continue
            
            log.debug(f"🔄 فحص دوري للمينتات الجديدة...")
            
            # جلب أحدث المينتات
            drops = await asyncio.to_thread(fetch_recent_drops_fast)
            
            for drop in drops:
                slug = drop.get("slug")
                if not slug or slug in processed_slugs_polling:
                    continue
                
                processed_slugs_polling.add(slug)
                
                chain_key = drop.get("chain", "robinhood")
                if chain_key not in config.chains:
                    chain_key = "robinhood"  # افتراضي
                
                # ✅ محاكاة payload بسيطة
                fake_payload = {
                    "collection": {
                        "slug": slug,
                        "name": drop.get("name", slug),
                        "contract_address": drop.get("contract_address", ""),
                        "opensea_url": f"https://opensea.io/collection/{slug}",
                    },
                    "item": {"nft": {"contract": drop.get("contract_address", "")}},
                }
                
                await process_new_mint_fast(slug, chain_key, fake_payload, detected_by="polling")
        
        except Exception as e:
            log.error(f"خطأ في الفحص الدوري: {e}")
            await asyncio.sleep(config.poll_new_drops_interval)

def fetch_recent_drops_fast() -> List[Dict]:
    """جلب المينتات الجديدة"""
    try:
        resp = requests.get(
            "https://api.opensea.io/api/v2/drops?limit=50&order_by=created_at&order_direction=desc",
            headers={"x-api-key": config.opensea_api_key},
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("drops", [])
        return []
    except:
        return []

# ---------------------------------------------------------------------------
# ✅ المستوى 3: مراقبة العقود النشطة
# ---------------------------------------------------------------------------

async def monitor_contracts():
    """مراقبة العقود التي تم اكتشافها مؤخراً"""
    while True:
        try:
            await asyncio.sleep(10)  # كل 10 ثواني
            
            if not config.bot_enabled:
                continue
            
            now = time.time()
            
            # فحص العقود الحديثة (آخر 5 دقائق)
            recent = [
                (addr, data)
                for addr, data in recent_contracts.items()
                if now - data["timestamp"] < 300  # 5 دقائق
            ]
            
            for contract_addr, data in recent:
                slug = data["slug"]
                chain_key = data["chain_key"]
                
                # ✅ فحص السلسلة مرة أخرى
                try:
                    w3 = w3_instances[chain_key]
                    public_drop = await asyncio.wait_for(
                        asyncio.to_thread(get_full_drop_info_fast, w3, contract_addr),
                        timeout=3,
                    )
                    
                    if public_drop:
                        price_wei = public_drop[0]
                        start_time = public_drop[1]
                        current_time = int(time.time())
                        
                        # ✅ المينت بدأ للتو
                        if start_time and abs(current_time - start_time) < 60:
                            eth_price_usd = get_eth_price_usd()
                            
                            if is_free_or_negligible(price_wei, eth_price_usd):
                                log.info(f"🔍 عقد '{slug}' بدأ للتو - شراء (مراقبة العقود)")
                                
                                # ✅ شراء مباشر
                                await buy_immediately(
                                    slug=slug,
                                    chain_key=chain_key,
                                    contract_address=contract_addr,
                                    price_wei=price_wei,
                                    collection_name=slug,
                                    opensea_url=f"https://opensea.io/collection/{slug}",
                                )
                except:
                    pass
        
        except Exception as e:
            log.error(f"خطأ في مراقبة العقود: {e}")
            await asyncio.sleep(30)

# ---------------------------------------------------------------------------
# ✅ حلقة المراقبة - نوم ذكي فقط
# ---------------------------------------------------------------------------

async def watch_loop():
    """تنام حتى وقت البدء ثم تشتري فوراً"""
    
    while True:
        if not pending_mints:
            await asyncio.sleep(5)
            continue
        
        now = time.time()
        
        upcoming = sorted(
            pending_mints.items(),
            key=lambda x: x[1].start_time
        )
        
        next_slug, next_mint = upcoming[0]
        
        sleep_seconds = next_mint.start_time - now - 1
        
        if sleep_seconds > 0:
            log.info(f"😴 '{next_slug}' بعد {sleep_seconds} ثانية (كشف: {next_mint.detected_by})")
            await asyncio.sleep(sleep_seconds)
        
        now = time.time()
        if now < next_mint.start_time:
            await asyncio.sleep(next_mint.start_time - now)
        
        log.info(f"🎉 '{next_slug}' بدأ! - شراء فوري")
        
        try:
            w3 = w3_instances[next_mint.chain_key]
            public_drop = await asyncio.wait_for(
                asyncio.to_thread(get_full_drop_info_fast, w3, next_mint.contract_address),
                timeout=3,
            )
            
            if public_drop:
                price_wei = public_drop[0]
                eth_price_usd = get_eth_price_usd()
                
                if is_free_or_negligible(price_wei, eth_price_usd):
                    await buy_immediately(
                        slug=next_slug,
                        chain_key=next_mint.chain_key,
                        contract_address=next_mint.contract_address,
                        price_wei=price_wei,
                        collection_name=next_mint.collection_name,
                        opensea_url=next_mint.opensea_url,
                    )
        except Exception as e:
            log.error(f"خطأ شراء '{next_slug}': {e}")
        finally:
            pending_mints.pop(next_slug, None)

# ---------------------------------------------------------------------------
# ✅ OpenSea Stream
# ---------------------------------------------------------------------------

class OpenSeaStream:
    def __init__(self, url: str):
        self.url = url
        self.msg_ref = 0
        self.backoff = 1
        self.max_backoff = 60
    
    async def listen(self):
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=None, open_timeout=10) as ws:
                    log.info("متصل بـ OpenSea Stream")
                    
                    self.backoff = 1
                    
                    join_ref = str(self.msg_ref)
                    await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                    self.msg_ref += 1
                    
                    last_heartbeat = time.time()
                    
                    while True:
                        if time.time() - last_heartbeat > config.heartbeat_interval:
                            hb_ref = str(self.msg_ref)
                            await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                            self.msg_ref += 1
                            last_heartbeat = time.time()
                        
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=config.recv_timeout)
                        except asyncio.TimeoutError:
                            continue
                        
                        asyncio.create_task(self._process_message_fast(raw))
            
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError):
                await asyncio.sleep(self.backoff)
                self.backoff = min(self.backoff * 2, self.max_backoff)
            except Exception as e:
                log.error(f"خطأ: {e}")
                await asyncio.sleep(3)
    
    async def _process_message_fast(self, raw: str):
        try:
            parsed = json.loads(raw)
        except:
            return
        
        if not isinstance(parsed, list) or len(parsed) != 5:
            return
        
        _, _, _, event_name, payload_wrapper = parsed
        
        if event_name != "item_transferred":
            return
        
        payload = (payload_wrapper or {}).get("payload") or {}
        item = payload.get("item", {}) or {}
        stream_chain_name = (item.get("chain", {}) or {}).get("name", "")
        
        chain_key = stream_name_to_chain.get(stream_chain_name)
        if chain_key is None:
            return
        
        from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
        if from_address != ZERO_ADDRESS:
            return
        
        slug = (payload.get("collection", {}) or {}).get("slug", "")
        if not slug:
            return
        
        asyncio.create_task(process_new_mint_fast(slug, chain_key, payload, detected_by="websocket"))

# ---------------------------------------------------------------------------
# تشغيل
# ---------------------------------------------------------------------------

async def run():
    log.info(f"بدء التشغيل مع {len(config.wallets)} محافظ")
    log.info("🟢 مستويات الكشف: WebSocket + فحص دوري + مراقبة العقود")
    
    await telegram_manager.start()
    
    if not config.bot_enabled:
        log.warning("BOT_ENABLED=false")
        telegram_manager.broadcast("🔴 BOT_ENABLED=false", config.wallets)
        await telegram_manager.send_queue.join()
        return
    
    telegram_manager.broadcast("✅ تم تشغيل النظام (3 مستويات للكشف)", config.wallets)
    
    stream = OpenSeaStream(STREAM_URL)
    
    try:
        await asyncio.gather(
            stream.listen(),
            watch_loop(),
            poll_new_drops(),      # ✅ مستوى 2
            monitor_contracts(),   # ✅ مستوى 3
        )
    except asyncio.CancelledError:
        log.info("تم الإلغاء")
    finally:
        await telegram_manager.stop()

def main():
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("إيقاف")
            break
        except Exception as e:
            log.critical(f"توقف: {e}")
            time.sleep(3)

if __name__ == "__main__":
    main()
