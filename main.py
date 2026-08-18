"""
النظام الكامل — فحص مسبق + نوم ذكي + شراء فوري + مراقبة المراحل المجانية
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum

import requests
import websockets
from dotenv import load_dotenv
from web3 import Web3

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
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
    max_retries: int = 3
    retry_delay_base: float = 1.0

@dataclass
class WalletConfig:
    wallet: str
    private_key: str
    bot_token: str
    chat_id: str
    chain_key: str = ""
    current_detail: Dict[str, Any] = field(default_factory=dict)

@dataclass
class WatchlistEntry:
    chain_key: str
    detail: Dict[str, Any]
    added_at: float
    twitter_checked: bool = False
    twitter_username: Optional[str] = None
    mint_status: MintStatus = MintStatus.UNKNOWN
    mint_start_time: Optional[int] = None
    mint_end_time: Optional[int] = None
    is_free: bool = False
    start_notified: bool = False
    last_price_check: float = 0
    price_check_count: int = 0

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
        
        self.notify_before_start = 43200  # 12 ساعة
        self.price_check_interval = 60  # ✅ فحص السعر كل 60 ثانية للمينتات المدفوعة
        
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

successful_mints: Dict[str, set] = {}
watchlist: Dict[str, WatchlistEntry] = {}
in_flight: Set[str] = set()
known_slugs: Set[str] = set()

_eth_price_cache = {"value": None, "ts": 0, "ttl": 300}

# ---------------------------------------------------------------------------
# دوال مساعدة
# ---------------------------------------------------------------------------

def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < _eth_price_cache["ttl"]):
        return _eth_price_cache["value"]
    
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8,
        )
        price = float(resp.json()["ethereum"]["usd"])
        _eth_price_cache.update({"value": price, "ts": now})
        return price
    except:
        return _eth_price_cache["value"] or 3000.0

def calculate_price_usd(price_wei: int, eth_price_usd: float) -> float:
    return (price_wei / 1e18) * eth_price_usd

def get_mint_status(w3: Web3, contract_address: str) -> Tuple[MintStatus, Optional[int], Optional[int]]:
    try:
        start_time, end_time = get_mint_times(w3, contract_address)
        current_time = int(time.time())
        
        if start_time and current_time < start_time:
            return MintStatus.NOT_STARTED, start_time, end_time
        if end_time and current_time > end_time:
            return MintStatus.ENDED, start_time, end_time
        if start_time and end_time:
            return MintStatus.ACTIVE, start_time, end_time
        
        return MintStatus.UNKNOWN, start_time, end_time
    except:
        return MintStatus.UNKNOWN, None, None

# ---------------------------------------------------------------------------
# API OpenSea
# ---------------------------------------------------------------------------

class OpenSeaAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.opensea.io/api/v2"
    
    def fetch_drop_detail(self, slug: str) -> Tuple[bool, Optional[Dict]]:
        try:
            resp = requests.get(
                f"{self.base_url}/drops/{slug}",
                headers={"x-api-key": self.api_key},
                timeout=10,
            )
            if resp.status_code == 200:
                return True, resp.json()
            return False, None
        except:
            return None, None

opensea_api = OpenSeaAPI(config.opensea_api_key)

# ---------------------------------------------------------------------------
# تيليجرام
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
                    timeout=10,
                )
                self.send_queue.task_done()
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except:
                await asyncio.sleep(1)

telegram_manager = TelegramManager()

# ---------------------------------------------------------------------------
# رسائل
# ---------------------------------------------------------------------------

def get_opensea_url(detail: dict) -> str:
    url = detail.get("opensea_url", "")
    if url:
        return url
    slug = detail.get("collection_slug") or ""
    if slug:
        return f"https://opensea.io/collection/{slug}"
    return ""

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

def build_start_notification(detail: dict, wait_seconds: int) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug", "Unknown")
    url = get_opensea_url(detail)
    time_str = format_time(wait_seconds)
    
    msg = f"🔔 <b>مينت قادم!</b>\n\nالمجموعة: <b>{name}</b>\nيبدأ خلال: {time_str}"
    if url:
        msg += f"\n\n🔗 <a href='{url}'>OpenSea</a>"
    return msg

def build_free_stage_notification(detail: dict) -> str:
    """✅ إشعار فتح مرحلة مجانية"""
    name = detail.get("collection_name") or detail.get("collection_slug", "Unknown")
    url = get_opensea_url(detail)
    
    msg = f"🎉 <b>فتحت مرحلة مجانية!</b>\n\nالمجموعة: <b>{name}</b>\nجاري الشراء..."
    if url:
        msg += f"\n\n🔗 <a href='{url}'>OpenSea</a>"
    return msg

def build_success_message(wallet_config: WalletConfig, result: dict, detail: dict) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug", "Unknown")
    url = get_opensea_url(detail)
    chain = "Robinhood" if wallet_config.chain_key == "robinhood" else "Ethereum"
    wallet_short = f"{wallet_config.wallet[:6]}...{wallet_config.wallet[-4:]}"
    
    msg = f"✅ <b>تم الشراء!</b> ({chain})\n\nالمحفظة: <code>{wallet_short}</code>\nالمجموعة: <b>{name}</b>\nالكمية: {result.get('quantity', 0)}"
    if url:
        msg += f"\n\n🔗 <a href='{url}'>OpenSea</a>"
    return msg

# ---------------------------------------------------------------------------
# شراء
# ---------------------------------------------------------------------------

async def purchase_for_wallet(
    wallet_config: WalletConfig,
    w3: Web3,
    slug: str,
    contract_address: str,
    price_wei: int,
    max_per_wallet: Optional[int],
    remaining: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> dict:
    wallet_addr = wallet_config.wallet.lower()
    lock = lock_manager.get_lock(wallet_addr)
    
    async with lock:
        try:
            if wallet_addr in successful_mints.get(slug, set()):
                return {"success": False, "wallet": wallet_config.wallet, "reason": "already_bought"}
            
            result = await purchase_with_retry(
                w3=w3,
                private_key=wallet_config.private_key,
                wallet_address=wallet_config.wallet,
                nft_contract=contract_address,
                price_wei_per_token=price_wei,
                max_per_wallet=max_per_wallet,
                remaining_supply=remaining,
                eth_price_usd=eth_price_usd,
                max_gas_fee_usd=max_gas_fee_usd,
                max_retries=3,
            )
            
            if result.get("success"):
                if slug not in successful_mints:
                    successful_mints[slug] = set()
                successful_mints[slug].add(wallet_addr)
                
                message = build_success_message(wallet_config, result, wallet_config.current_detail)
                telegram_manager.enqueue(wallet_config.bot_token, wallet_config.chat_id, message)
            
            return result
        finally:
            lock_manager.release_lock(wallet_addr)

async def buy_immediately(slug: str, chain_key: str, detail: dict):
    """شراء فوري بدون فحص (تم الفحص مسبقاً)"""
    
    stage = detail.get("active_stage")
    if not stage:
        return
    
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    
    if remaining <= 0:
        return
    
    contract_address = detail.get("contract_address")
    if not contract_address:
        return
    
    w3 = w3_instances[chain_key]
    eth_price_usd = get_eth_price_usd()
    
    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
    
    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    
    max_gas_fee_usd = config.chains[chain_key].max_gas_fee_usd
    
    already_bought = successful_mints.get(slug, set())
    pending_wallets = [w for w in config.wallets if w.wallet.lower() not in already_bought]
    
    if not pending_wallets:
        return
    
    for wallet in pending_wallets:
        wallet.chain_key = chain_key
        wallet.current_detail = detail
    
    tasks = [
        purchase_for_wallet(
            wallet_config=wallet,
            w3=w3,
            slug=slug,
            contract_address=contract_address,
            price_wei=price_wei,
            max_per_wallet=max_per_wallet,
            remaining=remaining,
            eth_price_usd=eth_price_usd,
            max_gas_fee_usd=max_gas_fee_usd,
        )
        for wallet in pending_wallets
    ]
    
    await asyncio.gather(*tasks, return_exceptions=True)

# ---------------------------------------------------------------------------
# فحص تويتر
# ---------------------------------------------------------------------------

async def check_twitter(slug: str) -> Tuple[bool, Optional[str]]:
    twitter_username = await asyncio.to_thread(
        get_twitter_username_from_opensea,
        slug,
        config.opensea_api_key
    )
    return bool(twitter_username), twitter_username

# ---------------------------------------------------------------------------
# تقييم المينتات الجديدة
# ---------------------------------------------------------------------------

async def evaluate_new_mint(slug: str, chain_key: str):
    if slug in known_slugs or slug in watchlist or slug in in_flight:
        return
    
    in_flight.add(slug)
    known_slugs.add(slug)
    
    try:
        found, detail = await asyncio.to_thread(opensea_api.fetch_drop_detail, slug)
        
        if not found or not detail or not detail.get("is_minting"):
            return
        
        stage = detail.get("active_stage")
        if not stage:
            return
        
        w3 = w3_instances[chain_key]
        contract_address = detail.get("contract_address")
        
        if not contract_address:
            return
        
        mint_status, start_time, end_time = get_mint_status(w3, contract_address)
        
        # ✅ المينت لم يبدأ - فحص مسبق
        if mint_status == MintStatus.NOT_STARTED:
            wait_seconds = start_time - int(time.time())
            
            if wait_seconds <= config.notify_before_start:
                log.info(f"🔔 '{slug}' سيبدأ خلال {wait_seconds} ثانية - فحص مسبق")
                
                has_twitter, twitter_username = await check_twitter(slug)
                
                if not has_twitter:
                    log.info(f"⏭️ '{slug}' لا يوجد X - تجاهل")
                    return
                
                entry = WatchlistEntry(
                    chain_key=chain_key,
                    detail=detail,
                    added_at=time.time(),
                    twitter_checked=True,
                    twitter_username=twitter_username,
                    mint_status=MintStatus.NOT_STARTED,
                    mint_start_time=start_time,
                    mint_end_time=end_time,
                )
                watchlist[slug] = entry
                
                message = build_start_notification(detail, wait_seconds)
                telegram_manager.broadcast(message, config.wallets)
            else:
                log.info(f"⏰ '{slug}' بعد أكثر من 12 ساعة - تجاهل")
            
            return
        
        # المينت انتهى
        if mint_status == MintStatus.ENDED:
            return
        
        # ✅ المينت نشط
        eth_price_usd = get_eth_price_usd()
        onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
        price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
        price_usd = calculate_price_usd(price_wei, eth_price_usd)
        
        # ✅ مجاني - شراء فوري
        if is_free_or_negligible(price_wei, eth_price_usd):
            log.info(f"🆓 '{slug}' مجاني - شراء")
            
            has_twitter, twitter_username = await check_twitter(slug)
            if not has_twitter:
                log.info(f"⏭️ '{slug}' لا يوجد X")
                return
            
            await buy_immediately(slug, chain_key, detail)
        
        # ⚠️ مدفوع - راقب لاحتمال فتح مرحلة مجانية
        else:
            log.info(f"💰 '{slug}' مدفوع (${price_usd:.4f}) - مراقبة لمرحلة مجانية")
            
            # فحص تويتر مسبقاً
            has_twitter, twitter_username = await check_twitter(slug)
            
            if has_twitter:
                entry = WatchlistEntry(
                    chain_key=chain_key,
                    detail=detail,
                    added_at=time.time(),
                    twitter_checked=True,
                    twitter_username=twitter_username,
                    mint_status=MintStatus.ACTIVE,
                    mint_start_time=start_time,
                    mint_end_time=end_time,
                    is_free=False,
                )
                watchlist[slug] = entry
    
    except Exception as e:
        log.error(f"خطأ '{slug}': {e}")
    finally:
        in_flight.discard(slug)

# ---------------------------------------------------------------------------
# ✅ حلقة المراقبة - نوعان من المراقبة
# ---------------------------------------------------------------------------

async def watch_loop():
    """تراقب المينتات القادمة (نوم ذكي) والمينتات المدفوعة (فحص سعر دوري)"""
    
    while True:
        now = time.time()
        
        # ✅ 1. المينتات التي لم تبدأ - نوم ذكي
        upcoming = [
            (entry.mint_start_time, slug, entry)
            for slug, entry in watchlist.items()
            if entry.mint_status == MintStatus.NOT_STARTED and entry.mint_start_time
        ]
        
        if upcoming:
            upcoming.sort(key=lambda x: x[0])
            next_start_time, next_slug, next_entry = upcoming[0]
            
            sleep_seconds = next_start_time - now - 1
            
            if sleep_seconds > 0:
                log.info(f"😴 '{next_slug}' يبدأ بعد {sleep_seconds} ثانية")
                await asyncio.sleep(sleep_seconds)
            
            # استيقظنا - شراء فوري
            log.info(f"🎉 '{next_slug}' بدأ! - شراء فوري")
            
            try:
                await buy_immediately(next_slug, next_entry.chain_key, next_entry.detail)
            except Exception as e:
                log.error(f"خطأ شراء '{next_slug}': {e}")
            finally:
                watchlist.pop(next_slug, None)
            continue
        
        # ✅ 2. المينتات المدفوعة - فحص سعر دوري
        paid_mints = [
            (slug, entry)
            for slug, entry in watchlist.items()
            if entry.mint_status == MintStatus.ACTIVE and not entry.is_free
        ]
        
        if paid_mints:
            for slug, entry in paid_mints:
                # فحص السعر كل 60 ثانية
                if now - entry.last_price_check < config.price_check_interval:
                    continue
                
                entry.last_price_check = now
                entry.price_check_count += 1
                
                try:
                    w3 = w3_instances[entry.chain_key]
                    contract_address = entry.detail.get("contract_address")
                    
                    if not contract_address:
                        watchlist.pop(slug, None)
                        continue
                    
                    # فحص حالة المينت
                    mint_status, _, _ = get_mint_status(w3, contract_address)
                    
                    if mint_status == MintStatus.ENDED:
                        watchlist.pop(slug, None)
                        continue
                    
                    # فحص السعر
                    eth_price_usd = get_eth_price_usd()
                    onchain_price = await asyncio.to_thread(
                        get_onchain_public_price_wei,
                        w3,
                        contract_address
                    )
                    
                    if onchain_price is None:
                        continue
                    
                    price_wei = onchain_price
                    
                    # ✅ السعر أصبح مجاني!
                    if is_free_or_negligible(price_wei, eth_price_usd):
                        log.info(f"🎉 '{slug}' فتحت مرحلة مجانية! - شراء")
                        
                        # إشعار
                        message = build_free_stage_notification(entry.detail)
                        telegram_manager.broadcast(message, config.wallets)
                        
                        # شراء
                        await buy_immediately(slug, entry.chain_key, entry.detail)
                        
                        # إزالة من المراقبة
                        watchlist.pop(slug, None)
                    
                except Exception as e:
                    log.error(f"خطأ فحص '{slug}': {e}")
        
        # ✅ 3. لا يوجد شيء - نم قليلاً
        await asyncio.sleep(10)

# ---------------------------------------------------------------------------
# OpenSea Stream
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
                async with websockets.connect(self.url, ping_interval=None, open_timeout=15) as ws:
                    log.info(f"متصل بـ OpenSea Stream")
                    
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
                        
                        await self._process_message(raw)
            
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError):
                await asyncio.sleep(self.backoff)
                self.backoff = min(self.backoff * 2, self.max_backoff)
            except Exception as e:
                log.error(f"خطأ: {e}")
                await asyncio.sleep(5)
    
    async def _process_message(self, raw: str):
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
        
        if slug in known_slugs:
            known_slugs.discard(slug)
        
        asyncio.create_task(evaluate_new_mint(slug, chain_key))

# ---------------------------------------------------------------------------
# مساعدة
# ---------------------------------------------------------------------------

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = calculate_price_usd(price_wei, eth_price_usd)
    return price_usd < config.free_price_threshold

# ---------------------------------------------------------------------------
# تشغيل
# ---------------------------------------------------------------------------

async def run():
    log.info(f"بدء التشغيل مع {len(config.wallets)} محافظ")
    
    await telegram_manager.start()
    
    if not config.bot_enabled:
        log.warning("BOT_ENABLED=false")
        telegram_manager.broadcast("🔴 BOT_ENABLED=false", config.wallets)
        await telegram_manager.send_queue.join()
        return
    
    telegram_manager.broadcast("✅ تم تشغيل النظام", config.wallets)
    
    stream = OpenSeaStream(STREAM_URL)
    
    try:
        await asyncio.gather(
            stream.listen(),
            watch_loop(),
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
            time.sleep(5)

if __name__ == "__main__":
    main()
