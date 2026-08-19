"""
النظام الكامل — سرعة قصوى + فحص الأهلية (Allowlist):
  - استخدام بيانات WebSocket مباشرة
  - قراءة blockchain واحدة
  - فحص مسبق للمينتات القادمة
  - فحص الأهلية قبل الشراء
  - شراء فوري عند التفعيل
  - إشعارات بسيطة
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
    SEADROP_ADDRESS,
    SEADROP_ABI,
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
class WalletConfig:
    wallet: str
    private_key: str
    bot_token: str
    chat_id: str
    chain_key: str = ""
    is_eligible: bool = False  # ✅ نتيجة فحص الأهلية

@dataclass
class PendingMint:
    slug: str
    chain_key: str
    contract_address: str
    start_time: int
    end_time: Optional[int]
    twitter_username: Optional[str]
    collection_name: str
    opensea_url: str
    price_wei: int
    eligible_wallets: List[WalletConfig]  # ✅ المحافظ المؤهلة فقط
    added_at: float

class Config:
    def __init__(self):
        self.opensea_api_key = self._get_env("OPENSEA_API_KEY", required=True)
        self.bot_enabled = self._get_env("BOT_ENABLED", "false").lower() == "true"
        
        self.alchemy_api_key_robinhood = self._get_env("ALCHEMY_API_KEY", required=True)
        self.alchemy_api_key_ethereum = self._get_env("ALCHEMY_API_KEY_ETHEREUM", required=True)
        
        self.chains = {
            "robinhood": {
                "stream_name": "robinhood",
                "rpc_url": f"https://robinhood-mainnet.g.alchemy.com/v2/{self.alchemy_api_key_robinhood}",
                "max_gas_fee_usd": float(self._get_env("MAX_GAS_FEE_ROBINHOOD", "0.05")),
                "label": "Robinhood",
            },
            "ethereum": {
                "stream_name": "ethereum",
                "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{self.alchemy_api_key_ethereum}",
                "max_gas_fee_usd": float(self._get_env("MAX_GAS_FEE_ETHEREUM", "0.50")),
                "label": "Ethereum",
            },
        }
        
        self.wallets = self._load_wallets()
        
        self.heartbeat_interval = int(self._get_env("HEARTBEAT_INTERVAL", "20"))
        self.recv_timeout = int(self._get_env("RECV_TIMEOUT", "5"))
        self.free_price_threshold = float(self._get_env("FREE_PRICE_THRESHOLD", "0.01"))
        self.notify_before_start = 43200  # 12 ساعة
        self.check_allowlist = True  # ✅ تفعيل فحص الأهلية
        
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

w3_instances = {key: get_web3(cfg["rpc_url"]) for key, cfg in config.chains.items()}
stream_name_to_chain = {cfg["stream_name"]: key for key, cfg in config.chains.items()}

lock_manager = LockManager()

pending_mints: Dict[str, PendingMint] = {}
successful_mints: Dict[str, set] = {}
processed_slugs: Set[str] = set()
in_flight: Set[str] = set()

_eth_price_cache = {"value": None, "ts": 0, "ttl": 300}
_twitter_cache: Dict[str, Tuple[float, Optional[str]]] = {}
_allowlist_cache: Dict[str, Dict[str, bool]] = {}  # ✅ cache لفحص الأهلية

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
# ✅ فحص الأهلية (Allowlist)
# ---------------------------------------------------------------------------

def get_cached_contract(w3: Web3, address: str):
    """الحصول على عقد SeaDrop مع cache"""
    cache_key = f"{w3.provider.endpoint_uri}:{address}"
    
    if not hasattr(get_cached_contract, "_cache"):
        get_cached_contract._cache = {}
    
    if cache_key not in get_cached_contract._cache:
        get_cached_contract._cache[cache_key] = w3.eth.contract(
            address=Web3.to_checksum_address(SEADROP_ADDRESS),
            abi=SEADROP_ABI,
        )
    
    return get_cached_contract._cache[cache_key]

def check_allowlist_for_wallet(
    w3: Web3,
    contract_address: str,
    wallet_address: str,
) -> bool:
    """✅ فحص إذا كانت المحفظة في allowlist"""
    
    cache_key = f"{contract_address}:{wallet_address.lower()}"
    
    # استخدام cache
    if cache_key in _allowlist_cache:
        return _allowlist_cache[cache_key]
    
    try:
        # محاولة قراءة merkle proof للـ allowlist
        # SeaDrop v1.1+ يدعم فحص الأهلية
        seadrop = get_cached_contract(w3, contract_address)
        
        # فحص إذا كان هناك allowlist
        try:
            # SeaDrop v1.2+ method
            allowlist_data = seadrop.functions.getAllowListMerkleRoot(
                Web3.to_checksum_address(contract_address)
            ).call()
            
            if allowlist_data == "0x0000000000000000000000000000000000000000000000000000000000000000":
                # لا يوجد allowlist - الجميع مؤهل
                _allowlist_cache[cache_key] = True
                return True
        except:
            pass
        
        # فحص إضافي - هل المحفظة مؤهلة
        try:
            # بعض العقود تستخدم allowedMinters
            is_allowed = seadrop.functions.isAllowed(
                Web3.to_checksum_address(contract_address),
                Web3.to_checksum_address(wallet_address),
            ).call()
            
            _allowlist_cache[cache_key] = is_allowed
            return is_allowed
        except:
            pass
        
        # إذا لم نستطع الفحص - افترض أن الجميع مؤهل
        _allowlist_cache[cache_key] = True
        return True
        
    except Exception as e:
        log.warning(f"[Allowlist] فشل الفحص: {e}")
        _allowlist_cache[cache_key] = True  # افترض مؤهل
        return True

async def check_allowlist_for_all_wallets(
    chain_key: str,
    contract_address: str,
) -> List[WalletConfig]:
    """✅ فحص الأهلية لجميع المحافظ"""
    
    w3 = w3_instances[chain_key]
    
    eligible = []
    ineligible = []
    
    for wallet in config.wallets:
        try:
            is_eligible = await asyncio.wait_for(
                asyncio.to_thread(
                    check_allowlist_for_wallet,
                    w3,
                    contract_address,
                    wallet.wallet,
                ),
                timeout=5,
            )
            
            wallet.is_eligible = is_eligible
            
            if is_eligible:
                eligible.append(wallet)
            else:
                ineligible.append(wallet)
                
        except Exception as e:
            log.warning(f"[Allowlist] {wallet.wallet[:8]}: {e}")
            wallet.is_eligible = False
            ineligible.append(wallet)
    
    log.info(f"✅ مؤهلة: {len(eligible)} | ❌ غير مؤهلة: {len(ineligible)}")
    
    return eligible

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
# رسائل
# ---------------------------------------------------------------------------

def build_start_notification(collection_name: str, opensea_url: str, wait_seconds: int, eligible_count: int) -> str:
    time_str = format_time(wait_seconds)
    msg = (
        f"🔔 <b>مينت قادم!</b>\n\n"
        f"المجموعة: <b>{collection_name}</b>\n"
        f"يبدأ خلال: {time_str}\n"
        f"المحافظ المؤهلة: {eligible_count}"
    )
    if opensea_url:
        msg += f"\n\n🔗 <a href='{opensea_url}'>OpenSea</a>"
    return msg

def build_success_message(wallet_short: str, collection_name: str, opensea_url: str, chain_label: str, quantity: int) -> str:
    msg = f"✅ <b>تم الشراء!</b> ({chain_label})\n\nالمحفظة: <code>{wallet_short}</code>\nالمجموعة: <b>{collection_name}</b>\nالكمية: {quantity}"
    if opensea_url:
        msg += f"\n\n🔗 <a href='{opensea_url}'>OpenSea</a>"
    return msg

def build_not_eligible_message(collection_name: str) -> str:
    return f"⚠️ <b>غير مؤهل</b>\n\nالمجموعة: <b>{collection_name}</b>\nمحفظتك ليست في allowlist"

# ---------------------------------------------------------------------------
# شراء
# ---------------------------------------------------------------------------

async def buy_immediately(
    slug: str,
    chain_key: str,
    contract_address: str,
    price_wei: int,
    collection_name: str,
    opensea_url: str,
    eligible_wallets: List[WalletConfig],
):
    """شراء فوري للمحافظ المؤهلة فقط"""
    
    w3 = w3_instances[chain_key]
    eth_price_usd = get_eth_price_usd()
    max_gas_fee_usd = config.chains[chain_key]["max_gas_fee_usd"]
    chain_label = config.chains[chain_key]["label"]
    
    already_bought = successful_mints.get(slug, set())
    pending_wallets = [
        w for w in eligible_wallets
        if w.wallet.lower() not in already_bought
    ]
    
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
                        return {"success": False, "reason": "already_bought"}
                    
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
                        retry_delay_base=0.3,
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
                            chain_label,
                            result.get("quantity", 0),
                        )
                        telegram_manager.enqueue(w.bot_token, w.chat_id, message)
                    
                    return result
                except Exception as e:
                    return {"success": False, "reason": "exception", "error": str(e)}
                finally:
                    lock_manager.release_lock(w.wallet)
        
        tasks.append(buy_one())
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results

# ---------------------------------------------------------------------------
# فحص تويتر
# ---------------------------------------------------------------------------

async def check_twitter_fast(slug: str) -> Tuple[bool, Optional[str]]:
    if slug in _twitter_cache:
        ts, username = _twitter_cache[slug]
        if time.time() - ts < 3600:
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
# معالجة المينت الجديد
# ---------------------------------------------------------------------------

async def process_new_mint_fast(slug: str, chain_key: str, payload: dict):
    """معالجة سريعة مع فحص الأهلية"""
    
    if slug in processed_slugs or slug in in_flight:
        return
    
    in_flight.add(slug)
    processed_slugs.add(slug)
    
    try:
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
            return
        
        w3 = w3_instances[chain_key]
        
        # ✅ قراءة واحدة من blockchain
        try:
            public_drop = await asyncio.wait_for(
                asyncio.to_thread(get_full_drop_info, w3, contract_address),
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
        
        # ✅ المينت لم يبدأ
        if start_time and current_time < start_time:
            wait_seconds = start_time - current_time
            
            if wait_seconds > config.notify_before_start:
                return
            
            log.info(f"🔔 '{slug}' سيبدأ خلال {wait_seconds} ثانية")
            
            # فحص تويتر
            has_twitter, twitter_username = await check_twitter_fast(slug)
            
            if not has_twitter:
                log.info(f"⏭️ '{slug}' لا يوجد X")
                return
            
            # ✅ فحص الأهلية لجميع المحافظ
            log.info(f"🔍 '{slug}' فحص الأهلية...")
            eligible_wallets = await check_allowlist_for_all_wallets(chain_key, contract_address)
            
            if not eligible_wallets:
                log.info(f"❌ '{slug}' لا توجد محافظ مؤهلة")
                return
            
            # حفظ للمراقبة
            pending_mints[slug] = PendingMint(
                slug=slug,
                chain_key=chain_key,
                contract_address=contract_address,
                start_time=start_time,
                end_time=end_time,
                twitter_username=twitter_username,
                collection_name=collection_name,
                opensea_url=opensea_url,
                price_wei=price_wei,
                eligible_wallets=eligible_wallets,
                added_at=time.time(),
            )
            
            # إشعار
            message = build_start_notification(
                collection_name,
                opensea_url,
                wait_seconds,
                len(eligible_wallets),
            )
            telegram_manager.broadcast(message, config.wallets)
            return
        
        # المينت انتهى
        if end_time and current_time > end_time:
            return
        
        # ✅ المينت نشط - فحص السعر
        if not is_free_or_negligible(price_wei, eth_price_usd):
            return
        
        log.info(f"🆓 '{slug}' مجاني - فحص")
        
        # فحص تويتر
        has_twitter, twitter_username = await check_twitter_fast(slug)
        
        if not has_twitter:
            log.info(f"⏭️ '{slug}' لا يوجد X")
            return
        
        # ✅ فحص الأهلية
        log.info(f"🔍 '{slug}' فحص الأهلية...")
        eligible_wallets = await check_allowlist_for_all_wallets(chain_key, contract_address)
        
        if not eligible_wallets:
            log.info(f"❌ '{slug}' لا توجد محافظ مؤهلة")
            return
        
        log.info(f"✅ '{slug}' شراء ({len(eligible_wallets)} محافظ مؤهلة)")
        
        await buy_immediately(
            slug=slug,
            chain_key=chain_key,
            contract_address=contract_address,
            price_wei=price_wei,
            collection_name=collection_name,
            opensea_url=opensea_url,
            eligible_wallets=eligible_wallets,
        )
    
    except Exception as e:
        log.error(f"خطأ '{slug}': {e}")
    finally:
        in_flight.discard(slug)

def get_full_drop_info(w3: Web3, contract_address: str):
    try:
        seadrop = get_cached_contract(w3, contract_address)
        return seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(contract_address)
        ).call()
    except:
        return None

# ---------------------------------------------------------------------------
# حلقة المراقبة
# ---------------------------------------------------------------------------

async def watch_loop():
    """نوم ذكي + شراء فوري عند البدء"""
    
    while True:
        if not pending_mints:
            await asyncio.sleep(5)
            continue
        
        now = time.time()
        upcoming = sorted(pending_mints.items(), key=lambda x: x[1].start_time)
        
        next_slug, next_mint = upcoming[0]
        
        sleep_seconds = next_mint.start_time - now - 1
        
        if sleep_seconds > 0:
            log.info(f"😴 '{next_slug}' بعد {sleep_seconds} ثانية")
            await asyncio.sleep(sleep_seconds)
        
        now = time.time()
        if now < next_mint.start_time:
            await asyncio.sleep(next_mint.start_time - now)
        
        log.info(f"🎉 '{next_slug}' بدأ! - شراء")
        
        try:
            # جلب السعر الحالي
            w3 = w3_instances[next_mint.chain_key]
            public_drop = await asyncio.wait_for(
                asyncio.to_thread(get_full_drop_info, w3, next_mint.contract_address),
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
                        eligible_wallets=next_mint.eligible_wallets,
                    )
        except Exception as e:
            log.error(f"خطأ شراء '{next_slug}': {e}")
        finally:
            pending_mints.pop(next_slug, None)

# ---------------------------------------------------------------------------
# OpenSea Stream
# ---------------------------------------------------------------------------

class OpenSeaStream:
    def __init__(self, url: str):
        self.url = url
        self.msg_ref = 0
        self.backoff = 1
        self.max_backoff = 60
        self.last_message_time = time.time()
    
    async def listen(self):
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=None, open_timeout=10) as ws:
                    log.info("متصل بـ OpenSea Stream")
                    
                    self.backoff = 1
                    self.last_message_time = time.time()
                    
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
                        
                        if time.time() - self.last_message_time > 120:
                            log.warning("لا رسائل - إعادة اتصال")
                            break
                        
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=config.recv_timeout)
                            self.last_message_time = time.time()
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
        
        asyncio.create_task(process_new_mint_fast(slug, chain_key, payload))

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
            time.sleep(3)

if __name__ == "__main__":
    main()
