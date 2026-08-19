"""
النظام النهائي — أقصى سرعة وأمان + إشعار موحد في بوت واحد
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
    chain_key: str = ""

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
    max_supply: int
    added_at: float

class Config:
    def __init__(self):
        self.opensea_api_key = self._get_env("OPENSEA_API_KEY", required=True)
        self.bot_enabled = self._get_env("BOT_ENABLED", "false").lower() == "true"
        
        self.alchemy_api_key_robinhood = self._get_env("ALCHEMY_API_KEY", required=True)
        self.alchemy_api_key_ethereum = self._get_env("ALCHEMY_API_KEY_ETHEREUM", required=True)
        
        # ✅ بوت تيليجرام واحد للإشعارات
        self.telegram_bot_token = self._get_env("TELEGRAM_BOT_TOKEN", required=True)
        self.telegram_chat_id = self._get_env("TELEGRAM_CHAT_ID", required=True)
        
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
        
    def _get_env(self, key: str, default: str = "", required: bool = False) -> str:
        value = os.environ.get(key, default).strip()
        if required and not value:
            raise ValueError(f"المتغير {key} مطلوب!")
        return value
    
    def _load_wallets(self) -> List[WalletConfig]:
        private_keys = [k.strip() for k in self._get_env("PRIVATE_KEYS", required=True).split(",") if k.strip()]
        wallets = [w.strip() for w in self._get_env("WALLETS", required=True).split(",") if w.strip()]
        
        if len(private_keys) != len(wallets):
            raise ValueError("أعداد المفاتيح والمحافظ غير متطابقة!")
        
        return [
            WalletConfig(
                wallet=wallets[i],
                private_key=private_keys[i],
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

pending_mints: Dict[str, PendingMint] = {}
successful_mints: Dict[str, set] = {}
processed_slugs: Set[str] = set()
in_flight: Set[str] = set()

_eth_price_cache = {"value": None, "ts": 0, "ttl": 300}
_twitter_cache: Dict[str, Tuple[float, Optional[str]]] = {}
_twitter_cache_ttl = 3600
_last_cleanup = time.time()

# ---------------------------------------------------------------------------
# ✅ إرسال إشعار موحد لبوت واحد
# ---------------------------------------------------------------------------

def send_telegram_notification(text: str):
    """إرسال إشعار للبوت الواحد"""
    try:
        telegram_api = f"https://api.telegram.org/bot{config.telegram_bot_token}"
        response = requests.post(
            f"{telegram_api}/sendMessage",
            data={
                "chat_id": config.telegram_chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": False,
            },
            timeout=10,
        )
        
        if response.status_code == 200:
            log.info("✅ تم إرسال الإشعار")
        else:
            log.error(f"❌ فشل إرسال الإشعار: {response.status_code} - {response.text[:100]}")
        
        return response.status_code == 200
    except Exception as e:
        log.error(f"❌ خطأ إرسال الإشعار: {e}")
        return False

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

def cleanup_caches():
    global _last_cleanup
    now = time.time()
    
    if now - _last_cleanup < 3600:
        return
    
    _last_cleanup = now
    
    expired = [s for s, (ts, _) in _twitter_cache.items() if now - ts > _twitter_cache_ttl]
    for s in expired:
        _twitter_cache.pop(s, None)
    
    if len(processed_slugs) > 1000:
        processed_slugs.clear()

# ---------------------------------------------------------------------------
# ✅ بناء الإشعارات الموحدة
# ---------------------------------------------------------------------------

def build_start_notification(mint: PendingMint, wait_seconds: int) -> str:
    """إشعار بدء موحد مع كل المعلومات"""
    time_str = format_time(wait_seconds)
    
    msg = (
        f"🔔 <b>مينت قادم!</b>\n\n"
        f"📦 <b>المجموعة:</b> {mint.collection_name}\n"
        f"⏰ <b>يبدأ خلال:</b> {time_str}\n"
        f"🔗 <b>السلسلة:</b> {'Robinhood' if mint.chain_key == 'robinhood' else 'Ethereum'}\n"
        f"👛 <b>عدد المحافظ:</b> {len(config.wallets)}\n"
    )
    
    if mint.opensea_url:
        msg += f"\n🔗 <a href='{mint.opensea_url}'>OpenSea</a>"
    
    return msg

def build_buy_summary_notification(
    mint: PendingMint,
    results: List[dict],
) -> str:
    """إشعار ملخص الشراء مع تفاصيل كل المحافظ"""
    
    chain_label = "Robinhood" if mint.chain_key == "robinhood" else "Ethereum"
    
    success_count = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
    failed_count = len(results) - success_count
    
    msg = (
        f"📊 <b>ملخص الشراء</b>\n\n"
        f"📦 <b>المجموعة:</b> {mint.collection_name}\n"
        f"🔗 <b>السلسلة:</b> {chain_label}\n"
        f"✅ <b>نجح:</b> {success_count}/{len(results)}\n"
        f"❌ <b>فشل:</b> {failed_count}/{len(results)}\n\n"
        f"<b>تفاصيل المحافظ:</b>\n"
    )
    
    for i, r in enumerate(results):
        if not isinstance(r, dict):
            continue
        
        wallet = r.get("wallet", "Unknown")
        wallet_short = f"{wallet[:6]}...{wallet[-4:]}" if len(wallet) > 10 else wallet
        
        if r.get("success"):
            msg += f"✅ <code>{wallet_short}</code> - {r.get('quantity', 0)} NFT\n"
        else:
            reason = r.get("reason", "unknown")
            msg += f"❌ <code>{wallet_short}</code> - {reason}\n"
    
    if mint.opensea_url:
        msg += f"\n🔗 <a href='{mint.opensea_url}'>OpenSea</a>"
    
    return msg

def build_simple_success_notification(mint: PendingMint, success_count: int, total: int) -> str:
    """إشعار نجاح بسيط"""
    chain_label = "Robinhood" if mint.chain_key == "robinhood" else "Ethereum"
    
    msg = (
        f"✅ <b>تم الشراء!</b>\n\n"
        f"📦 <b>المجموعة:</b> {mint.collection_name}\n"
        f"🔗 <b>السلسلة:</b> {chain_label}\n"
        f"✅ <b>نجح:</b> {success_count}/{total} محافظ\n"
    )
    
    if mint.opensea_url:
        msg += f"\n🔗 <a href='{mint.opensea_url}'>OpenSea</a>"
    
    return msg

# ---------------------------------------------------------------------------
# ✅ فحص تويتر
# ---------------------------------------------------------------------------

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
# ✅ شراء
# ---------------------------------------------------------------------------

async def buy_immediately(
    mint: PendingMint,
    max_retries: int = 2,
) -> List[dict]:
    """شراء فوري لجميع المحافظ مع إشعار موحد"""
    
    w3 = w3_instances[mint.chain_key]
    eth_price_usd = get_eth_price_usd()
    max_gas_fee_usd = config.chains[mint.chain_key].max_gas_fee_usd
    
    all_results = []
    
    for attempt in range(max_retries):
        already_bought = successful_mints.get(mint.slug, set())
        pending_wallets = [w for w in config.wallets if w.wallet.lower() not in already_bought]
        
        if not pending_wallets:
            break
        
        tasks = []
        for wallet in pending_wallets:
            wallet.chain_key = mint.chain_key
            lock = lock_manager.get_lock(wallet.wallet)
            
            async def buy_one(w=wallet, l=lock):
                async with l:
                    try:
                        if w.wallet.lower() in successful_mints.get(mint.slug, set()):
                            return {"success": False, "wallet": w.wallet, "reason": "already_bought"}
                        
                        result = await purchase_with_retry(
                            w3=w3,
                            private_key=w.private_key,
                            wallet_address=w.wallet,
                            nft_contract=mint.contract_address,
                            price_wei_per_token=0,
                            max_per_wallet=None,
                            remaining_supply=1000,
                            eth_price_usd=eth_price_usd,
                            max_gas_fee_usd=max_gas_fee_usd,
                            max_retries=1,
                            retry_delay_base=0.3,
                        )
                        
                        result["wallet"] = w.wallet
                        
                        if result.get("success"):
                            if mint.slug not in successful_mints:
                                successful_mints[mint.slug] = set()
                            successful_mints[mint.slug].add(w.wallet.lower())
                            log.info(f"✅ شراء ناجح: {w.wallet[:8]}...")
                        else:
                            log.warning(f"❌ فشل {w.wallet[:8]}: {result.get('reason')}")
                        
                        return result
                    except Exception as e:
                        return {"success": False, "wallet": w.wallet, "reason": "exception", "error": str(e)}
                    finally:
                        lock_manager.release_lock(w.wallet)
            
            tasks.append(buy_one())
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # معالجة النتائج
        processed = []
        for r in results:
            if isinstance(r, dict):
                processed.append(r)
            else:
                processed.append({"success": False, "reason": "exception"})
        
        all_results.extend(processed)
        
        # إذا نجحت كل المحافظ - توقف
        if len(successful_mints.get(mint.slug, set())) >= len(config.wallets):
            break
        
        # إعادة محاولة للمحافظ الفاشلة
        if attempt < max_retries - 1:
            failed = [r for r in processed if not r.get("success")]
            if failed:
                log.info(f"🔄 إعادة محاولة {len(failed)} محافظ")
                await asyncio.sleep(1)
    
    # ✅ إرسال إشعار موحد بعد الشراء
    success_count = len(successful_mints.get(mint.slug, set()))
    total = len(config.wallets)
    
    if success_count > 0:
        # إشعار ملخص كامل
        message = build_buy_summary_notification(mint, all_results)
        send_telegram_notification(message)
    
    return all_results

# ---------------------------------------------------------------------------
# ✅ معالجة المينت الجديد
# ---------------------------------------------------------------------------

async def process_new_mint_fast(slug: str, chain_key: str, payload: dict):
    """معالجة سريعة للمينت الجديد"""
    
    if slug in processed_slugs or slug in in_flight:
        return
    
    in_flight.add(slug)
    processed_slugs.add(slug)
    
    try:
        log.info(f"🔍 معالجة '{slug}'...")
        
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
            found, detail = await asyncio.to_thread(fetch_drop_detail_fast, slug)
            if not found or not detail:
                return
            contract_address = detail.get("contract_address", "")
            collection_name = detail.get("collection_name") or collection_name
            opensea_url = detail.get("opensea_url") or opensea_url
        
        if not contract_address:
            return
        
        w3 = w3_instances[chain_key]
        
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
        
        # إنشاء كائن PendingMint
        mint = PendingMint(
            slug=slug,
            chain_key=chain_key,
            contract_address=contract_address,
            start_time=start_time,
            end_time=end_time,
            twitter_username=None,
            collection_name=collection_name,
            opensea_url=opensea_url,
            max_supply=0,
            added_at=time.time(),
        )
        
        # المينت لم يبدأ
        if start_time and current_time < start_time:
            wait_seconds = start_time - current_time
            
            if wait_seconds > config.notify_before_start:
                return
            
            log.info(f"🔔 '{slug}' سيبدأ خلال {wait_seconds} ثانية")
            
            has_twitter, twitter_username = await check_twitter_fast(slug)
            
            if not has_twitter:
                log.info(f"⏭️ '{slug}' لا يوجد X")
                return
            
            mint.twitter_username = twitter_username
            
            pending_mints[slug] = mint
            
            # ✅ إشعار موحد
            message = build_start_notification(mint, wait_seconds)
            send_telegram_notification(message)
            return
        
        # المينت انتهى
        if end_time and current_time > end_time:
            return
        
        # ✅ المينت نشط - فحص السعر
        if not is_free_or_negligible(price_wei, eth_price_usd):
            return
        
        log.info(f"🆓 '{slug}' مجاني - فحص X")
        
        has_twitter, twitter_username = await check_twitter_fast(slug)
        
        if not has_twitter:
            log.info(f"⏭️ '{slug}' لا يوجد X")
            return
        
        mint.twitter_username = twitter_username
        
        log.info(f"✅ '{slug}' شراء")
        
        await buy_immediately(mint)
    
    except Exception as e:
        log.error(f"خطأ '{slug}': {e}")
    finally:
        in_flight.discard(slug)

def fetch_drop_detail_fast(slug: str) -> Tuple[bool, Optional[Dict]]:
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
    try:
        from buyer import SEADROP_ADDRESS, SEADROP_ABI
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        return seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(contract_address)
        ).call()
    except:
        return None

# ---------------------------------------------------------------------------
# ✅ حلقة المراقبة
# ---------------------------------------------------------------------------

async def watch_loop():
    """نوم ذكي + شراء فوري عند البدء"""
    
    while True:
        cleanup_caches()
        
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
            # فحص سريع
            has_twitter, _ = await check_twitter_fast(next_slug)
            
            if not has_twitter:
                pending_mints.pop(next_slug, None)
                continue
            
            w3 = w3_instances[next_mint.chain_key]
            public_drop = await asyncio.wait_for(
                asyncio.to_thread(get_full_drop_info_fast, w3, next_mint.contract_address),
                timeout=3,
            )
            
            if public_drop:
                price_wei = public_drop[0]
                eth_price_usd = get_eth_price_usd()
                
                if is_free_or_negligible(price_wei, eth_price_usd):
                    await buy_immediately(next_mint)
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
        self.last_message_time = time.time()
    
    async def listen(self):
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=None, open_timeout=10) as ws:
                    log.info("✅ متصل بـ OpenSea Stream")
                    
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
# ✅ تشغيل
# ---------------------------------------------------------------------------

async def run():
    log.info(f"🚀 بدء التشغيل مع {len(config.wallets)} محافظ")
    
    # ✅ إشعار التشغيل
    send_telegram_notification(
        f"✅ <b>تم تشغيل النظام</b>\n\n"
        f"👛 المحافظ: {len(config.wallets)}\n"
        f"🔗 السلاسل: Robinhood + Ethereum"
    )
    
    if not config.bot_enabled:
        log.warning("BOT_ENABLED=false")
        send_telegram_notification("🔴 BOT_ENABLED=false")
        return
    
    stream = OpenSeaStream(STREAM_URL)
    
    try:
        await asyncio.gather(
            stream.listen(),
            watch_loop(),
        )
    except asyncio.CancelledError:
        log.info("تم الإلغاء")

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
