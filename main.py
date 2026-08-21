"""
النظام الكامل — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها:
  - يكتشف مينتات اليوم على Robinhood + Ethereum
  - يشتري لجميع المحافظ المعرفة بالتوازي (Parallel Execution)
  - يرسل إشعار الشراء أو التحديث لكل محفظة على بوت التيليجرام الخاص بها
  
تحسينات:
- كشف أسرع للمينتات الجديدة عبر تحليل متعدد المصادر
- نظام أولويات للشراء بناءً على سرعة المينت
- إعادة محاولة ذكية مع تبريد متكيف
- معالجة متوازية محسنة
- تخزين مؤقت للنتائج
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_wallet_lock,
    get_wallet_balance_usd,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

# تفكيك المحافظ والمفاتيح وإعدادات التيليجرام
PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]
TELEGRAM_BOT_TOKENS = [t.strip() for t in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if t.strip()]
TELEGRAM_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

if not (len(PRIVATE_KEYS) == len(WALLETS) == len(TELEGRAM_BOT_TOKENS) == len(TELEGRAM_CHAT_IDS)):
    raise ValueError("أعداد المفاتيح، المحافظ، توكنات البوتات، و Chat IDs غير متطابقة في ملف .env!")

# إنشاء هيكلية المحافظ
WALLETS_DATA = []
for i in range(len(WALLETS)):
    WALLETS_DATA.append({
        "wallet": WALLETS[i],
        "private_key": PRIVATE_KEYS[i],
        "bot_token": TELEGRAM_BOT_TOKENS[i],
        "chat_id": TELEGRAM_CHAT_IDS[i],
    })

ALCHEMY_API_KEY_ROBINHOOD = os.environ["ALCHEMY_API_KEY"]
ALCHEMY_API_KEY_ETHEREUM = os.environ["ALCHEMY_API_KEY_ETHEREUM"]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 15

# إعدادات محسنة
MINT_DETECTION_WINDOW_SECONDS = 30
MIN_MINT_INTERVAL_SECONDS = 2
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 5
BALANCE_CHECK_INTERVAL = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

CHAIN_CONFIGS = {
    "robinhood": {
        "stream_chain_name": "robinhood",
        "rpc_url": f"https://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ROBINHOOD}",
        "max_gas_fee_usd": 0.05,
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ETHEREUM}",
        "max_gas_fee_usd": 0.50,
    },
}

W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# تتبع المحافظ التي اشترت بنجاح: slug -> set(wallet_address)
successful_mints: dict[str, set[str]] = {}
watchlist: dict[str, dict] = {}
in_flight: set[str] = set()

# تبريد مؤقت للمجموعات التي رُفضت
REJECTION_COOLDOWN_SECONDS = 120
rejected_cooldown: dict[str, float] = {}

# ==================== تحسينات جديدة ====================

class MintState:
    """حالة متقدمة لتتبع المينتات"""
    def __init__(self, slug: str, chain_key: str, detail: dict):
        self.slug = slug
        self.chain_key = chain_key
        self.detail = detail
        self.first_seen = time.time()
        self.last_attempt = 0
        self.retry_count = 0
        self.bought_wallets: Set[str] = set()
        self.failed_wallets: Dict[str, int] = defaultdict(int)
        self.is_processing = False
        self.is_ready = False
        self.price_wei = None
        self.eth_price_usd = None
        self.contract_address = None
        self.max_per_wallet = None
        self.remaining_supply = None
        
    def can_attempt(self) -> bool:
        if self.is_processing:
            return False
        if self.retry_count >= MAX_RETRY_ATTEMPTS:
            return False
        if time.time() - self.last_attempt < MIN_MINT_INTERVAL_SECONDS:
            return False
        return True
    
    def mark_attempt(self):
        self.last_attempt = time.time()
        self.is_processing = True
        
    def mark_done(self, success: bool = True):
        self.is_processing = False
        if not success:
            self.retry_count += 1

# تخزين المينتات النشطة
active_mints: Dict[str, MintState] = {}
mint_history: List[dict] = []

# تخزين مؤقت للـ Twitter
twitter_cache: Dict[str, tuple] = {}
CACHE_DURATION = 300  # 5 دقائق

# ==================== وظائف مساعدة محسنة ====================

def get_cached_twitter(slug: str) -> Optional[str]:
    """جلب اسم تويتر من التخزين المؤقت"""
    if slug in twitter_cache:
        username, timestamp = twitter_cache[slug]
        if time.time() - timestamp < CACHE_DURATION:
            return username
    return None

def set_cached_twitter(slug: str, username: Optional[str]):
    """تخزين اسم تويتر في التخزين المؤقت"""
    twitter_cache[slug] = (username, time.time())

def is_in_cooldown(slug: str) -> bool:
    ts = rejected_cooldown.get(slug)
    if ts is None:
        return False
    if time.time() - ts >= REJECTION_COOLDOWN_SECONDS:
        rejected_cooldown.pop(slug, None)
        return False
    return True

def mark_rejected(slug: str):
    rejected_cooldown[slug] = time.time()

_eth_price_cache = {"value": None, "ts": 0}

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
        log.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0

def fetch_drop_detail(slug: str):
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            return True, resp.json()
        if resp.status_code == 404:
            return False, None
        return None, None
    except Exception as e:
        log.warning(f"[Drops API] خطأ: {e}")
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

def stage_has_ended(stage: dict) -> bool:
    end = parse_iso(stage.get("end_time", ""))
    if not end:
        return False
    return datetime.now(timezone.utc) > end

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

def analyze_mint_pattern(slug: str) -> dict:
    """تحليل نمط المينتات لتحديد سرعتها"""
    recent_mints = [m for m in mint_history if m.get('slug') == slug]
    if len(recent_mints) < 3:
        return {'frequency': 'unknown', 'avg_interval': 0}
    
    timestamps = [m['timestamp'] for m in recent_mints[-10:]]
    intervals = [timestamps[i] - timestamps[i-1] for i in range(1, len(timestamps))]
    avg_interval = sum(intervals) / len(intervals) if intervals else 0
    
    if avg_interval < 2:
        frequency = 'very_high'
    elif avg_interval < 5:
        frequency = 'high'
    elif avg_interval < 15:
        frequency = 'medium'
    else:
        frequency = 'low'
    
    return {
        'frequency': frequency,
        'avg_interval': avg_interval,
        'sample_size': len(timestamps)
    }

def cleanup_old_mints():
    """تنظيف المينتات القديمة"""
    now = time.time()
    expired = [
        slug for slug, state in active_mints.items()
        if now - state.first_seen > 300
    ]
    for slug in expired:
        active_mints.pop(slug, None)

# ---------------------------------------------------------------------------
# إدارة رسائل التيليجرام
# ---------------------------------------------------------------------------

send_queue: "asyncio.Queue[dict]" = asyncio.Queue()

def enqueue_message(bot_token: str, chat_id: str, text: str):
    send_queue.put_nowait({
        "bot_token": bot_token,
        "chat_id": chat_id,
        "text": text
    })

def broadcast_message(text: str):
    for w in WALLETS_DATA:
        enqueue_message(w["bot_token"], w["chat_id"], text)

async def telegram_sender():
    while True:
        msg = await send_queue.get()
        try:
            telegram_api = f"https://api.telegram.org/bot{msg['bot_token']}"
            await asyncio.to_thread(
                requests.post,
                f"{telegram_api}/sendMessage",
                data={"chat_id": msg["chat_id"], "text": msg["text"], "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception as e:
            log.error(f"خطأ إرسال تليجرام للبوت ({msg['bot_token'][:10]}...): {e}")
        send_queue.task_done()
        await asyncio.sleep(0.1)

def build_single_wallet_success_msg(detail: dict, result: dict, chain_key: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    chain_label = "Robinhood Chain" if chain_key == "robinhood" else "Ethereum Mainnet"
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    return (
        f"✅ <b>تم الشراء بنجاح لمحافظتك!</b> ({chain_label})\n\n"
        f"المحفظة: <code>{w_short}</code>\n"
        f"المجموعة: <b>{name}</b>\n"
        f"الكمية: {result['quantity']}\n"
        f"رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
        f"المعاملة: {result['tx_hash']}\n"
        f"🔗 {url}"
    )

def build_watching_message(detail: dict, reason: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"👀 <b>تحت المراقبة لمحافظتك</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}\nسنحاول الشراء تلقائيًا فور توفر الفرصة."

def build_gaveup_message(detail: dict, reason: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"❌ <b>انتهت الفرصة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}"

# ---------------------------------------------------------------------------
# الشراء المتوازي المحسن
# ---------------------------------------------------------------------------

async def enhanced_purchase_execution(
    mint_state: MintState,
    wallets: List[dict],
    w3,
    max_gas_fee_usd: float
) -> List[dict]:
    """تنفيذ شراء محسن مع أولويات ومعالجة متوازية"""
    
    slug = mint_state.slug
    contract_address = mint_state.contract_address
    price_wei = mint_state.price_wei
    max_per_wallet = mint_state.max_per_wallet
    remaining = mint_state.remaining_supply
    eth_price_usd = mint_state.eth_price_usd
    
    # تحليل نمط المينت
    pattern = analyze_mint_pattern(slug)
    
    # ترتيب المحافظ حسب عدد المحاولات الفاشلة
    sorted_wallets = sorted(
        wallets,
        key=lambda w: mint_state.failed_wallets.get(w['wallet'], 0)
    )
    
    batch_size = min(5, len(sorted_wallets))
    results = []
    
    for i in range(0, len(sorted_wallets), batch_size):
        batch = sorted_wallets[i:i+batch_size]
        tasks = []
        
        for wallet_data in batch:
            wallet_addr = wallet_data['wallet']
            
            if wallet_addr in mint_state.bought_wallets:
                continue
                
            lock = get_wallet_lock(wallet_addr)
            
            async def purchase_with_lock(w_data, lock):
                async with lock:
                    if w_data['wallet'] in mint_state.bought_wallets:
                        return {'success': False, 'wallet': w_data['wallet'], 'reason': 'already_bought'}
                    
                    # التحقق من الرصيد
                    balance_usd = await asyncio.to_thread(
                        get_wallet_balance_usd,
                        w3,
                        w_data['wallet'],
                        eth_price_usd
                    )
                    if balance_usd < 0.10:  # MIN_BALANCE_RESERVE_USD
                        return {'success': False, 'wallet': w_data['wallet'], 'reason': 'insufficient_balance'}
                    
                    res = await asyncio.to_thread(
                        attempt_purchase_single_wallet,
                        w3,
                        w_data['private_key'],
                        w_data['wallet'],
                        contract_address,
                        price_wei,
                        max_per_wallet,
                        remaining,
                        eth_price_usd,
                        max_gas_fee_usd
                    )
                    
                    if res.get('success'):
                        mint_state.bought_wallets.add(w_data['wallet'])
                        msg = build_single_wallet_success_msg(
                            mint_state.detail, 
                            res, 
                            mint_state.chain_key
                        )
                        enqueue_message(
                            w_data['bot_token'],
                            w_data['chat_id'],
                            msg
                        )
                    else:
                        mint_state.failed_wallets[w_data['wallet']] += 1
                    
                    return res
            
            tasks.append(purchase_with_lock(wallet_data, lock))
        
        if tasks:
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in batch_results:
                if isinstance(r, Exception):
                    log.error(f"خطأ في مهمة الشراء: {r}")
                else:
                    results.append(r)
        
        # تبريد حسب سرعة المينت
        if pattern.get('frequency') == 'very_high':
            await asyncio.sleep(0.5)
        elif pattern.get('frequency') == 'high':
            await asyncio.sleep(1)
        else:
            await asyncio.sleep(2)
    
    return results

# ---------------------------------------------------------------------------
# تقييم المينتات المحسن
# ---------------------------------------------------------------------------

async def enhanced_evaluate_new_mint(slug: str, chain_key: str):
    """نسخة محسنة من evaluate_new_mint"""
    
    if is_in_cooldown(slug):
        return
    
    if slug in active_mints:
        mint_state = active_mints[slug]
        if not mint_state.can_attempt():
            return
    else:
        mint_state = MintState(slug, chain_key, {})
        active_mints[slug] = mint_state
    
    mint_state.mark_attempt()
    
    try:
        # 1. جلب تفاصيل المينت
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not found or not detail:
            mint_state.mark_done(success=False)
            mark_rejected(slug)
            return
        
        mint_state.detail = detail
        mint_state.contract_address = detail.get('contract_address')
        
        stage = detail.get('active_stage')
        if not stage or not started_today_local(stage):
            mint_state.mark_done(success=False)
            return
        
        # 2. جلب السعر
        w3 = W3_INSTANCES[chain_key]
        eth_price_usd = get_eth_price_usd()
        mint_state.eth_price_usd = eth_price_usd
        
        if mint_state.contract_address:
            onchain_price = await asyncio.to_thread(
                get_onchain_public_price_wei, 
                w3, 
                mint_state.contract_address
            )
            mint_state.price_wei = (
                onchain_price if onchain_price is not None 
                else int(stage.get('price', '0'))
            )
            
            if not is_free_or_negligible(mint_state.price_wei, eth_price_usd):
                watchlist[slug] = {'chain_key': chain_key, 'detail': detail}
                mint_state.mark_done(success=False)
                return
        
        # 3. التحقق من حساب X مع تخزين مؤقت
        twitter_username = get_cached_twitter(slug)
        if twitter_username is None:
            twitter_username = await asyncio.to_thread(
                get_twitter_username_from_opensea, 
                slug, 
                OPENSEA_API_KEY
            )
            set_cached_twitter(slug, twitter_username)
        
        if not twitter_username:
            log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X مربوط.")
            mint_state.mark_done(success=False)
            mark_rejected(slug)
            return
        
        log.info(f"✅ '{slug}': يوجد حساب X مربوط (@{twitter_username})")
        
        # 4. تحديث بيانات الشراء
        max_supply = int(detail.get('max_supply') or 0)
        total_supply = int(detail.get('total_supply') or 0)
        mint_state.remaining_supply = max_supply - total_supply
        
        if mint_state.remaining_supply <= 0:
            mint_state.mark_done(success=False)
            return
        
        max_per_wallet_raw = stage.get('max_total_mintable_by_wallet') or stage.get('max_per_wallet')
        mint_state.max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
        
        # 5. تنفيذ الشراء
        max_gas_fee_usd = CHAIN_CONFIGS[chain_key]['max_gas_fee_usd']
        
        # تحديد المحافظ المتاحة
        available_wallets = [
            w for w in WALLETS_DATA 
            if w['wallet'] not in mint_state.bought_wallets
        ]
        
        if available_wallets:
            results = await enhanced_purchase_execution(
                mint_state,
                available_wallets,
                w3,
                max_gas_fee_usd
            )
            
            success_count = sum(1 for r in results if r.get('success'))
            if success_count > 0:
                log.info(f"✅ تم شراء {success_count} محفظة من {slug}")
            else:
                log.info(f"⚠️ فشل شراء جميع المحافظ لـ {slug}")
            
            mint_state.mark_done(success=success_count > 0)
            
            # إذا لم تكتمل جميع المحافظ، ضع في المراقبة
            if success_count < len(WALLETS_DATA) and success_count > 0:
                watchlist[slug] = {'chain_key': chain_key, 'detail': detail}
        else:
            mint_state.mark_done(success=True)
            
    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
        mint_state.mark_done(success=False)
    finally:
        cleanup_old_mints()

# ---------------------------------------------------------------------------
# حلقة المراقبة المحسنة
# ---------------------------------------------------------------------------

async def enhanced_watch_loop():
    """نسخة محسنة من watch_loop"""
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        
        if not watchlist:
            continue
        
        for slug in list(watchlist.keys()):
            if slug in active_mints:
                mint_state = active_mints[slug]
                if not mint_state.can_attempt():
                    continue
            
            entry = watchlist.get(slug)
            if not entry:
                continue
            
            try:
                chain_key = entry['chain_key']
                found, fresh_detail = await asyncio.to_thread(fetch_drop_detail, slug)
                
                if not found or not fresh_detail or not fresh_detail.get('is_minting'):
                    watchlist.pop(slug, None)
                    broadcast_message(
                        build_gaveup_message(
                            entry.get('detail', {}), 
                            'المينت لم يعد نشطًا.'
                        )
                    )
                    continue
                
                stage = fresh_detail.get('active_stage')
                if not stage or (stage_has_ended(stage) and not fresh_detail.get('next_stage')):
                    watchlist.pop(slug, None)
                    broadcast_message(
                        build_gaveup_message(
                            fresh_detail, 
                            'انتهت المرحلة.'
                        )
                    )
                    continue
                
                if slug in active_mints:
                    mint_state = active_mints[slug]
                    mint_state.detail = fresh_detail
                    mint_state.mark_attempt()
                    
                    w3 = W3_INSTANCES[chain_key]
                    eth_price_usd = get_eth_price_usd()
                    mint_state.eth_price_usd = eth_price_usd
                    
                    contract_address = fresh_detail.get('contract_address')
                    if contract_address:
                        onchain_price = await asyncio.to_thread(
                            get_onchain_public_price_wei,
                            w3,
                            contract_address
                        )
                        if onchain_price is not None:
                            mint_state.price_wei = onchain_price
                    
                    max_supply = int(fresh_detail.get('max_supply') or 0)
                    total_supply = int(fresh_detail.get('total_supply') or 0)
                    mint_state.remaining_supply = max_supply - total_supply
                    
                    if mint_state.remaining_supply <= 0:
                        watchlist.pop(slug, None)
                        mint_state.mark_done(success=False)
                        continue
                    
                    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]['max_gas_fee_usd']
                    
                    remaining_wallets = [
                        w for w in WALLETS_DATA 
                        if w['wallet'] not in mint_state.bought_wallets
                    ]
                    
                    if remaining_wallets:
                        results = await enhanced_purchase_execution(
                            mint_state,
                            remaining_wallets,
                            w3,
                            max_gas_fee_usd
                        )
                        
                        success_count = sum(1 for r in results if r.get('success'))
                        mint_state.mark_done(success=success_count > 0)
                        
                        if len(mint_state.bought_wallets) >= len(WALLETS_DATA):
                            watchlist.pop(slug, None)
                        else:
                            watchlist[slug] = {'chain_key': chain_key, 'detail': fresh_detail}
                    else:
                        watchlist.pop(slug, None)
                        mint_state.mark_done(success=True)
                        
            except Exception as e:
                log.error(f"خطأ بدورة مراقبة '{slug}': {e}")

# ---------------------------------------------------------------------------
# الاستماع إلى OpenSea المحسن
# ---------------------------------------------------------------------------

async def process_event_buffer(events: List[dict]):
    """معالجة مجموعة من الأحداث المجمعة"""
    grouped = {}
    for event in events:
        slug = event['slug']
        if slug not in grouped:
            grouped[slug] = event
        else:
            if event['timestamp'] > grouped[slug]['timestamp']:
                grouped[slug] = event
    
    for slug, event in grouped.items():
        asyncio.create_task(enhanced_evaluate_new_mint(slug, event['chain_key']))

async def enhanced_listen_opensea():
    """نسخة محسنة من listen_opensea"""
    msg_ref = 0
    event_buffer = []
    last_flush = time.time()
    
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"متصل بـ OpenSea Stream — يراقب لـ {len(WALLETS_DATA)} محافظ.")
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()
                
                while True:
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()
                    
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        if event_buffer and time.time() - last_flush > 5:
                            await process_event_buffer(event_buffer)
                            event_buffer = []
                            last_flush = time.time()
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
                    item = payload.get("item", {}) or {}
                    stream_chain_name = (item.get("chain", {}) or {}).get("name", "")
                    
                    chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
                    if chain_key is None:
                        continue
                    
                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue
                    
                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug:
                        continue
                    
                    # تسجيل في التاريخ
                    mint_history.append({
                        'slug': slug,
                        'timestamp': time.time()
                    })
                    if len(mint_history) > 1000:
                        mint_history = mint_history[-500:]
                    
                    event_buffer.append({
                        'slug': slug,
                        'chain_key': chain_key,
                        'timestamp': time.time(),
                        'payload': payload
                    })
                    
                    if len(event_buffer) >= 5:
                        await process_event_buffer(event_buffer)
                        event_buffer = []
                        last_flush = time.time()
                    
        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"خطأ غير متوقع: {e}.")
            await asyncio.sleep(5)

# ---------------------------------------------------------------------------
# التشغيل الرئيسي
# ---------------------------------------------------------------------------

async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        broadcast_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false).")
        await telegram_sender()
        return
    
    broadcast_message(f"✅ تم تشغيل المحفظة الخاصة بك بنجاح وربطها بهذا البوت!")
    await asyncio.gather(
        enhanced_listen_opensea(), 
        enhanced_watch_loop(), 
        telegram_sender()
    )

def main():
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدويًا.")
            break
        except Exception as e:
            log.critical(f"توقف غير متوقع: {e}.")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break

if __name__ == "__main__":
    main()
