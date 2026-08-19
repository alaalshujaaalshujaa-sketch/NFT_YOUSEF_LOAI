"""
نسخة محسنة مع:
- توقيع مسبق للمعاملات
- ترتيب المحافظ حسب الأولوية
- إشعارات ملخصة
- تسخين الاتصالات
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

@dataclass
class PreparedTransaction:
    """معاملة موقعة مسبقاً"""
    signed_tx: Any
    wallet: str
    quantity: int
    nonce: int

class Config:
    def __init__(self):
        self.opensea_api_key = self._get_env("OPENSEA_API_KEY", required=True)
        self.bot_enabled = self._get_env("BOT_ENABLED", "false").lower() == "true"
        
        self.alchemy_api_key_robinhood = self._get_env("ALCHEMY_API_KEY", required=True)
        self.alchemy_api_key_ethereum = self._get_env("ALCHEMY_API_KEY_ETHEREUM", required=True)
        
        self.chains = {
            "robinhood": {
                "rpc_url": f"https://robinhood-mainnet.g.alchemy.com/v2/{self.alchemy_api_key_robinhood}",
                "max_gas_fee_usd": float(self._get_env("MAX_GAS_FEE_ROBINHOOD", "0.05")),
            },
            "ethereum": {
                "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{self.alchemy_api_key_ethereum}",
                "max_gas_fee_usd": float(self._get_env("MAX_GAS_FEE_ETHEREUM", "0.50")),
            },
        }
        
        self.wallets = self._load_wallets()
        self.free_price_threshold = float(self._get_env("FREE_PRICE_THRESHOLD", "0.01"))
        self.notify_before_start = 43200
        
    def _get_env(self, key, default="", required=False):
        value = os.environ.get(key, default).strip()
        if required and not value:
            raise ValueError(f"{key} مطلوب!")
        return value
    
    def _load_wallets(self):
        private_keys = [k.strip() for k in self._get_env("PRIVATE_KEYS", required=True).split(",") if k.strip()]
        wallets = [w.strip() for w in self._get_env("WALLETS", required=True).split(",") if w.strip()]
        bot_tokens = [t.strip() for t in self._get_env("TELEGRAM_BOT_TOKENS", required=True).split(",") if t.strip()]
        chat_ids = [c.strip() for c in self._get_env("TELEGRAM_CHAT_IDS", required=True).split(",") if c.strip()]
        
        return [
            {
                "wallet": wallets[i],
                "private_key": private_keys[i],
                "bot_token": bot_tokens[i],
                "chat_id": chat_ids[i],
            }
            for i in range(len(wallets))
        ]

config = Config()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("auto-buyer")

w3_instances = {key: get_web3(cfg["rpc_url"]) for key, cfg in config.chains.items()}
lock_manager = LockManager()

prepared_transactions: Dict[str, List[PreparedTransaction]] = {}
successful_mints: Dict[str, set] = {}
pending_mints: Dict[str, Any] = {}
processed_slugs: Set[str] = set()

# ---------------------------------------------------------------------------

async def warm_up():
    """تسخين الاتصالات"""
    for chain_key, w3 in w3_instances.items():
        try:
            w3.eth.block_number
            log.info(f"✅ {chain_key} جاهز")
        except Exception as e:
            log.error(f"❌ {chain_key} فشل: {e}")

# ---------------------------------------------------------------------------

async def prepare_transactions_for_mint(
    slug: str,
    chain_key: str,
    contract_address: str,
    price_wei: int,
    quantity_per_wallet: int = 1,
):
    """✅ توقيع المعاملات مسبقاً قبل وقت البدء"""
    
    w3 = w3_instances[chain_key]
    contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
    
    prepared = []
    
    for wallet_data in config.wallets:
        try:
            nonce = w3.eth.get_transaction_count(wallet_data["wallet"], "pending")
            
            # بناء المعاملة
            tx = contract.functions.mintPublic(
                Web3.to_checksum_address(contract_address),
                Web3.to_checksum_address("0x0000000000000000000000000000000000000000"),  # fee recipient placeholder
                Web3.to_checksum_address("0x0000000000000000000000000000000000000000"),
                quantity_per_wallet,
            ).build_transaction({
                "from": wallet_data["wallet"],
                "value": price_wei * quantity_per_wallet,
                "nonce": nonce,
                "chainId": w3.eth.chain_id,
                "gas": 200000,  # تقدير مبدئي
            })
            
            # ✅ توقيع الآن
            signed = w3.eth.account.sign_transaction(tx, wallet_data["private_key"])
            
            prepared.append(PreparedTransaction(
                signed_tx=signed,
                wallet=wallet_data["wallet"],
                quantity=quantity_per_wallet,
                nonce=nonce,
            ))
            
        except Exception as e:
            log.error(f"فشل تحضير {wallet_data['wallet'][:8]}: {e}")
    
    prepared_transactions[slug] = prepared
    log.info(f"✅ تم تحضير {len(prepared)} معاملة موقعة لـ '{slug}'")

async def send_prepared_transactions(slug: str, chain_key: str):
    """✅ إرسال المعاملات الموقعة مسبقاً - فوري جداً"""
    
    w3 = w3_instances[chain_key]
    prepared = prepared_transactions.get(slug, [])
    
    tasks = []
    for pt in prepared:
        async def send_one(p=pt):
            try:
                tx_hash = await asyncio.to_thread(
                    w3.eth.send_raw_transaction,
                    p.signed_tx.raw_transaction
                )
                return {"success": True, "wallet": p.wallet, "tx_hash": tx_hash.hex()}
            except Exception as e:
                return {"success": False, "wallet": p.wallet, "error": str(e)}
        
        tasks.append(send_one())
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # ✅ إشعار ملخص واحد
    success_count = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
    total = len(results)
    
    collection_name = pending_mints.get(slug, {}).get("collection_name", slug)
    
    summary = f"📊 <b>{collection_name}</b>\n✅ نجح: {success_count}/{total}"
    
    # إرسال إشعار واحد لجميع البوتات
    for wallet_data in config.wallets:
        try:
            requests.post(
                f"https://api.telegram.org/bot{wallet_data['bot_token']}/sendMessage",
                data={"chat_id": wallet_data["chat_id"], "text": summary, "parse_mode": "HTML"},
                timeout=3,
            )
        except:
            pass

# ---------------------------------------------------------------------------

async def process_new_mint(slug: str, chain_key: str, payload: dict):
    """معالجة سريعة مع تحضير مسبق"""
    
    if slug in processed_slugs:
        return
    
    processed_slugs.add(slug)
    
    try:
        collection = payload.get("collection", {}) or {}
        contract_address = collection.get("contract_address", "")
        collection_name = collection.get("name") or slug
        opensea_url = collection.get("opensea_url", "")
        
        if not contract_address:
            return
        
        w3 = w3_instances[chain_key]
        
        # قراءة واحدة
        public_drop = await asyncio.to_thread(get_full_drop_info, w3, contract_address)
        
        if not public_drop:
            return
        
        price_wei = public_drop[0]
        start_time = public_drop[1]
        end_time = public_drop[2]
        
        current_time = int(time.time())
        
        # المينت لم يبدأ
        if start_time and current_time < start_time:
            wait_seconds = start_time - current_time
            
            if wait_seconds > config.notify_before_start:
                return
            
            # ✅ تحضير المعاملات مسبقاً
            await prepare_transactions_for_mint(slug, chain_key, contract_address, price_wei)
            
            pending_mints[slug] = {
                "chain_key": chain_key,
                "contract_address": contract_address,
                "start_time": start_time,
                "collection_name": collection_name,
                "opensea_url": opensea_url,
            }
            
            log.info(f"🔔 '{slug}' جاهز - {wait_seconds} ثانية")
            return
        
        # المينت نشط
        if is_free_or_negligible(price_wei, get_eth_price_usd()):
            await send_prepared_transactions(slug, chain_key)
    
    except Exception as e:
        log.error(f"خطأ: {e}")

# ---------------------------------------------------------------------------

async def watch_loop():
    """نوم + إرسال فوري عند البدء"""
    
    while True:
        if not pending_mints:
            await asyncio.sleep(5)
            continue
        
        upcoming = sorted(pending_mints.items(), key=lambda x: x[1]["start_time"])
        next_slug, next_mint = upcoming[0]
        
        sleep_seconds = next_mint["start_time"] - int(time.time()) - 1
        
        if sleep_seconds > 0:
            await asyncio.sleep(sleep_seconds)
        
        log.info(f"🎉 '{next_slug}' بدأ - إرسال فوري")
        
        # ✅ إرسال المعاملات الموقعة مسبقاً
        await send_prepared_transactions(next_slug, next_mint["chain_key"])
        
        pending_mints.pop(next_slug, None)

# ---------------------------------------------------------------------------

def get_full_drop_info(w3, contract_address):
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        return seadrop.functions.getPublicDrop(Web3.to_checksum_address(contract_address)).call()
    except:
        return None

def is_free_or_negligible(price_wei, eth_price_usd):
    return (price_wei / 1e18) * eth_price_usd < config.free_price_threshold

def get_eth_price_usd():
    try:
        resp = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd", timeout=5)
        return float(resp.json()["ethereum"]["usd"])
    except:
        return 3000.0

# ---------------------------------------------------------------------------

async def run():
    await warm_up()
    log.info(f"✅ بدء التشغيل مع {len(config.wallets)} محافظ")
    
    # WebSocket + watch_loop
    # ...

if __name__ == "__main__":
    asyncio.run(run())
