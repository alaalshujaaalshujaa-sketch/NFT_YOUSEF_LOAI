import os
import logging
import time
import aiohttp
import asyncio

log = logging.getLogger("twitter-verifier")

_twitter_cache = {}
CACHE_DURATION = 300  # 5 دقائق

async def get_twitter_username_from_opensea(slug: str, opensea_api_key: str):
    """جلب اسم المستخدم من تويتر عبر OpenSea API - نسخة غير متزامنة"""
    if slug in _twitter_cache:
        username, timestamp = _twitter_cache[slug]
        if time.time() - timestamp < CACHE_DURATION:
            return username
    
    try:
        url = f"https://api.opensea.io/api/v2/collections/{slug}"
        headers = {"x-api-key": opensea_api_key}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    username = data.get("twitter_username")
                    _twitter_cache[slug] = (username, time.time())
                    return username
                else:
                    log.warning(f"[OpenSea API] HTTP {resp.status} عند جلب '{slug}'")
    except Exception as e:
        log.warning(f"[Twitter Check] تعذر جلب معلومات المجموعة لـ {slug}: {e}")
    return None

def get_twitter_username_from_opensea_sync(slug: str, opensea_api_key: str):
    """نسخة متزامنة للتوافق مع الكود القديم"""
    import requests
    if slug in _twitter_cache:
        username, timestamp = _twitter_cache[slug]
        if time.time() - timestamp < CACHE_DURATION:
            return username
    
    try:
        url = f"https://api.opensea.io/api/v2/collections/{slug}"
        headers = {"x-api-key": opensea_api_key}
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            username = resp.json().get("twitter_username")
            _twitter_cache[slug] = (username, time.time())
            return username
    except Exception as e:
        log.warning(f"[Twitter Check] تعذر جلب معلومات المجموعة لـ {slug}: {e}")
    return None

async def is_valid_twitter_account(username: str) -> bool:
    """التحقق من حساب تويتر - نسخة غير متزامنة"""
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
    if not username or not bearer_token:
        return False

    try:
        url = f"https://api.x.com/2/users/by/username/{username}?user.fields=verified,public_metrics"
        headers = {"Authorization": f"Bearer {bearer_token}"}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    user_data = data.get("data", {})
                    metrics = user_data.get("public_metrics", {})
                    
                    is_verified = user_data.get("verified", False)
                    followers_count = metrics.get("followers_count", 0)
                    
                    if is_verified or followers_count >= 100:
                        log.info(f"✅ حساب X موثوق: @{username} (متابعين: {followers_count})")
                        return True
                    else:
                        log.info(f"⚠️ حساب X ضعيف: @{username} (متابعين: {followers_count})")
                        return False
                elif resp.status == 429:
                    log.error(f"[X API] تجاوزت حد الطلبات (429) عند فحص @{username}")
                    return False
                else:
                    log.error(f"[X API] استجابة غير متوقعة (HTTP {resp.status}) عند فحص @{username}")
                    return False
    except Exception as e:
        log.error(f"[X API Error] خطأ أثناء التحقق من @{username}: {e}")
    return False

def is_valid_twitter_account_sync(username: str) -> bool:
    """نسخة متزامنة للتوافق مع الكود القديم"""
    import requests
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
    if not username or not bearer_token:
        return False

    try:
        url = f"https://api.x.com/2/users/by/username/{username}?user.fields=verified,public_metrics"
        headers = {"Authorization": f"Bearer {bearer_token}"}
        resp = requests.get(url, headers=headers, timeout=5)
        
        if resp.status_code == 200:
            data = resp.json()
            user_data = data.get("data", {})
            metrics = user_data.get("public_metrics", {})
            
            is_verified = user_data.get("verified", False)
            followers_count = metrics.get("followers_count", 0)
            
            if is_verified or followers_count >= 100:
                log.info(f"✅ حساب X موثوق: @{username} (متابعين: {followers_count})")
                return True
            else:
                log.info(f"⚠️ حساب X ضعيف: @{username} (متابعين: {followers_count})")
                return False
        elif resp.status_code == 429:
            log.error(f"[X API] تجاوزت حد الطلبات (429) عند فحص @{username}")
            return False
        else:
            log.error(f"[X API] استجابة غير متوقعة (HTTP {resp.status_code}) عند فحص @{username}")
            return False
    except Exception as e:
        log.error(f"[X API Error] خطأ أثناء التحقق من @{username}: {e}")
    return False
