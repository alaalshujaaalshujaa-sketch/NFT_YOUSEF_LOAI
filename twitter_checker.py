import os
import logging
import time
import requests
from typing import Optional

log = logging.getLogger("twitter-verifier-fast")

# ============ Cache ============
_twitter_cache: dict = {}
_twitter_cache_ttl = 60  # دقيقة واحدة

def get_cached_twitter(slug: str) -> Optional[str]:
    """الحصول على اسم تويتر من cache"""
    now = time.time()
    if slug in _twitter_cache:
        username, timestamp = _twitter_cache[slug]
        if now - timestamp < _twitter_cache_ttl:
            return username
    return None

def set_cached_twitter(slug: str, username: Optional[str]):
    """تخزين اسم تويتر في cache"""
    _twitter_cache[slug] = (username, time.time())

def get_twitter_username_from_opensea(slug: str, opensea_api_key: str) -> Optional[str]:
    """جلب اسم تويتر من OpenSea مع cache"""
    # التحقق من cache أولاً
    cached = get_cached_twitter(slug)
    if cached is not None:
        return cached
    
    try:
        url = f"https://api.opensea.io/api/v2/collections/{slug}"
        headers = {"x-api-key": opensea_api_key}
        resp = requests.get(url, headers=headers, timeout=3)
        
        if resp.status_code == 200:
            username = resp.json().get("twitter_username")
            set_cached_twitter(slug, username)
            return username
        else:
            log.warning(f"[OpenSea] HTTP {resp.status_code} عند جلب '{slug}'")
            set_cached_twitter(slug, None)
            return None
    except Exception as e:
        log.warning(f"[Twitter Check] تعذر جلب معلومات المجموعة لـ {slug}: {e}")
        set_cached_twitter(slug, None)
        return None

def is_valid_twitter_account(username: str) -> bool:
    """التحقق من صحة حساب تويتر (مبسط للسرعة)"""
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
    if not username or not bearer_token:
        return False

    try:
        url = f"https://api.x.com/2/users/by/username/{username}?user.fields=verified,public_metrics"
        headers = {"Authorization": f"Bearer {bearer_token}"}
        resp = requests.get(url, headers=headers, timeout=3)

        if resp.status_code == 200:
            user_data = resp.json().get("data", {})
            metrics = user_data.get("public_metrics", {})
            
            is_verified = user_data.get("verified", False)
            followers_count = metrics.get("followers_count", 0)
            
            return is_verified or followers_count >= 100
        elif resp.status_code == 429:
            log.warning(f"[X API] حد الطلبات لتويتر @{username}")
            return False
        else:
            log.error(f"[X API] خطأ HTTP {resp.status_code} عند فحص @{username}")
            return False
    except Exception as e:
        log.error(f"[X API Error] {e}")
        return False
