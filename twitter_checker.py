"""
التحقق من حسابات X (تويتر) مع تخزين مؤقت.
"""

import os
import logging
import requests
from datetime import datetime, timedelta

log = logging.getLogger("twitter-verifier")

# تخزين مؤقت للنتائج
_cache = {}
CACHE_DURATION = timedelta(minutes=5)

def get_cached(key: str):
    """جلب من التخزين المؤقت"""
    if key in _cache:
        value, timestamp = _cache[key]
        if datetime.now() - timestamp < CACHE_DURATION:
            return value
    return None

def set_cache(key: str, value):
    """تخزين في التخزين المؤقت"""
    _cache[key] = (value, datetime.now())

def get_twitter_username_from_opensea(slug: str, opensea_api_key: str) -> str | None:
    """جلب اسم المستخدم من OpenSea مع تخزين مؤقت"""
    # التحقق من التخزين المؤقت
    cache_key = f"twitter_{slug}"
    cached = get_cached(cache_key)
    if cached is not None:
        return cached
    
    try:
        url = f"https://api.opensea.io/api/v2/collections/{slug}"
        headers = {"x-api-key": opensea_api_key}
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            username = resp.json().get("twitter_username")
            set_cache(cache_key, username)
            return username
        else:
            log.warning(f"[OpenSea Collections API] HTTP {resp.status_code} عند جلب '{slug}': {resp.text[:200]}")
    except Exception as e:
        log.warning(f"[Twitter Check] تعذر جلب معلومات المجموعة لـ {slug}: {e}")
    return None

def is_valid_twitter_account(username: str) -> bool:
    """التحقق من صحة حساب X"""
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
    if not username:
        return False
    if not bearer_token:
        log.error("[X API] TWITTER_BEARER_TOKEN غير مضبوط في متغيرات البيئة")
        return False

    # التحقق من التخزين المؤقت
    cache_key = f"valid_{username}"
    cached = get_cached(cache_key)
    if cached is not None:
        return cached

    try:
        url = f"https://api.x.com/2/users/by/username/{username}?user.fields=verified,public_metrics"
        headers = {"Authorization": f"Bearer {bearer_token}"}
        resp = requests.get(url, headers=headers, timeout=5)

        if resp.status_code == 200:
            user_data = resp.json().get("data", {})
            metrics = user_data.get("public_metrics", {})

            is_verified = user_data.get("verified", False)
            followers_count = metrics.get("followers_count", 0)

            if is_verified or followers_count >= 100:
                log.info(f"✅ حساب X موثوق: @{username} (متابعين: {followers_count})")
                set_cache(cache_key, True)
                return True
            else:
                log.info(f"⚠️ حساب X ضعيف: @{username} (متابعين: {followers_count})")
                set_cache(cache_key, False)
                return False

        elif resp.status_code == 429:
            log.error(f"[X API] تجاوزت حد الطلبات (429) عند فحص @{username}")
            return False
        elif resp.status_code in (401, 403):
            log.error(f"[X API] فشل مصادقة (HTTP {resp.status_code}) عند فحص @{username}")
            return False
        else:
            log.error(f"[X API] استجابة غير متوقعة (HTTP {resp.status_code}) عند فحص @{username}")
            return False

    except Exception as e:
        log.error(f"[X API Error] خطأ أثناء التحقق من @{username}: {e}")

    return False
