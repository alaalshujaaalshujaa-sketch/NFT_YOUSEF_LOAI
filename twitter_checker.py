import os
import logging
import time
import requests
from typing import Optional

log = logging.getLogger("twitter-verifier")

# 🔥 تخزين مؤقت لتويتر مع TTL
_twitter_cache = {}
_CACHE_TTL = 300  # 5 دقائق (أقل من السابق للدقة)

# 🔥 تخزين مؤقت للـ slugs المرفوضة
_rejected_twitter_cache = {}
_REJECTED_TTL = 600  # 10 دقائق

def get_twitter_username_from_opensea(slug: str, opensea_api_key: str) -> Optional[str]:
    """جلب اسم المستخدم من تويتر مع تخزين مؤقت"""
    
    # التحقق من الكاش
    if slug in _twitter_cache:
        cached_result, timestamp = _twitter_cache[slug]
        if time.time() - timestamp < _CACHE_TTL:
            return cached_result
    
    try:
        url = f"https://api.opensea.io/api/v2/collections/{slug}"
        headers = {"x-api-key": opensea_api_key}
        resp = requests.get(url, headers=headers, timeout=5)
        
        if resp.status_code == 200:
            result = resp.json().get("twitter_username")
            # تخزين في الكاش
            _twitter_cache[slug] = (result, time.time())
            return result
        else:
            log.warning(f"[OpenSea Collections API] HTTP {resp.status_code} عند جلب '{slug}'")
    except Exception as e:
        log.warning(f"[Twitter Check] تعذر جلب معلومات المجموعة لـ {slug}: {e}")
    
    return None


def get_twitter_username_cached(slug: str, opensea_api_key: str) -> Optional[str]:
    """نفس الدالة مع استخدام الكاش (alias للتوافق)"""
    return get_twitter_username_from_opensea(slug, opensea_api_key)


def is_valid_twitter_account(username: str) -> bool:
    """التحقق من صحة حساب تويتر (يستخدم للتحقق الإضافي)"""
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
    if not username:
        return False
    if not bearer_token:
        log.error("[X API] TWITTER_BEARER_TOKEN غير مضبوط في متغيرات البيئة")
        return False

    try:
        url = f"https://api.x.com/2/users/by/username/{username}?user.fields=verified,public_metrics"
        headers = {"Authorization": f"Bearer {bearer_token}"}
        resp = requests.get(url, headers=headers, timeout=5)

        if resp.status_code == 200:
            user_data = resp.json().get("data", {})
            metrics = user_data.get("public_metrics", {})

            is_verified = user_data.get("verified", False)
            followers_count = metrics.get("followers_count", 0)

            # 🔥 شرط القبول: موثّق أو متابعين 100+
            if is_verified or followers_count >= 100:
                log.info(f"✅ حساب X موثوق: @{username} (متابعين: {followers_count})")
                return True
            else:
                log.info(f"⚠️ حساب X ضعيف: @{username} (متابعين: {followers_count})")
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
        log.error(f"[X API Error] خطأ أثناء التحقق من حساب @{username}: {e}")

    return False
