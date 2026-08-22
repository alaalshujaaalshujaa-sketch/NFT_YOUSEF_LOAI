import os
import logging
import requests
import time

log = logging.getLogger("twitter-verifier")

# تخزين مؤقت بسيط لتقليل طلبات API
_twitter_cache = {}
CACHE_DURATION = 300  # 5 دقائق

def get_twitter_username_from_opensea(slug: str, opensea_api_key: str) -> str | None:
    """
    جلب اسم مستخدم تويتر من OpenSea Collections API
    - فقط يتحقق من وجود حساب X مربوط
    - لا يتحقق من صحة الحساب أو عدد المتابعين
    - يستخدم تخزين مؤقت لتقليل طلبات API
    """
    # التحقق من التخزين المؤقت
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
        else:
            log.warning(f"[OpenSea Collections API] HTTP {resp.status_code} عند جلب '{slug}': {resp.text[:200]}")
            
    except Exception as e:
        log.warning(f"[Twitter Check] تعذر جلب معلومات المجموعة لـ {slug}: {e}")
    
    return None


def is_valid_twitter_account(username: str) -> bool:
    """
    دالة احتياطية - تعيد True دائماً
    لأن الكود الأصلي كان يتحقق فقط من وجود الحساب، وليس من صحته
    """
    return True if username else False
