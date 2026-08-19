"""
فحص تويتر مع تخزين مؤقت لتسريع العملية
"""

import os
import logging
import time
import requests
from typing import Optional

log = logging.getLogger("twitter-checker")

# تخزين مؤقت
_twitter_cache = {}
_CACHE_TTL = 300  # 5 دقائق

# تخزين slugs المرفوضة
_rejected_cache = {}
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
            _twitter_cache[slug] = (result, time.time())
            return result
        else:
            log.warning(f"[OpenSea API] HTTP {resp.status_code} عند جلب '{slug}'")
    except Exception as e:
        log.warning(f"[Twitter Check] خطأ لـ {slug}: {e}")
    
    return None

def is_twitter_rejected(slug: str) -> bool:
    """التحقق إذا كان slug مرفوضاً سابقاً"""
    if slug in _rejected_cache:
        timestamp = _rejected_cache[slug]
        if time.time() - timestamp < _REJECTED_TTL:
            return True
        else:
            del _rejected_cache[slug]
    return False

def mark_twitter_rejected(slug: str):
    """تسجيل slug مرفوض"""
    _rejected_cache[slug] = time.time()