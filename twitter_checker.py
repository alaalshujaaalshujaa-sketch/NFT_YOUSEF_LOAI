"""
التحقق من وجود حساب X (تويتر) عبر OpenSea API فقط
"""

import logging
import requests
from typing import Optional

log = logging.getLogger("twitter_checker")

def get_twitter_username_from_opensea(slug: str, api_key: str) -> Optional[str]:
    """
    جلب اسم المستخدم في X (تويتر) من OpenSea API
    """
    try:
        # جلب تفاصيل المجموعة من OpenSea
        url = f"https://api.opensea.io/api/v2/collections/{slug}"
        headers = {
            "x-api-key": api_key,
            "accept": "application/json"
        }
        
        log.info(f"🔍 جاري البحث عن حساب X للمجموعة: {slug}")
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            # محاولة 1: البحث في social_links
            social_links = data.get('social_links', [])
            if social_links:
                for link in social_links:
                    if isinstance(link, dict):
                        url_lower = link.get('url', '').lower()
                        if 'twitter.com' in url_lower or 'x.com' in url_lower:
                            username = link.get('username')
                            if username:
                                log.info(f"✅ تم العثور على حساب X: @{username}")
                                return username
            
            # محاولة 2: البحث المباشر
            twitter_username = data.get('twitter_username')
            if twitter_username:
                log.info(f"✅ تم العثور على حساب X: @{twitter_username}")
                return twitter_username
            
            # محاولة 3: البحث في project_details
            project_details = data.get('project_details')
            if isinstance(project_details, dict):
                twitter_username = project_details.get('twitter_username')
                if twitter_username:
                    log.info(f"✅ تم العثور على حساب X: @{twitter_username}")
                    return twitter_username
            
            log.warning(f"⚠️ لا يوجد حساب X للمجموعة {slug}")
            return None
            
        else:
            log.warning(f"⚠️ استجابة غير متوقعة: {response.status_code}")
            return None
            
    except Exception as e:
        log.error(f"❌ خطأ: {e}")
        return None
