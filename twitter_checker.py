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
    
    Args:
        slug: معرف المجموعة في OpenSea
        api_key: مفتاح API الخاص بـ OpenSea
    
    Returns:
        اسم المستخدم في X أو None إذا لم يكن موجوداً
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
            
            # البحث عن حسابات التواصل الاجتماعي
            social_links = data.get('social_links', [])
            
            # البحث عن رابط X (تويتر)
            for link in social_links:
                url_lower = link.get('url', '').lower()
                if 'twitter.com' in url_lower or 'x.com' in url_lower:
                    username = link.get('username')
                    if username:
                        log.info(f"✅ تم العثور على حساب X: @{username} للمجموعة {slug}")
                        return username
            
            # محاولة بديلة: البحث في data مباشرة
            twitter_username = data.get('twitter_username')
            if twitter_username:
                log.info(f"✅ تم العثور على حساب X: @{twitter_username} للمجموعة {slug}")
                return twitter_username
            
            # محاولة ثالثة: البحث في project details
            project_details = data.get('project_details', {})
            twitter_username = project_details.get('twitter_username')
            if twitter_username:
                log.info(f"✅ تم العثور على حساب X: @{twitter_username} للمجموعة {slug}")
                return twitter_username
            
            log.warning(f"⚠️ لا يوجد حساب X للمجموعة {slug}")
            return None
            
        elif response.status_code == 404:
            log.warning(f"⚠️ المجموعة {slug} غير موجودة في OpenSea")
            return None
        else:
            log.warning(f"⚠️ استجابة غير متوقعة من OpenSea: {response.status_code}")
            return None
            
    except requests.exceptions.Timeout:
        log.error(f"❌ انتهى الوقت في جلب بيانات المجموعة {slug}")
        return None
    except requests.exceptions.ConnectionError:
        log.error(f"❌ خطأ في الاتصال بـ OpenSea للمجموعة {slug}")
        return None
    except Exception as e:
        log.error(f"❌ خطأ في جلب حساب X للمجموعة {slug}: {e}")
        return None

# دالة بديلة تستخدم Drops API إذا فشلت الطريقة الأولى
def get_twitter_from_drops_api(slug: str, api_key: str) -> Optional[str]:
    """
    محاولة جلب حساب X من Drops API كطريقة احتياطية
    """
    try:
        url = f"https://api.opensea.io/api/v2/drops/{slug}"
        headers = {
            "x-api-key": api_key,
            "accept": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            # البحث في بيانات المينت
            collection = data.get('collection', {})
            social_links = collection.get('social_links', [])
            
            for link in social_links:
                url_lower = link.get('url', '').lower()
                if 'twitter.com' in url_lower or 'x.com' in url_lower:
                    username = link.get('username')
                    if username:
                        log.info(f"✅ (Drops API) تم العثور على حساب X: @{username} للمجموعة {slug}")
                        return username
            
            # محاولة استخراج من البيانات المباشرة
            twitter_username = data.get('twitter_username')
            if twitter_username:
                log.info(f"✅ (Drops API) تم العثور على حساب X: @{twitter_username} للمجموعة {slug}")
                return twitter_username
                
        return None
    except Exception as e:
        log.debug(f"⚠️ فشل Drops API لـ {slug}: {e}")
        return None

# الدالة الرئيسية مع Fallback
def get_twitter_username_from_opensea_with_fallback(slug: str, api_key: str) -> Optional[str]:
    """
    محاولة جلب اسم المستخدم في X باستخدام طرق متعددة
    """
    # المحاولة الأولى: Collections API
    username = get_twitter_username_from_opensea(slug, api_key)
    if username:
        return username
    
    # المحاولة الثانية: Drops API
    username = get_twitter_from_drops_api(slug, api_key)
    if username:
        return username
    
    # إذا لم يتم العثور على حساب
    log.info(f"❌ لم يتم العثور على حساب X للمجموعة {slug}")
    return None

# تصدير الدالة الرئيسية (للتوافق مع main.py)
get_twitter_username_from_opensea = get_twitter_username_from_opensea_with_fallback
