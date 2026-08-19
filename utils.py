"""
أدوات مساعدة للبوت
"""

import time
import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta

log = logging.getLogger("utils")

# ============================================
# 🔥 تخزين مؤقت ذكي
# ============================================

class FastCache:
    """تخزين مؤقت سريع مع انتهاء صلاحية"""
    
    def __init__(self, default_ttl: int = 60):
        self._cache: Dict[str, tuple[float, Any]] = {}
        self.default_ttl = default_ttl
    
    def get(self, key: str) -> Optional[Any]:
        if key in self._cache:
            timestamp, value = self._cache[key]
            if time.time() - timestamp < self.default_ttl:
                return value
            del self._cache[key]
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        self._cache[key] = (time.time(), value)
    
    def clear(self):
        self._cache.clear()
    
    def remove(self, key: str):
        if key in self._cache:
            del self._cache[key]

# ============================================
# 🔥 إدارة الوقت
# ============================================

LOCAL_TZ = timezone(timedelta(hours=3))

def parse_iso(ts: str):
    """تحويل ISO string إلى datetime"""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def started_today_local(stage: dict) -> bool:
    """التحقق إذا كان المينت بدأ اليوم"""
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()

def stage_has_ended(stage: dict) -> bool:
    """التحقق إذا انتهت المرحلة"""
    end = parse_iso(stage.get("end_time", ""))
    if not end:
        return False
    return datetime.now(timezone.utc) > end

# ============================================
# 🔥 تقييد الطلبات (Rate Limiter)
# ============================================

class RateLimiter:
    """تقييد عدد الطلبات"""
    
    def __init__(self, max_requests: int, time_window: float):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
    
    async def wait(self):
        """انتظار إذا تم تجاوز الحد"""
        now = time.time()
        # إزالة الطلبات القديمة
        self.requests = [ts for ts in self.requests if now - ts < self.time_window]
        
        if len(self.requests) >= self.max_requests:
            # انتظار حتى يصبح هناك مجال
            oldest = self.requests[0]
            wait_time = self.time_window - (now - oldest)
            if wait_time > 0:
                await asyncio.sleep(wait_time + 0.1)
        
        self.requests.append(now)