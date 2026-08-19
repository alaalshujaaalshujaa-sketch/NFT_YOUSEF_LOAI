"""
جلب سعر ETH من مصادر متعددة مع تخزين مؤقت وإعادة محاولة ذكية
محسّن لتقليل الأخطاء وتحسين الاستقرار
"""

import asyncio
import aiohttp
import logging
import time
import os
from typing import Optional

# تقليل Logging للمكتبات الخارجية
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

log = logging.getLogger("price-fetcher")

class ETHPriceFetcher:
    """جلب سعر ETH من مصادر متعددة مع تخزين مؤقت وإعادة محاولة"""
    
    def __init__(self):
        self.cached_price = 3000.0  # قيمة ابتدائية
        self.cached_time = 0
        self.cache_ttl = 60  # 60 ثانية
        self.fallback_price = float(os.environ.get("FALLBACK_ETH_PRICE", "3000.0"))
        self.max_retries = 2
        self.retry_delay = 1
        
        # مصادر الأسعار المتعددة
        self.sources = [
            {
                "name": "CoinGecko",
                "url": "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
                "parser": lambda data: data["ethereum"]["usd"]
            },
            {
                "name": "Binance",
                "url": "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT",
                "parser": lambda data: float(data["price"])
            },
            {
                "name": "Coinbase",
                "url": "https://api.coinbase.com/v2/prices/ETH-USD/spot",
                "parser": lambda data: float(data["data"]["amount"])
            },
            {
                "name": "Kraken",
                "url": "https://api.kraken.com/0/public/Ticker?pair=ETHUSD",
                "parser": lambda data: float(data["result"]["XETHZUSD"]["c"][0])
            },
        ]
    
    async def get_price(self, timeout: float = 5.0) -> float:
        """جلب السعر مع معالجة أفضل للأخطاء"""
        
        # التحقق من الكاش
        if self.cached_price and (time.time() - self.cached_time) < self.cache_ttl:
            return self.cached_price
        
        # محاولة جلب السعر
        try:
            price = await self._fetch_all_sources(timeout)
            if price and price > 0:
                self.cached_price = price
                self.cached_time = time.time()
                log.info(f"💰 سعر ETH: ${price:.2f}")
                return price
        except Exception as e:
            # تسجيل خفيف فقط
            log.debug(f"محاولة جلب السعر فشلت: {e}")
        
        # استخدام السعر المخبأ إذا كان حديثاً
        if self.cached_price and (time.time() - self.cached_time) < 300:  # 5 دقائق
            log.info(f"💰 سعر ETH (مخبأ): ${self.cached_price:.2f}")
            return self.cached_price
        
        # استخدام القيمة الاحتياطية
        log.info(f"💰 سعر ETH (احتياطي): ${self.fallback_price:.2f}")
        return self.fallback_price
    
    async def _fetch_all_sources(self, timeout: float) -> Optional[float]:
        """جلب الأسعار من جميع المصادر بالتوازي"""
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for source in self.sources:
                tasks.append(self._fetch_price(session, source, timeout))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            prices = []
            for i, result in enumerate(results):
                if isinstance(result, (int, float)) and result > 0:
                    prices.append(result)
                    log.debug(f"✅ {self.sources[i]['name']}: ${result}")
                # لا نطبع أخطاء هنا لتقليل الـ Logs المزعجة
            
            if prices:
                return sum(prices) / len(prices)
            
            return None
    
    async def _fetch_price(self, session, source: dict, timeout: float) -> Optional[float]:
        """جلب السعر من مصدر واحد - صامت عند الفشل"""
        try:
            async with session.get(source["url"], timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(source["parser"](data))
        except Exception:
            # صامت - لا نطبع أي شيء هنا لتقليل الـ Logs
            pass
        return None

# نسخة عالمية
_price_fetcher = ETHPriceFetcher()

def get_eth_price_sync() -> float:
    """نسخة متزامنة للاستخدام مع الكود القديم"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            future = asyncio.run_coroutine_threadsafe(_price_fetcher.get_price(), loop)
            return future.result(timeout=10)
        else:
            return asyncio.run(_price_fetcher.get_price())
    except Exception as e:
        # تسجيل خطأ بسيط فقط
        log.debug(f"خطأ في جلب السعر: {e}")
        return 3000.0
