"""
جلب سعر ETH من مصادر متعددة مع تخزين مؤقت
"""

import asyncio
import aiohttp
import logging
import time
import os
from typing import Optional

log = logging.getLogger("price-fetcher")

class ETHPriceFetcher:
    """جلب سعر ETH من مصادر متعددة"""
    
    def __init__(self):
        self.cached_price = None
        self.cached_time = 0
        self.cache_ttl = 30  # 30 ثانية
        self.fallback_price = 3000.0  # قيمة احتياطية
        
        # مصادر الأسعار
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
        ]
    
    async def get_price(self, timeout: float = 3.0) -> float:
        """جلب السعر من أسرع مصدر"""
        
        # التحقق من الكاش
        if self.cached_price and (time.time() - self.cached_time) < self.cache_ttl:
            return self.cached_price
        
        # جلب من جميع المصادر بالتوازي
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_price(session, src, timeout) for src in self.sources]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            prices = []
            for i, result in enumerate(results):
                if isinstance(result, (int, float)) and result > 0:
                    prices.append(result)
                    log.debug(f"✅ {self.sources[i]['name']}: ${result}")
            
            if prices:
                avg_price = sum(prices) / len(prices)
                self.cached_price = avg_price
                self.cached_time = time.time()
                log.info(f"💰 سعر ETH: ${avg_price:.2f}")
                return avg_price
            
            # استخدام القيمة المخبأة
            if self.cached_price:
                log.warning(f"⚠️ استخدام سعر مخبأ: ${self.cached_price:.2f}")
                return self.cached_price
            
            # استخدام القيمة الاحتياطية
            log.warning(f"⚠️ استخدام قيمة احتياطية: ${self.fallback_price}")
            return self.fallback_price
    
    async def _fetch_price(self, session, source: dict, timeout: float):
        """جلب السعر من مصدر واحد"""
        try:
            async with session.get(source["url"], timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(source["parser"](data))
        except Exception as e:
            log.debug(f"⚠️ {source['name']}: {str(e)[:30]}")
            return None

# نسخة عالمية
_price_fetcher = ETHPriceFetcher()

def get_eth_price_sync() -> float:
    """نسخة متزامنة للاستخدام مع الكود القديم"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            future = asyncio.run_coroutine_threadsafe(_price_fetcher.get_price(), loop)
            return future.result(timeout=5)
        else:
            return asyncio.run(_price_fetcher.get_price())
    except Exception as e:
        log.error(f"خطأ في جلب السعر: {e}")
        return 3000.0