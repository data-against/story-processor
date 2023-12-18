from typing import Union

from scrapy import Request, Spider, signals
from scrapy.crawler import Crawler
from scrapy.http import Response
from scrapy.settings import Settings
from scrapy.statscollectors import StatsCollector

from processor.redis_cache_storage import RedisCacheStorage


class RedisDeltaFetch:
    def __init__(self, settings: Settings, stats: StatsCollector):
        self.cache = RedisCacheStorage(settings)
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        assert crawler.stats

        o = cls(crawler.settings, crawler.stats)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_opened(self, spider):
        self.cache.open_spider(spider)

    def spider_closed(self, spider):
        self.cache.close_spider(spider)

    def process_request(
        self, request: Request, spider: Spider
    ) -> Union[Request, Response, None]:
        cached_response = self.cache.retrieve_response(spider, request)

        if cached_response is None:
            self.stats.inc_value("httpcache/miss", spider=spider)
            return None

        self.stats.inc_value("httpcache/hit", spider=spider)
        return cached_response

    def process_response(
        self, request: Request, response: Response, spider: Spider
    ) -> Union[Response, Request]:
        self.stats.inc_value("httpcache/store", spider=spider)
        self.cache.store_response(spider=spider, request=request, response=response)

        return response
