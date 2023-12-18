import logging
import pickle

import redis
from scrapy import Spider
from scrapy.http import Headers
from scrapy.responsetypes import responsetypes
from scrapy.settings import Settings

logger = logging.getLogger(__name__)


class RedisCacheStorage:
    def __init__(self, settings: Settings):
        self.cache = None
        self._finger_printer = None
        self.settings = settings
        self.ttl = self.settings.getint("CACHE_TTL")
        self.redis_config = self.settings.get("REDIS_CONFIG")

    def open_spider(self, spider: Spider):
        logger.debug("Using Redis as the HTTP Cache backend")
        self._finger_printer = spider.crawler.request_fingerprinter
        self.cache = redis.Redis(
            host=self.redis_config.get("host"),
            port=self.redis_config.get("port"),
            decode_responses=True,
        )

    def close_spider(self, spider: Spider):
        logger.info(
            f"Number of entries in Redis Cache at end of run: {self.cache.dbsize()=}"
        )
        logger.debug("Terminating Redis connection")

    def store_response(self, spider, request, response):
        key = self._finger_printer.fingerprint(request).hex()
        data = {
            "status": response.status,
            "url": response.url,
            "headers": dict(response.headers),
            "body": response.body,
        }
        self.cache.set(name=key, value=pickle.dumps(data, protocol=4), ex=self.ttl)

    def _read_data(self, request):
        key = self._finger_printer.fingerprint(request).hex()
        cached_response = self.cache.get(key)

        if cached_response is not None:
            return pickle.loads(cached_response)
        return

    def retrieve_response(self, spider, request):
        data = self._read_data(request)
        if data is None:
            return

        url = data["url"]
        status = data["status"]
        headers = Headers(data["headers"])
        body = data["body"]

        response_class = responsetypes.fromargs(headers=headers, url=url, body=body)
        response = response_class(url=url, headers=headers, status=status, body=body)
        return response
