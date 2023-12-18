import logging
from typing import Any, Callable, Dict, List, Optional

import scrapy
from scrapy.crawler import CrawlerProcess

import processor


class UrlSpider(scrapy.Spider):
    name: str = "urlspider"

    custom_settings: Dict[str, Any] = {
        "COOKIES_ENABLED": False,
        # "HTTPCACHE_ENABLED": True,  # useful to have set True locally for repetative runs while debugging code changes
        "LOG_LEVEL": "INFO",
        "CONCURRENT_REQUESTS": 32,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 32,
        "DOWNLOAD_TIMEOUT": 30,
        "USER_AGENT": "Data Against Feminicides bot for open academic research (+http://datoscontrafeminicidio.net/)",
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
        "DOWNLOADER_MIDDLEWARES": {"processor.redis_delta_fetch.RedisDeltaFetch": 543},
        "REDIS_CONFIG": {
            "host": processor.REDIS_URL[0],
            "port": processor.REDIS_URL[1],
        },
        "CACHE_TTL": 4 * 24 * 60 * 60,
    }

    def __init__(
        self, handle_parse: Optional[Callable], *args: List, **kwargs: Dict
    ) -> None:
        """
        Handle_parse will be called wth a story:Dict object
        :param handle_parse:
        :param args:
        :param kwargs:
        """
        super(UrlSpider, self).__init__(*args, **kwargs)
        self.on_parse = handle_parse
        logging.getLogger("scrapy").setLevel(logging.INFO)
        logging.getLogger("scrapy.core.engine").setLevel(logging.INFO)

    def parse(self, response):
        # grab the original, undirected URL so we can relink later
        orig_url = (
            response.request.meta["redirect_urls"][0]
            if "redirect_urls" in response.request.meta
            else response.request.url
        )
        story_data = dict(
            content=response.text, final_url=response.request.url, original_url=orig_url
        )
        if self.on_parse:
            self.on_parse(story_data)
        return None


def fetch_all_html(urls: List[str], handle_parse: Callable) -> None:
    if len(urls) == 0:
        return
    process = CrawlerProcess()
    process.crawl(UrlSpider, handle_parse=handle_parse, start_urls=urls)
    process.start()
