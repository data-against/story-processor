import logging
from typing import Any, Callable, Dict, List, Optional

import scrapy
import scrapy.crawler as crawler
from twisted.internet import defer, reactor


class UrlSpider(scrapy.Spider):
    name: str = "urlspider"

    custom_settings: Dict[str, Any] = {
        "COOKIES_ENABLED": False,
        # "HTTPCACHE_ENABLED": True,  # useful to have set True locally for repetative runs while debugging code changes
        "LOG_LEVEL": "INFO",
        "CONCURRENT_REQUESTS": 64,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 5,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 64,
        "DOWNLOAD_TIMEOUT": 20,
        "USER_AGENT": "Mozilla/5.0 (compatible; Data Against Feminicide academic research; datoscontrafeminicidio.net)",
    }

    def __init__(
        self,
        handle_parse: Optional[Callable],
        start_urls: List[str],
        *args: List,
        **kwargs: Dict
    ) -> None:
        """
        Handle_parse will be called with a story:Dict object
        :param handle_parse:
        :param start_urls:
        :param args:
        :param kwargs:
        """
        super().__init__(*args, **kwargs)
        self.on_parse = handle_parse
        self.start_urls = start_urls
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


def run_spider(handle_parse: Callable, urls: List[str]) -> defer.Deferred:
    """Runs a spider for a batch of URLs and returns a deferred object:"""
    runner = crawler.CrawlerRunner()
    deferred = runner.crawl(UrlSpider, handle_parse=handle_parse, start_urls=urls)
    return deferred


def fetch_all_html(
    urls: List[str], handle_parse: Callable, num_spiders: int = 4
) -> None:
    """Splits URLs into batches and manages the concurrent execution of multiple spiders"""
    if not urls:
        return

    batch_size = len(urls) // num_spiders + (len(urls) % num_spiders > 0)
    batches = [urls[i : i + batch_size] for i in range(0, len(urls), batch_size)]

    deferreds = [run_spider(handle_parse, batch) for batch in batches]

    dl = defer.DeferredList(deferreds)
    dl.addBoth(lambda _: reactor.stop())
    reactor.run()
