from typing import List, Dict, Any, Callable, Optional
import scrapy
from scrapy.crawler import CrawlerProcess


class UrlSpider(scrapy.Spider):
    name: str = 'urlspider'

    custom_settings: Dict[str, Any] = {
        "COOKIES_ENABLED": False,
        #"HTTPCACHE_ENABLED": True,  # useful to have set True locally for repetative runs while debugging code changes
        "LOG_LEVEL": "INFO",
        "AUTOTHROTTLE_ENABLED": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 10,
        "DOWNLOAD_TIMEOUT": 60,
        "USER_AGENT": 'Data Against Feminicides bot for open academic research (+http://datoscontrafeminicidio.net/)',
    }

    def __init__(
            self, handle_parse: Optional[Callable],
            *args: List, **kwargs: Dict
    ) -> None:
        """
        Handle_parse will be called wth a story:Dict object
        :param handle_parse:
        :param args:
        :param kwargs:
        """
        super(UrlSpider, self).__init__(*args, **kwargs)
        self.on_parse = handle_parse

    def parse(self, response):
        # grab the original, undirected URL so we can relink later
        orig_url = response.request.meta['redirect_urls'][0] if 'redirect_urls' in response.request.meta else response.request.url
        story_data = dict(html_content=response.text, final_url=response.request.url, original_url=orig_url)
        if self.on_parse:
            self.on_parse(story_data)
        return None


def fetch_all_html(urls: List[str], handle_parse: Callable) -> List[Dict]:
    process = CrawlerProcess()
    process.crawl(UrlSpider, handle_parse=handle_parse, start_urls=urls)
    process.start()
