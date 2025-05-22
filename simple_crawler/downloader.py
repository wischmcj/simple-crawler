from __future__ import annotations

from urllib.parse import urlparse

import requests
from config.configuration import get_logger
from manager import Manager
from protego import Protego

logger = get_logger("downloader")


class SiteDownloader:
    def __init__(self, manager: Manager, write_to_db: bool = True):
        self.manager = manager
        self.crawl_tracker = manager.crawl_tracker
        self.write_to_db = write_to_db

    def save_html(self, html: str, filename: str):
        with open(filename, "w", encoding="UTF-8") as f:
            f.write(html)

    # Politeness
    def can_fetch(self, url: str) -> bool:
        """Check if we're allowed to crawl this URL according to robots.txt"""
        parsed_url = urlparse(url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        try:
            robots_response = requests.get(robots_url)
            self.rp = Protego.parse(robots_response.text)
            return self.rp.can_fetch("*", url)
        except Exception as e:
            logger.warning(f"Error checking robots.txt for {url}: {e}")
            return True  # If we can't check robots.txt, we probably want to set a reasonable default

    def read_politeness_info(self, url: str):
        parsed_url = urlparse(url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        robots_response = requests.get(robots_url)
        self.rp = Protego.parse(robots_response.text)
        sitemap_url = self.rp.sitemaps
        rrate = self.rp.request_rate("*")
        crawl_delay = self.rp.crawl_delay("*")
        return sitemap_url, rrate, crawl_delay

    def on_success(self, url: str, content: str, status_code: int):
        update_map = {
            "content": content,
            "attrs": {"crawl_status": "downloaded", "status_code": status_code},
        }
        _ = self.crawl_tracker.update_url(url, update_map)

    def on_failure(self, url: str, crawl_status: str, status_code: int):
        update_map = {
            "attrs": {"crawl_status": crawl_status, "status_code": status_code}
        }
        _ = self.crawl_tracker.update_url(url, update_map, close=True)

    def get_page_elements(self, url: str, cache_results: bool = True) -> set[str]:
        """Get the page elements from a webpage"""

        # Check if we're allowed to crawl the page
        if not self.can_fetch(url):
            msg = f"Skipping {url} (not allowed by robots.txt)"
            logger.info(msg)
            self.on_failure(url, "disallowed", 403)
            return None, 403

        # Get the page elementsupdate_urlupdate_url
        try:
            response = requests.get(url, timeout=1)
            response.raise_for_status()
            if cache_results:
                self.on_success(url, response.text, response.status_code)
        except Exception as e:
            # If we can't get the page, we'll return the error
            # and closed the url out, not passing it to the parser
            logger.error(f"Error getting {url}: {e}")
            if cache_results:
                self.on_failure(url, "error", response.status_code)
            raise e
        return response.text, response.status_code
