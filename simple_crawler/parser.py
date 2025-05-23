from __future__ import annotations

from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from config.configuration import get_logger
from manager import Manager

# from utils import BaseWorkClass

# this is essentially the same as the
# parser, so we dont assign a specific logger
logger = get_logger("parser")


class Parser:
    def __init__(self, manager: Manager, write_to_db: bool = True, url: str = None):
        self.url = url
        self.crawl_tracker = manager.crawl_tracker
        self.write_to_db = write_to_db

    def get_links_from_content(self, url: str, content: str) -> set[str]:
        """Extract all links from a webpage"""
        soup = BeautifulSoup(content, "html.parser")
        links = set()
        # Looking for <a></a> tags with an href
        # Future state: look for other linkable tags like <img> or <script>
        tag_instances = soup.find_all("a", href=True)
        logger.debug(f"Found {len(tag_instances)} anchor tags")
        for tag in soup.find_all("a", href=True):
            try:
                href = tag["href"]
                absolute_url = urljoin(url, href)
            except Exception as e:
                logger.error(f"Error parsing {url}: {e}")
                return set()
            # Only include URLs from the same domain
            if urlparse(absolute_url).netloc == urlparse(url).netloc:
                links.add(absolute_url)
                self.crawl_tracker.request_download(absolute_url)
        return links

    def on_success(self, url, links):
        """Callback for when a job succeeds"""
        update_map = {"attrs": {"crawl_status": "parsed"}, "linked_urls": links}
        _ = self.crawl_tracker.update_url(url, update_map)

    def on_failure(self, url):
        """Callback for when a job fails"""
        update_map = {"attrs": {"crawl_status": "error"}}
        _ = self.crawl_tracker.update_url(url, update_map, close=True)

    # Crawling Logic
    def parse(self, url, content=None):
        """Main crawling method"""
        if content is None:
            content = self.crawl_tracker.get_cached_response(url)
        links = set()
        logger.debug(f"Parsing {url}")
        try:
            links = self.get_links_from_content(url, content)
            self.on_success(url, list(links))
        except Exception as e:
            logger.error(f"Error parsing {url}: {e}")
            self.on_failure(url)
        return links
