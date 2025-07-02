
import atexit
import datetime

import redis

from parser import Parser
from downloader import SiteDownloader
from mapper import SiteMapper
from utils import parse_url
from helper_classes import CrawlerRun
# from cache import URLCache, URLData, VisitTracker

from config.configuration import get_logger

logger = get_logger(__name__)

REDIS_HOST = "localhost"
REDIS_PORT = 7777


redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)

to_visit = []


class CrawlRun:
    def __init__(self, manager: Manager):
        self.manager = manager
        self.SiteMapper = SiteMapper(manager=manager)
        self.SiteDownloader = SiteDownloader(manager=manager)
        self.Parser = Parser(manager=manager)


def get_top_level_links(crawler_run):
    sitemap_url, sitemap_indexes, sitemap_details = get_sitemap(crawler_run)
    if sitemap_url is None:
        frontier_links = [crawler_run.seed_url]
    else:
        frontier_links = [x['loc'] for x in sitemap_details]
    return frontier_links

def download_and_parse_url(url: str):
    downloader = SiteDownloader(url,logger=logger)
    content = downloader.get_page_elements(url)
    parser = Parser(url,logger=logger)
    return content

def process_url(crawler_run: CrawlerRun):
    frontier_links = get_top_level_links(crawler_run)
    for url in frontier_links:
        links = download_and_parse_url(url)
        for link in links:
            to_visit.append(link)
    pass

def flush_db():
    logger.info("Flushing database")
    redis_conn.flushall()

def crawl(manager: Manager):


    atexit.register(flush_db)
    process_url(crawler_run)
    CrawlerRun(seed_url=seed_url)




if __name__ == "__main__":
    main("https://www.google.com")