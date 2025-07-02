
import argparse
import redis
from asyncio import Queue
import asyncio
import atexit
from datetime import datetime
from email.parser import Parser

from config.configuration import get_logger
# from manager import Manager
from downloader import SiteDownloader
from parser import Parser
from config.configuration import get_logger
from helper_classes import BaseListener
from manager import DataManager


logger = get_logger('crawler')


# def run_w_on_ends(func, args, on_success, on_failure):
#     try:
#         result = func(*args)
#         on_success(result)
#     except Exception as e:
#         on_failure(e)

import asyncio


class VisitHandler(BaseListener):
    def __init__(self, pubsub, context, queue):
        """Initialize with a Redis pubsub object"""
        self.pubsub = pubsub
        self.running = True
        self.context = context
        self.queue = queue

    def add_to_visited(self, url: str):
        """Enqueue a job"""
        logger.info(f"Adding {url} to visited")
        self.context.visited.add(url)

    def handle_messages(self):
        """Main message handling loop"""
        while self.running:
            for message in self.pubsub.listen():
                if message["type"] == "message":
                    url = message["data"].decode("utf-8")
                    self.add_to_visited(url)

class DownloadHandler(BaseListener):
    def __init__(self, pubsub, context, queue):
        """Initialize with a Redis pubsub object"""
        self.pubsub = pubsub
        self.running = True
        self.context = context
        self.queue = queue

    def add_to_visit(self, url: str):
        """Enqueue a job"""
        logger.info(f"Enqueuing download for {url}")
        if url not in self.context.visited:
            self.context.to_visit.append(url)

    def handle_messages(self):
        """Main message handling loop"""
        while self.running:
            for message in self.pubsub.listen():
                if message["type"] == "message":
                    url = message["data"].decode("utf-8")
                    self.add_to_visit(url)

class QueueContext:
    def __init__(self, seed_url: str,
                  max_depth: int = 10,
                  host: str = "localhost",
                  port: int = 7777,
                  retries: int = 3,
                  debug: bool = False,
                  db_file: str = "sqlite.db",
                  rdb_file: str = "data.rdb"):
        self.seed_url = seed_url
        self.max_depth = max_depth
        self.parse_queue = Queue(10)
        self.visited = set()
        self.running = True
        self.rdb = redis.Redis(host=host, port=port)

    #     self.download_channel = "download"
    #     self.visited_channel = "visited"  
    #     self.add_listener(self.download_channel)
    #     self.add_listener(self.visited_channel)

        self.data_manager = DataManager(seed_url=seed_url, max_pages=max_depth, 
                                        host=host, port=port, retries=retries, 
                                        debug=debug, db_file=db_file, rdb_file=rdb_file)

    def get_page_to_visit(self) -> list[str]:
        """Get all frontier seeds for a URL"""
        return self.rdb.lpop("to_visit")

    def add_page_to_visit(self, url: str):
        """Add a frontier URL to the visit queue"""
        channel = "to_visit"
        self.rdb.lpush(f"to_visit", url)
        return url

    def add_page_visited(self, url: str):
        """Add a visited seed for a URL"""
        self.rdb.sadd("visited", url)
        return url

    def get_pages_visited(self) -> list[str]:
        """Get all frontier seeds for a URL"""
        return self.rdb.smembers("visited")
    

    def is_page_visited(self, url: str) -> bool:
        """Check if a page has been visited"""
        resp = self.rdb.sismember("visited", url)
        is_member = bool(resp.decode("utf-8"))
        return is_member

    # def add_listener(self, channel):
    #     pubsub = self.redis_conn.pubsub()
    #     pubsub.subscribe(channel)
    #     print(f"Subscribed to {channel}. Waiting for messages...")
    #     handler = DownloadHandler(pubsub=pubsub, data_manager=self.data_manager, queue=self.parse_queue, redis_conn=self.redis_conn)
    #     handler.start()
    #     self.listeners.append(handler)

    async def process_url(self, url: str):
        self.add_page_to_visit(url)
        self.parse_queue = Queue(10)
        download_producer = asyncio.create_task(self.download_url(self.parse_queue))
        parsed_results = self.parse_page(self.parse_queue, url)
        all_links = await asyncio.gather(download_producer,parsed_results)
        # breakpoint()
        logger.info(f"Completed processing {url}")
        return all_links

    async def download_url(self, parse_queue: Queue):
        """
            Periodically check the queue for new items requested for download.
            If there are new items, download them and add them to the queue.
            If there are no new items, sleep for a short period of time and check again.
        """
        downloader = SiteDownloader(host="localhost", port=7777)
        check_every = 0.2
        empty_count = 0
        max_empty_count = 10
        while self.running:
            url = self.get_page_to_visit()
            if url is not None:
                empty_count = 0
                print(f"Content received for {url} ")
                try:
                    self.visited.add(url)
                    content, status = downloader.get_page_elements(url)
                    await parse_queue.put((url, content))
                except Exception as e:
                    logger.error(f"Error downloading page {url}: {e}")
                    # on_download_failure(url)
                    return
                await asyncio.sleep(check_every)
            else:
                empty_count+=1
                if empty_count > max_empty_count:
                    self.running = False
                    logger.info(f"Completed processing {url}")
                    return
        logger.info(f"Completed processing {url}")
        return
        
    async def parse_page(self, parse_queue: Queue, url: str):
        """
            Parse the content of a page, extract urls.
            Pass extracted links back into queue for download
        """
        parser = Parser()
        while not parse_queue.empty():
            url, content = await parse_queue.get()
            print(f"Content received for {url} ")
            link_list = parser.get_links_from_content(url, content) # .get_links(content)
            if len(link_list) > 0:
                for link in link_list:
                    print(f"Link found: {link}")
                    self.add_page_to_visit(link)
            else:
                logger.info(f"No new links found for {url}")
            parse_queue.task_done()

    # def crawl(self):
    #     self.add_listener(self.download_channel)
    #     while self.running:
    #         for message in self.pubsub.listen():
    #             if message["type"] == "message":
    #                 url = message["data"].decode("utf-8")
    #                 self.process_url(url)

def main():
    queue_context = QueueContext(seed_url="https://overstory.com")
    test = asyncio.run(queue_context.process_url(url="https://overstory.com"))
    breakpoint()

if __name__ == "__main__":
    test = main()
    