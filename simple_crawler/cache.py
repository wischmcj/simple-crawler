from __future__ import annotations

import json
from collections import defaultdict
from enum import Enum

import redis
from config.configuration import get_logger

logger = get_logger("data")


class CrawlStatus(Enum):
    """Enum for tracking URL crawl status"""

    ERROR = -2
    DISALLOWED = -1
    FRONTIER = 0
    DOWNLOADED = 1
    PARSED = 2
    CLOSED = 3


class CrawlTracker:
    """Track the status of a URL"""

    def __init__(
        self, redis_conn: redis.Redis, seed_url: str, run_id: str, max_pages: int
    ):
        self.rdb = redis_conn
        self.seed_url = seed_url
        self.run_id = run_id
        self.urls = defaultdict(dict)
        self.max_pages = max_pages
        self.limit_reached = False

    async def init_url_data(self, url: str) -> None:
        init_vals = {
            "attrs": {
                "seed_url": self.seed_url,
                "run_id": self.run_id,
                "crawl_status": 0,
                "max_pages": self.max_pages,
            }
        }
        await self.update_url(url, init_vals)

    async def close_url(self, url: str, pipe=None) -> None:
        if pipe is None:
            pipe = self.rdb.pipeline()
        # Create single transaction w/ multiple operations
        key = f"urls:{url}"
        pipe.incr("completed_pages").get("completed_pages")
        _, completed_pages = await pipe.execute()
        await self.rdb.publish("db", json.dumps({"key": key, "table_name": "urls"}))

        if int(completed_pages) >= self.max_pages:
            self.limit_reached = True
            await self.rdb.publish("db", "exit")

    async def update_url(self, url, url_data: dict, close=False) -> None:
        """
        Progresses the status of the URL through the crawl pipeline.
        If and error state is passed, the url is closed and removed from the cache.
        """
        key = f"urls:{url}"
        pipe = self.rdb.pipeline()
        # updated all fields to redis in a single transaction
        for field, value in url_data.items():
            if field == "attrs":
                pipe.hset(f"{key}:attrs", mapping=value)
            elif field == "linked_urls":
                pipe.lpush(f"{key}:linked_urls", *value)
            else:
                pipe.set(f"{key}:{field}", value)
        if not close:
            return await pipe.hincrby(url, "crawl_status").execute()
        else:
            await self.close_url(url, pipe)

    async def get_page_to_visit(self) -> list[str]:
        """Get all frontier seeds for a URL"""
        if self.limit_reached:
            logger.warning("Max pages reached, closing queue")
            return "exit"
        url = await self.rdb.lpop("to_visit")
        if url is not None:
            url = url.decode("utf-8")
        return url

    async def request_download(self, url: str) -> None:
        """Used to request that a page be downloaded"""
        is_new = await self.rdb.sadd("download_requests", url)
        if is_new:
            await self.init_url_data(url)
            is_new = await self.rdb.lpush("to_visit", url)
        return bool(is_new)

    async def request_parse(self, url: str) -> None:
        """Used to request that a page be parsed, and
        to ensure it has not already been parsed"""
        is_new = await self.rdb.sadd("parse_requests", url)
        if is_new:
            await self.init_url_data(url)
        return bool(is_new)

    async def get_cached_response(self, url: str):
        """Retrieve URL data from cache"""
        key = f"urls:{url}"
        bcontent = await self.rdb.get(f"{key}:content")
        content = bcontent.decode("utf-8") if bcontent else None
        return content
