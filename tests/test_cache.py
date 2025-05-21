from __future__ import annotations

import pytest
import json
from unittest.mock import AsyncMock

from simple_crawler.utils import deserialize
from simple_crawler.cache import CrawlStatus, CrawlTracker


@pytest.fixture
def crawl_tracker(async_redis_conn):
    return CrawlTracker(async_redis_conn, "http://example.com", "test_run", 100)


@pytest.fixture
def sample_url():
    return "http://example.com/public/"


@pytest.fixture
def sample_html_content():
    return "<html>Test</html>"


@pytest.fixture
def sample_url_attrs():
    return {
        "seed_url": "http://example.com",
        "req_status": 200,
        "crawl_status": 0,
        "run_id": "run_0",
        "max_pages": 100,
    }


@pytest.fixture
def sample_linked_urls():
    return [
        "https://github.com/hadialqattan/pycln",
        "https://github.com/asottile/pyupgrade",
        "https://github.com/codespell-project/codespell",
    ]


@pytest.fixture
def sample_failed_url_data(sample_url):
    return {
        "url": sample_url,
        "req_status": 404,
        "crawl_status": CrawlStatus.ERROR.value,
        "run_id": "test_run",
    }


@pytest.mark.asyncio
async def test_init_url_data(crawl_tracker, sample_url):
    """Test initializing URL data in the tracker"""
    await crawl_tracker.init_url_data(sample_url)

    # Check that URL data was initialized correctly
    key = f"urls:{sample_url}"
    test = await crawl_tracker.rdb.hgetall(f"{key}:attrs")
    test = await deserialize(test)
    assert test["seed_url"] == crawl_tracker.seed_url
    assert test["run_id"] == crawl_tracker.run_id
    assert int(test["crawl_status"]) == 0
    assert int(test["max_pages"]) == crawl_tracker.max_pages


@pytest.mark.asyncio
async def test_request_download(crawl_tracker, sample_url):
    """Test requesting a URL for download"""

    # First request should return True (new URL)
    is_new = await crawl_tracker.request_download(sample_url)
    assert is_new is True

    is_new = await crawl_tracker.request_download(sample_url)
    assert is_new is False

    # Verify sadd was called with correct arguments
    # crawl_tracker.rdb.sadd.assert_called_with("download_requests", sample_url)
    test = await crawl_tracker.rdb.smembers("download_requests")
    assert sample_url == test.pop().decode("utf-8")


@pytest.mark.asyncio
async def test_get_page_to_visit(crawl_tracker, sample_url):
    """Test getting next page to visit from queue"""
    # Mock lpop to return URL

    # Get next URL
    next_url = await crawl_tracker.get_page_to_visit()
    assert next_url == sample_url

    other_url = "https://github.com/hadialqattan/pycln"
    is_new = await crawl_tracker.request_download(other_url)
    assert is_new

    # Queue should be empty
    next_url = await crawl_tracker.get_page_to_visit()
    assert next_url == other_url

    next_url = await crawl_tracker.get_page_to_visit()
    assert next_url is None


@pytest.mark.asyncio
async def test_request_parse(crawl_tracker, sample_url):
    """Test requesting a URL for parsing"""

    # First request should return True (new URL)
    is_new = await crawl_tracker.request_parse(sample_url)
    assert is_new is True

    is_new = await crawl_tracker.request_parse(sample_url)
    assert is_new is False

    test = await crawl_tracker.rdb.smembers("parse_requests")
    assert sample_url == test.pop().decode("utf-8")


@pytest.mark.asyncio
async def test_update_url(crawl_tracker, sample_url):
    """Test updating URL data"""
    url_data = {
        "attrs": {"crawl_status": CrawlStatus.DOWNLOADED.value, "req_status": 200},
        "linked_urls": ["http://example.com/1", "http://example.com/2"],
        "content": "<html>Test</html>",
    }

    await crawl_tracker.update_url(sample_url, url_data)

    key = f"urls:{sample_url}"
    b_data = await crawl_tracker.rdb.hgetall(f"{key}:attrs")
    data = await deserialize(b_data)
    assert data["seed_url"] == crawl_tracker.seed_url
    assert data["run_id"] == crawl_tracker.run_id
    assert int(data["crawl_status"]) == 1
    assert int(data["max_pages"]) == crawl_tracker.max_pages

    linked_urls = await crawl_tracker.rdb.lrange(f"{key}:linked_urls", 0, -1)
    urls = [url.decode("utf-8") for url in linked_urls]
    assert all([x in urls for x in url_data["linked_urls"]])

    content = await crawl_tracker.rdb.get(f"{key}:content")
    assert content.decode("utf-8") == url_data["content"]
    await crawl_tracker.rdb.delete(key)


@pytest.mark.asyncio
async def test_close_url(crawl_tracker, sample_url):
    """Test closing a URL and publishing its data"""

    crawl_tracker.rdb.publish = AsyncMock()
    crawl_tracker.rdb.publish.return_value = True

    await crawl_tracker.close_url(sample_url)

    completed_pages = await crawl_tracker.rdb.get("completed_pages")
    assert int(completed_pages) == 1

    await crawl_tracker.close_url(sample_url)

    completed_pages = await crawl_tracker.rdb.get("completed_pages")
    assert int(completed_pages) == 2

    # Verify publish was called with correct data
    key = f"urls:{sample_url}"
    assert crawl_tracker.rdb.publish.call_args[0] == (
        "db",
        json.dumps({"key": key, "table_name": "urls"}),
    )


@pytest.mark.asyncio
async def test_max_pages_limit(crawl_tracker, sample_url):
    """Test that crawler stops when max pages is reached"""
    # Set max pages to 1
    crawl_tracker.max_pages = 1

    # Mock pipeline for close_url
    crawl_tracker.rdb.set("completed_pages", 2)

    # Close URL to increment completed pages
    await crawl_tracker.close_url(sample_url)

    # Try to get next page
    next_url = await crawl_tracker.get_page_to_visit()
    assert next_url == "exit"  # Should return exit signal
    assert crawl_tracker.limit_reached is True


@pytest.mark.asyncio
async def test_url_cache_get_cached_response(
    crawl_tracker, sample_url, sample_html_content
):
    """Test retrieving URL data from cache"""
    # Mock hget to return content and status
    url_data = {
        "attrs": {"crawl_status": CrawlStatus.DOWNLOADED.value, "req_status": 200},
        "content": "<html>Test</html>",
    }

    await crawl_tracker.update_url(sample_url, url_data)

    content = await crawl_tracker.get_cached_response(sample_url)
    assert content == sample_html_content
