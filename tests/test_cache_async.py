from __future__ import annotations

import pytest
import redis
from cache import CrawlTracker, URLCache
from utils import deserialize


@pytest.fixture
def redis_conn():
    return redis.Redis(host="localhost", port=7777, decode_responses=False)


@pytest.fixture
def url_cache(redis_conn):
    return URLCache(redis_conn)


@pytest.fixture
def crawl_tracker(redis_conn):
    return CrawlTracker(redis_conn)


@pytest.fixture
def sample_url():
    return "https://example.com"


@pytest.fixture
def sample_html_content():
    return "<html>Test</html>"


@pytest.fixture
def sample_url_mapping(sample_url, sample_html_content):
    return {
        # "url": "http://example.com/public/",
        "seed_url": "http://example.com",
        "content": sample_html_content,
        "req_status": 200,
        "crawl_status": "downloaded",
        "run_id": "test_run",
        "max_pages": 100,
    }


def test_deserialize(redis_conn, sample_url, sample_url_mapping):
    set_resp = redis_conn.hset(sample_url, mapping=sample_url_mapping)
    assert set_resp == len(sample_url_mapping)

    res = deserialize(redis_conn.hgetall(sample_url))
    assert res == sample_url_mapping
