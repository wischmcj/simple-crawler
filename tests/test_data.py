from __future__ import annotations

import sqlite3

import pytest
import redis

from simple_crawler.data import BaseTable, DatabaseManager

@pytest.fixture
def db_file():
    return "data/test.db"


@pytest.fixture
def conn(db_file):
    connection = sqlite3.connect(db_file)
    yield connection
    connection.close()


@pytest.fixture
def db_manager(db_file):
    manager = DatabaseManager(db_file=db_file)
    return manager


@pytest.fixture
def base_table(conn):
    table = BaseTable(conn)
    table.cursor.execute("DROP TABLE IF EXISTS test")
    table.table_name = "test"
    table.columns = ["id", "name"]
    table.types = ["INTEGER", "TEXT"]
    table.primary_key = "id"
    table.unique_keys = ["id"]
    table.create_table()
    yield table
    table.cursor.execute("DROP TABLE IF EXISTS test")

def url_data():
    return {
        "url": "http://example.com/page1",
        "content": "test content",
        "req_status": "200",
        "crawl_status": "frontier",
        "parent_url": "http://example.com",
        "seed_url": "http://example.com",
        "run_id": "1",
    }


@pytest.fixture
def incomplete_url_data():
    return {
        "seed_url": "https://www.overstory.com",
        "run_id": "2025_05_12_15_47_34",
        "crawl_status": "downloaded",
        "url": "https://www.overstory.com/blog",
        "req_status": 200,
    }


@pytest.fixture
def sitemap_data():
    return {
        "run_id": "1",
        "url": "https://www.google.com/gmail/sitemap.xml",
        "index_url": "https://www.google.com/gmail/sitemap.xml",
        "seed_url": "https://www.google.com/",
        "loc": "https://www.google.com/intl/am/gmail/about/",
        "priority": None,
        "frequency": None,
        "modified": None,
        "status": "Success",
    }


@pytest.fixture
def redis_conn():
    return redis.Redis(host="localhost", port=7777, decode_responses=False)


class TestBaseTable:
    def test_build_create_string(self, base_table):
        base_table.table_name = "test"
        base_table.columns = ["id", "name"]
        base_table.types = ["INTEGER", "TEXT"]
        base_table.primary_key = "id"
        base_table.unique_keys = ["id"]
        create_string = base_table.build_create_string()
        assert "CREATE TABLE IF NOT EXISTS test" in create_string
        assert "id INTEGER PRIMARY KEY AUTOINCREMENT" in create_string
        assert "name TEXT" in create_string
        assert "created_at TIMESTAMP" in create_string

    def test_create_table(self, base_table):
        base_table.table_name = "test"
        base_table.columns = ["id", "name"]
        base_table.types = ["INTEGER", "TEXT"]
        base_table.primary_key = "id"
        base_table.unique_keys = ["id"]

        base_table.create_table()
        # Verify table exists
        base_table.cursor.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='test';
        """
        )
        assert base_table.cursor.fetchone() is not None

    def test_execute_query_no_results(self, base_table):
        result = base_table.execute_query("SELECT * FROM test", return_results=False)
        assert result is True


# class MockPubSub:
#     def __init__(self):
#         self.messages = []

#     def listen(self):
#         try:
#             return self.messages.pop(0)
#         except IndexError:
#             return []


# class TestUrlBulkWriter:
#     def test_store_url(self, db_file):
#         pubsub = MockPubSub()
#         writer = UrlBulkWriter(pubsub, db_file, batch_size=2)

#         url_data = {"url": "http://example.com", "run_id": "1"}
#         writer.store_url(url_data)

#         # First URL should be buffered
#         assert len(writer.urls_to_write) == 1
#         assert writer.urls_to_write[0] == url_data

#         # Second URL should trigger flush
#         writer.store_url(url_data)
#         assert len(writer.urls_to_write) == 2

#         writer.store_url(url_data)
#         assert len(writer.urls_to_write) == 0  # Should have flushed

#     def test_flush(self, db_file):
#         pubsub = MockPubSub()
#         writer = UrlBulkWriter(pubsub, db_file)

#         url_data = {"url": "http://example.com", "run_id": "1"}
#         writer.store_url(url_data)
#         writer.store_url(url_data)

#         assert len(writer.urls_to_write) == 2
#         writer.flush_urls()
#         assert len(writer.urls_to_write) == 0
