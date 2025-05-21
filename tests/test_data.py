from __future__ import annotations

import asyncio
import sqlite3
from typing import Literal
from unittest.mock import AsyncMock, patch

from fakeredis.aioredis import FakeRedis
import pytest
from redis import asyncio as redis_async

from simple_crawler.data import BaseTable, BulkDBWriter, DatabaseManager


@pytest.fixture
def db_file():
    return "data/test.db"


@pytest.fixture
def conn(db_file: Literal["data/test.db"]):
    connection = sqlite3.connect(db_file)
    yield connection
    connection.close()


@pytest.fixture
def db_manager(db_file: Literal["data/test.db"]):
    manager = DatabaseManager(db_file=db_file)
    return manager


@pytest.fixture
def base_table(conn: sqlite3.Connection):
    table = BaseTable(conn)
    return table


@pytest.fixture
def run():
    return {
        "run_id": "1",
        "seed_url": "http://example.com",
        "start_time": "2023-01-01 00:00:00",
        "max_pages": 100,
        "end_time": None,
    }


@pytest.fixture
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
def mock_redis():
    mock = AsyncMock(spec=redis_async.Redis)
    mock.pubsub = AsyncMock()
    mock.pubsub.return_value.get_message = AsyncMock()
    mock.pubsub.subscribe = AsyncMock()
    mock.publish = AsyncMock()
    return mock


@pytest.fixture
def mock_aiosqlite():
    with patch("aiosqlite.connect") as mock:
        mock = AsyncMock()
        mock_conn = AsyncMock()
        mock_exit = AsyncMock()
        mock_conn.executemany.return_value = True
        mock.return_value.__aenter__.return_value = mock_conn
        yield mock
        mock.return_value.__aexit__.return_value = mock_exit


class MockPubSub:
    def __init__(self, return_value, stop_on=2):
        self.return_value = return_value
        self.times_called = 0
        self.stop_on = stop_on

    async def get_message(self, *args, **kwargs):
        self.times_called += 1
        if self.times_called >= self.stop_on:
            return {"data": b"exit"}
        return self.return_value


class MockBulkDBWriter(BulkDBWriter):
    def __init__(self):
        table = AsyncMock(spec=BaseTable)
        table.table_name = "test_table"
        self.tables = {"test_table": table}
        super().__init__(self.tables)


class TestBaseTable:
    @pytest.fixture
    def base_table(self, mock_aiosqlite: AsyncMock):
        return BaseTable(
            "test.db",
            "test_table",
            ["id", "name", "value"],
            ["INTEGER", "TEXT", "TEXT"],
            "id",
            ["id"],
        )

    @pytest.mark.asyncio
    async def test_build_create_string(self, base_table: BaseTable):
        returned = await base_table.build_create_string()
        create_string, params = returned
        assert "CREATE TABLE IF NOT EXISTS test_table" in create_string
        assert "id INTEGER" in create_string
        assert "name TEXT" in create_string
        assert "value TEXT" in create_string
        assert "UNIQUE(id)" in create_string

    @pytest.mark.asyncio
    async def test_create_table(self, base_table: BaseTable, mock_aiosqlite: AsyncMock):
        task = asyncio.create_task(base_table.db_operation(operation="create"))
        await task
        assert task.result() is True

    @pytest.mark.asyncio
    async def test_build_insert_string(self, base_table: BaseTable):
        data = [
            {"name": "test1", "value": "value1"},
            {"name": "test2", "value": "value2"},
        ]
        query, params = await base_table.build_insert_string(data)
        assert "INSERT INTO test_table" in query
        assert len(params) == 2
        assert params[0] == ("test1", "value1")
        assert params[1] == ("test2", "value2")

    @pytest.mark.asyncio
    async def test_db_operation(self, base_table: BaseTable, mock_aiosqlite: AsyncMock):
        data = [{"name": "test1", "value": "value1"}]
        result = await base_table.db_operation(data)
        assert result is True

    @pytest.mark.asyncio
    async def test_execute_query(
        self, base_table: BaseTable, mock_aiosqlite: AsyncMock
    ):
        query = "SELECT * FROM test_table"
        result = await base_table.execute_query(query)
        assert result is True


class TestBulkDBWriter:
    @pytest.fixture
    def bulk_writer(self, mock_redis: AsyncMock):
        tables = {
            "test_table": BaseTable(
                "test.db", "test_table", ["id", "data"], ["INTEGER", "TEXT"]
            )
        }
        return BulkDBWriter(tables, batch_size=2)

    @pytest.fixture
    def no_flush_bulk_writer(self, mock_redis: AsyncMock):
        bulk_writer = MockBulkDBWriter()
        bulk_writer.flush_data = AsyncMock()
        return bulk_writer

    @pytest.mark.asyncio
    async def test_store_data_single(self, bulk_writer: BulkDBWriter):
        data = {"data": {"id": 1, "data": "test1"}}
        await bulk_writer.store_data("test_table", data)
        assert len(bulk_writer.to_write) == 1
        assert bulk_writer.to_write["test_table"][0] == data

    @pytest.mark.asyncio
    async def test_store_data_multiple(self, no_flush_bulk_writer: MockBulkDBWriter):
        data = []
        data.append({"data": {"id": 1, "data": "test1"}})
        data.append({"data": {"id": 2, "data": "test2"}})
        await no_flush_bulk_writer.store_data("test_table", data[0])
        await no_flush_bulk_writer.store_data("test_table", data[1])
        assert len(no_flush_bulk_writer.to_write["test_table"]) == 2
        assert no_flush_bulk_writer.to_write["test_table"][0] == data[0]
        assert no_flush_bulk_writer.to_write["test_table"][1] == data[1]

    @pytest.mark.asyncio
    async def test_flush_data(
        self, bulk_writer: BulkDBWriter, mock_aiosqlite: AsyncMock
    ):
        data = {"data": {"id": 1, "data": "test1"}}
        bulk_writer.to_write = {"test_table": [data]}
        _ = await bulk_writer.flush_data("test_table")
        assert bulk_writer.to_write == {"test_table": []}

    @pytest.mark.asyncio
    async def test_handle_message_stopword(
        self, bulk_writer: BulkDBWriter, mock_redis: AsyncMock
    ):
        return_value = {"data": b"exit"}
        channel = MockPubSub(return_value, stop_on=1)
        await bulk_writer.handle_message(channel)
        assert bulk_writer.running is False

    @pytest.mark.asyncio
    async def test_handle_message_data(self, no_flush_bulk_writer: MockBulkDBWriter):
        return_value = {
            "type": "message",
            "pattern": None,
            "channel": b"writer",
            "data": b'{"table_name": "test_table", "data": {"id": 1, "data": "test1"}}',
        }

        pubsub = MockPubSub(return_value, stop_on=2)
        await no_flush_bulk_writer.handle_message(pubsub)
        assert len(no_flush_bulk_writer.to_write["test_table"]) == 1


class TestDatabaseManager:
    @pytest.fixture
    def db_manager(
        self,
        mock_redis: AsyncMock,
        mock_aiosqlite: AsyncMock,
        async_redis_conn: FakeRedis,
    ):
        db_manager = DatabaseManager(mock_redis, "test.db")
        db_manager.redis_conn = async_redis_conn
        return db_manager

    @pytest.mark.asyncio
    async def test_init_db(self, db_manager: DatabaseManager):
        await db_manager._init_db()
        assert "runs" in db_manager.tables
        assert "urls" in db_manager.tables
        assert "sitemaps" in db_manager.tables
        await db_manager.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown(self, db_manager):
        await db_manager._init_db()
        await db_manager.shutdown()
        assert len(db_manager.listeners) > 0

    @pytest.mark.asyncio
    async def test_start_run_publishes_message(self, db_manager):
        await db_manager._init_db()
        db_manager.tables["runs"].execute_query = AsyncMock()
        run_id = "test_run"
        seed_url = "http://example.com"
        max_pages = 100
        _ = await db_manager.start_run(run_id, seed_url, max_pages)
        await db_manager.shutdown()
        assert (
            db_manager.tables["runs"].execute_query.call_args[0][0]
            == "INSERT INTO runs (run_id,seed_url,max_pages,event) VALUES (?,?,?,?)"
        )

    @pytest.mark.asyncio
    async def test_complete_run(self, db_manager):
        await db_manager._init_db()
        db_manager.tables["runs"].execute_query = AsyncMock()
        run_id = "test_run"
        seed_url = "http://example.com"
        max_pages = 100
        _ = await db_manager.complete_run(run_id, seed_url, max_pages)
        await db_manager.shutdown()
        assert (
            db_manager.tables["runs"].execute_query.call_args[0][0]
            == "INSERT INTO runs (run_id,seed_url,max_pages,event) VALUES (?,?,?,?)"
        )
