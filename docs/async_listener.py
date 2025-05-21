from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys

import aiosqlite
import redis.asyncio as redis

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
crawler_dir = os.path.join(parent_dir, "simple_crawler")

sys.path.append(parent_dir)
sys.path.append(crawler_dir)


from config.configuration import get_logger

from simple_crawler.utils import deserialize

STOPWORD = "exit"


logger = get_logger("main")


class BaseTable:
    def __init__(
        self,
        db_file,
        table_name: str = "",
        columns: list[str] = ["id", "col1", "col2", "col3"],
        types: list[str] = ["INTEGER", "INTEGER", "TEXT", "TEXT"],
        primary_key: str = "id",
        unique_keys: list[str] = ["id"],
    ):
        self.db_file = db_file
        self.table_name = table_name
        self.columns = columns
        self.types = types
        self.primary_key = primary_key
        self.unique_keys = unique_keys
        self.query_fuctions = {
            "select": self.create_select_query,
            "insert": self.create_insert_query,
            "create": self.build_create_string,
        }

    async def build_create_string(self, *args, **kwargs):
        """Build a create string for a table"""
        cols_to_types = dict(zip(self.columns, self.types))

        if len(self.unique_keys) == 0:
            self.unique_keys = [self.primary_key]
        unique_cols = ", ".join(self.unique_keys)
        pk = self.primary_key
        pk_row = {pk: f"{cols_to_types[pk]} PRIMARY KEY AUTOINCREMENT"}
        cols_to_types.update(pk_row)
        cols_w_types = [f"{col} {ctype}" for col, ctype in cols_to_types.items()]

        create_cols_string = ", ".join(cols_w_types)
        create_string = f"""CREATE TABLE IF NOT EXISTS {self.table_name} (
                                    {create_cols_string},
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                    UNIQUE({unique_cols})
                                )"""
        return create_string

    async def create_insert_query(self, columns: list[str], data: list[dict], *args):
        params = [
            tuple(row.get(col, "") for col in columns if col not in ["id"])
            for row in data
        ]
        placeholders = ",".join(["?" for _ in range(len(params[0]))])
        select_list = ",".join([col for col in columns if col not in ["id"]])
        insert_query = (
            f"INSERT INTO {self.table_name} ({select_list}) VALUES ({placeholders})"
        )
        return insert_query, params

    async def create_select_query(
        self, select_fields: list[str], value: str = None, field: str = None
    ) -> str:
        """Create a select query"""
        select_fields_str = ", ".join(select_fields)
        where = f"WHERE {field} = ?" if field is not None and value is not None else ""
        create_string = f"""SELECT {select_fields_str}
                                FROM {self.table_name}
                                {where}"""
        params = (value,) if value is not None else ()
        return create_string, params

    async def create_query(
        self, operation: str, columns: list[str], data: list[dict], field: str
    ):
        """Create a query"""
        if columns is None:
            if data is not None:
                columns = [col for col in data[0] if col not in ["id"]]
            else:
                columns = self.columns
        function = self.query_fuctions[operation]
        query, params = await function(columns, data, field)
        return query, params

    async def build_select_string(
        self, select_fields: list[str], value: str = None, field: str = None
    ) -> str:
        """Create a select query"""
        select_fields_str = ", ".join(select_fields)
        where = f"WHERE {field} = ?" if field is not None and value is not None else ""
        create_string = f"""SELECT {select_fields_str}
                                FROM {self.table_name}
                                {where}"""
        params = (value,) if value is not None else ()
        return create_string, params

    async def build_update_string(
        self, columns: list[str], data: list[dict], field: str, value: str
    ):
        params = [
            tuple(row.get(col, "") for col in columns if col not in ["id"])
            for row in data
        ]
        params.append(value)
        params = tuple(params)
        vals = {", ".join([f"{col} = ?" for col in columns if col not in ["id"]])}
        update_query = f"""UPDATE {self.table_name} SET
                            {vals}
                            WHERE {field} = ?;"""
        return update_query, params

    async def db_operation(self, operation: str, *args, **kwargs):
        """Create a table if it doesn't exist"""
        function = self.query_fuctions[operation]
        create_string, params = await function(*args, **kwargs)
        await self.execute_query(create_string, params=params)

    async def execute_query(
        self, query: str, params: tuple | list[tuple] = (), return_result=False
    ):
        """Execute a query"""
        print(f"Executing query: {query} w/ params: {params}")
        result = []
        for i in range(3):
            try:
                async with aiosqlite.connect(self.db_file) as db:
                    if return_result:
                        async with db.execute(query, params) as cursor:
                            async for row in cursor:
                                result.append(row)
                            return result
                    else:
                        if isinstance(params, list):
                            await db.executemany(query, params)
                        else:
                            await db.execute(query, params)
                        await db.commit()
                        return True
            except sqlite3.OperationalError as e:
                logger.error(f"Integrity error: {e}")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error: {e}")
                return Exception
        return False


class BulkDBWriter:
    def __init__(self, tables: list[BaseTable], batch_size=4):
        self.tables = {table.table_name: table for table in tables}
        self.batch_size = batch_size
        self.to_write = []
        self.running = True

    async def flush_data(self, table):
        """Builds and insert query and executes it"""
        if len(self.to_write) > 0:
            print("Flushing data...")
            result = await table.db_operation("insert", self.to_write)
            print("Successfully stored data")
            return result
        else:
            print("No data to store")
            return None

    async def store_data(self, row_data: dict, table: BaseTable) -> None:
        """Store URL data in database"""
        self.to_write.append(row_data)
        print(f"Currently {len(self.to_write)} rows to write")
        if len(self.to_write) > self.batch_size:
            await self.flush_data(table)
            self.to_write = []

    async def handle_message(self, channel: redis.client.PubSub):
        while self.running:
            message = await channel.get_message(ignore_subscribe_messages=True)
            if message is not None:
                print(f"Received message: {message}")
                if message["data"] == b"exit":
                    self.running = False
                    break
                else:
                    data = await deserialize(message.get("data", ""))
                    data = json.loads(data)
                    table_name = data["table"]
                    table = self.tables[table_name]
                    data = data["data"]
                    print(type(data))
                    await self.store_data(data, table)


r = redis.Redis(host="localhost", port=7777)


async def create_tables(data_file):
    test_table = BaseTable(data_file, "test_table")
    future = asyncio.create_task(test_table.create_table())
    await future

    table_name = "sitemaps"
    columns = [
        "id",
        "run_id",
        "seed_url",
        "url",
        "index_url",
        "loc",
        "priority",
        "frequency",
        "modified",
        "status",
    ]
    types = [
        "INTEGER",
        "TEXT",
        "TEXT",
        "TEXT",
        "TEXT",
        "TEXT",
        "TEXT",
        "TEXT",
        "TEXT",
        "TEXT",
        "TEXT",
    ]
    primary_key = "id"
    table_name = "urls"
    unique_keys = ["id"]
    sitemap_table = BaseTable(
        data_file, "sitemaps", columns, types, primary_key, unique_keys
    )
    future = asyncio.create_task(sitemap_table.create_table())
    await future

    columns = [
        "id",
        "seed_url",
        "url",
        "content",
        "req_status",
        "crawl_status",
        "run_id",
        "linked_urls",
    ]
    types = [
        "INTEGER",
        "TEXT",
        "TEXT",
        "BLOB",
        "TEXT",
        "TEXT",
        "TEXT",
        "BLOB",
    ]
    primary_key = "id"
    unique_keys = ["id"]
    url_table = BaseTable(data_file, "urls", columns, types, primary_key, unique_keys)
    future = asyncio.create_task(url_table.create_table())
    await future

    table_name = "runs"
    columns = [
        "id",
        "run_id",
        "seed_url",
        "start_time",
        "max_pages",
        "end_time",
    ]
    types = ["INTEGER", "TEXT", "TEXT", "DATETIME", "INTEGER", "DATETIME"]
    primary_key = "id"
    unique_keys = ["id", "run_id"]

    run_table = BaseTable(data_file, "runs", columns, types, primary_key, unique_keys)
    future = asyncio.create_task(run_table.create_table())
    await future

    return [test_table, sitemap_table, url_table, run_table]


async def main_pubsub(tables):
    test_table, sitemap_table, url_table, run_table = tables
    async with r.pubsub() as pubsub:
        await pubsub.subscribe("writer")
        writer = BulkDBWriter(tables)
        future = asyncio.create_task(writer.handle_message(pubsub))

        # INSERT_YOUR_CODE

        # Publish example data for the three new tables: runs, urls, sitemaps

        run_num = 10
        # Example data for 'runs' table
        for i in range(10):
            await r.publish(
                "writer",
                json.dumps(
                    {
                        "table": run_table.table_name,
                        "data": {
                            "run_id": f"run_{i+run_num}",
                            "seed_url": "https://example.com",
                            "start_time": "2024-06-01T12:00:00",
                            "max_pages": 100,
                            "end_time": None,
                        },
                    }
                ),
            )

        # Example data for 'urls' table
        for i in range(10):
            await r.publish(
                "writer",
                json.dumps(
                    {
                        "table": url_table.table_name,
                        "data": {
                            "seed_url": "https://example.com",
                            "url": f"https://example.com/page{i+run_num}",
                            "content": f"<html><body>Example Page {i}</body></html>",
                            "req_status": "200",
                            "crawl_status": "parsed",
                            "run_id": f"run_{i}",
                            "linked_urls": [
                                "https://example.com/page2",
                                "https://example.com/page3",
                            ],
                        },
                    }
                ),
            )

        # Example data for 'sitemaps' table
        for i in range(10):
            await r.publish(
                "writer",
                json.dumps(
                    {
                        "table": sitemap_table.table_name,
                        "data": {
                            "run_id": f"run_{i+run_num}",
                            "seed_url": "https://example.com",
                            "url": f"https://example.com/sitemap_{i}.xml",
                            "index_url": "https://example.com/sitemap_index.xml",
                            "loc": f"https://example.com/sitemap_{i}.xml",
                            "priority": "1.0",
                            "frequency": "daily",
                            "status": "active",
                        },
                    }
                ),
            )
        # await r.publish("channel:1", json.dumps({"col1": 1, "col2": "Hello", "col3": "World"}))
        # await r.publish("channel:1", json.dumps({"col1": 1, "col2": "Hello", "col3": "World"}))
        # await r.publish("channel:1", json.dumps({"col1": 1, "col2": "Hello", "col3": "World"}))
        # await asyncio.sleep(1)
        # await r.publish("channel:1", json.dumps({"col1": 1, "col2": "Hello", "col3": "World"}))
        # await r.publish("channel:1", json.dumps({"col1": 1, "col2": "Hello", "col3": "World"}))

        # future = asyncio.create_task( sitemap_table.select_by_field('run_id', 'run_0'))
        # test = await future

        await r.publish("writer", STOPWORD)
        await future
        print("SELECTING URLS")
        future = asyncio.create_task(run_table.select_by_field("run_id", "run_5"))
        test = await future
        print("SELECTING URLS")
        future = asyncio.create_task(run_table.select_all())
        test = await future
        print(test)

    #### RESULTS ####
    # (Reader) Message Received: {'type': 'message', 'pattern': None, 'channel': b'channel:1', 'data': b'Hello'}
    # (Reader) Doing something else
    # (Reader) Done sleeping {'type': 'message', 'pattern': None, 'channel': b'channel:1', 'data': b'Hello'}
    # (Reader) Message Received: {'type': 'message', 'pattern': None, 'channel': b'channel:2', 'data': b'World'}
    # (Reader) Done sleeping {'type': 'message', 'pattern': None, 'channel': b'channel:2', 'data': b'World'}
    # (Reader) Message Received: {'type': 'message', 'pattern': None, 'channel': b'channel:1', 'data': b'STOP'}
    # (Reader) Doing something else
    # (Reader) Done sleeping {'type': 'message', 'pattern': None, 'channel': b'channel:1', 'data': b'STOP'}
    # (Reader) STOP
    # (Reader) Doing something else
    # (venv) ╭─penguaman at penguaputer


if __name__ == "__main__":
    data_file = "data/test/db.sqlite"
    # if os.path.exists(data_file):
    #     os.remove(data_file)

    tables = asyncio.run(create_tables(data_file))
    asyncio.run(main_pubsub(tables))

    # add_listener(r, reader, BaseListener)
    # asyncio.run(main())
