from __future__ import annotations

import asyncio
import json
import sqlite3
from collections import defaultdict

import aiosqlite
from config.configuration import _get_table_details, get_logger
from redis import asyncio as redis
from utils import deserialize

logger = get_logger("data")

STOPWORD = b"exit"
RUNS_TABLE = "runs"


class BulkDBWriter:
    def __init__(self, tables: dict[str, BaseTable], redis_conn, batch_size=4):
        self.tables = tables
        self.redis_conn = redis_conn
        self.batch_size = batch_size
        self.to_write = defaultdict(list)
        self.running = True

    async def store_data(self, table_name: str, data: dict = None, key: str = None) -> None:
        """Store URL data in database"""
        if key is not None:
            data = await self.process_key(key)
        self.to_write[table_name].append(data)
        print(f"Currently {len(self.to_write[table_name])} rows to write")
        if len(self.to_write[table_name]) > self.batch_size:
            await self.flush_data(table_name)
            self.to_write[table_name] = []

    async def flush_data(self, table_name):
        """Builds and insert query and executes it"""
        if table_name == "all":
            tables = self.to_write.items()
        else:
            tables = [(table_name, self.to_write[table_name])]
        print("Flushing data...")

        for table_name, data in tables:
            table = self.tables[table_name]
            result = await table.db_operation(data=data, operation="insert")
            self.to_write[table_name] = []
            return result
    
    async def process_key(self, key: str):
        pipe = self.redis_conn.pipeline()
        pipe.lrange(f"{key}:linked_urls", 0, -1)
        pipe.hgetall(f"{key}:attrs")
        pipe.get(f"{key}:content")
        pipe.delete(key)
        linked_urls, attrs, content, _ = await pipe.execute()
        url_data = await deserialize(attrs) # is this needed?
        url_data['linked_urls'] = [url.decode("utf-8") for url in linked_urls]
        url_data['content'] = content.decode("utf-8")
        return url_data

    async def handle_message(self, channel: redis.client.PubSub):
        while self.running:
            message = await channel.get_message(ignore_subscribe_messages=True)
            logger.debug(f"Received message: {message}")
            if message is not None:
                if message["data"] == STOPWORD:
                    await self.flush_data("all")
                    self.running = False
                    break
                kwargs = json.loads(message.get("data", b'{}'))
                await self.store_data(**kwargs)


class DatabaseManager:
    def __init__(self, redis_conn, db_file="data/db.sqlite"):
        self.db_file = db_file
        self.redis_conn = redis_conn
        self.tables = {}
        self._init_db()

    async def _init_db(self):
        # Initialize databases
        self.table_details = _get_table_details()
        await self.create_tables()
        missing_tables = set(self.tables.keys()) - {"runs", "urls", "sitemaps"}
        if missing_tables:
            raise Exception(f"Missing tables: {missing_tables}")
        self.listeners = []
        self.futures = []
        await self.add_listener(BulkDBWriter, (self.tables, self.redis_conn))

    async def shutdown(self):
        """Shutdown the database manager"""
        logger.info("Shutting down database manager")
        try:
            await self.redis_conn.publish("writer", STOPWORD)
        except Exception as e:
            logger.info(f"Unable to publish stop message to DB writer: {e}")
        await asyncio.gather(*self.futures)

    async def add_listener(self, listener_cls, args=()):
        pubsub = self.redis_conn.pubsub()
        await pubsub.subscribe("writer")
        writer = listener_cls(*args)
        future = asyncio.create_task(writer.handle_message(pubsub))
        self.listeners.append(writer)
        self.futures.append(future)

    async def create_tables(self):
        for details in self.table_details:
            table = BaseTable(**details)
            await table.db_operation(operation="create")
            self.tables[table.table_name] = table

    async def start_run(self, run_id: str, seed_url: str, max_pages: int) -> int:
        """Start a new crawl run"""
        data = {
            "run_id": run_id,
            "seed_url": seed_url,
            "max_pages": max_pages,
            "event": "start_run",
        }
        await self.redis_conn.publish(
            "writer", json.dumps({"table_name": RUNS_TABLE, "data": data})
        )

    async def complete_run(self, run_id: str, seed_url: str, max_pages: int):
        """Mark a run as completed and set end time"""
        data = {
            "run_id": run_id,
            "seed_url": seed_url,
            "max_pages": max_pages,
            "event": "complete_run",
        }
        logger.info(f"Completing run {run_id}")
        await self.redis_conn.publish(
            "writer", json.dumps({"table_name": RUNS_TABLE, "data": data})
        )


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
        self.string_functions = {
            "insert": self.build_insert_string,
            "update": self.build_update_string,
            "create": self.build_create_string,
        }

    async def build_create_string(self, *args):
        """Build a create string for a table"""
        cols_to_types = dict(zip(self.columns, self.types))

        if len(self.unique_keys) == 0:
            self.unique_keys = [self.primary_key]
        unique_cols = ", ".join(self.unique_keys)
        cols_w_types = [f"{col} {ctype}" for col, ctype in cols_to_types.items()]
        create_cols_string = ", ".join(cols_w_types)
        create_string = f"""CREATE TABLE IF NOT EXISTS {self.table_name} (
                                    {create_cols_string},
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                    UNIQUE({unique_cols})
                                )"""
        return create_string, ()

    async def build_insert_string(self, data: list[dict]):
        columns = [k for k, v in data[0].items() if k not in ["id"]]
        params = [tuple(row.get(col, "") for col in columns) for row in data]
        placeholders = ",".join(["?" for _ in columns])
        select_list = ",".join(columns)
        insert_query = (
            f"INSERT INTO {self.table_name} ({select_list}) VALUES ({placeholders})"
        )
        return insert_query, params

    async def build_update_string(self, data: list[dict]):
        columns = [k for k, v in data[0].items() if k not in ["id", "created_at"]]
        params = [tuple(row.get(col, "") for col in columns) for row in data]
        placeholders = ", ".join([f"{col} = ?" for col in columns])
        placeholders.append(data[0]["id"])
        query = f"""UPDATE {self.table_name}
              SET {placeholders}
              WHERE {self.primary_key} = ?"""
        return query, params

    async def db_operation(self, data: list[dict] = None, operation: str = "insert"):
        """Create a table if it doesn't exist"""
        query, params = await self.string_functions[operation](data)
        result = await self.execute_query(query, params=params)
        if operation == "insert" and result is False:
            self.db_operation(data, "update")
            logger.warning(f"Row already exists, updated {self.table_name}")
        return result

    async def execute_query(self, query: str, params: tuple | list[tuple] = ()):
        """Execute a query"""
        logger.debug(f"Executing query: {query} w/ params: {params}")
        for _ in range(3):
            try:
                async with aiosqlite.connect(self.db_file) as db:
                    func = db.executemany if isinstance(params, list) else db.execute
                    await func(query, params)
                    await db.commit()
                    return True
            except sqlite3.OperationalError as e:
                logger.error(f"Integrity error: {e}")
                await asyncio.sleep(1)
        return False
