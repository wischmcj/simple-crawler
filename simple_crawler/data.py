from __future__ import annotations

import json
import sqlite3

from config.configuration import _get_table_details, get_logger

logger = get_logger("data")

STOPWORD = b"exit"
RUNS_TABLE = "runs"


class BulkDBWriter:
    def __init__(self, tables: dict[str, BaseTable], batch_size=4):
        self.tables = tables
        self.batch_size = batch_size
        self.to_write = defaultdict(list)
        self.running = True

    async def store_data(self, table_name: str, data: dict) -> None:
        """Store URL data in database"""
        self.to_write[table_name].append(data)
        print(f"Currently {len(self.to_write[table_name])} rows to write")
        if len(self.to_write[table_name]) > self.batch_size:
            await self.flush_data(table_name)
            self.to_write[table_name] = []

    async def flush_data(self, table_name):
        """Builds and insert query and executes it"""
        logger.info("Flushing data...")
        tables = [(table_name, self.tables[table_name])] 
        if table_name == "all":
            tables = self.to_write.items()

        for table_name, data in tables:
            table = self.tables[table_name]
            result = await table.db_operation(data=data, operation="insert")
            self.to_write[table.table_name] = []
            return result

    async def handle_message(self, channel: redis.client.PubSub):
        while self.running:
            message = await channel.get_message(ignore_subscribe_messages=True)
            logger.debug(f"Received message: {message}")
            if message is not None:
                if message["data"] == STOPWORD:
                    self.running = False
                    break
                else:
                    request = await deserialize(message.get("data", ""))
                    kwargs = json.loads(request)
                    await self.store_data(**kwargs)
        await self.flush_data("all")


class DatabaseManager:
    def __init__(self, redis_conn, db_file="data/db.sqlite"):
        self.db_file = db_file
        self.redis_conn = redis_conn
        self.tables = {}
        self._init_db()

    def _init_db(self):
        # Initialize databases
        self.table_details = _get_table_details()
        self.create_tables()
        missing_tables = set(self.tables.keys()) - {"runs", "urls", "sitemaps"}
        if missing_tables:
            raise Exception(f"Missing tables: {missing_tables}")
        self.listeners = []
        self.futures = []
        self.add_listener(BulkDBWriter, (self.tables,))

    def shutdown(self):
        """Shutdown the database manager"""
        logger.info("Shutting down database manager")
        self.redis_conn.publish("writer", STOPWORD)

    def add_listener(self, listener_cls, args=()):
        pubsub = self.redis_conn.pubsub()
        pubsub.subscribe("writer")
        writer = listener_cls(*args)
        asyncio.run(writer.handle_message(pubsub))
        self.listeners.append(writer)

    def create_tables(self):
        for details in self.table_details:
            table = BaseTable(**details)
            table.db_operation(operation="create")
            self.tables[table.table_name] = table


class BaseTable:
    def __init__(
        self,
        conn,
        table_name: str = "",
        columns: list[str] = ["id", "col1", "col2", "col3"],
        types: list[str] = ["INTEGER", "INTEGER", "TEXT", "TEXT"],
        primary_key: str = "id",
        unique_keys: list[str] = ["id"],
    ):
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.table_name = table_name
        self.columns = columns
        self.types = types
        self.primary_key = primary_key
        self.unique_keys = unique_keys

    def build_create_string(self, *args):
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
        logger.debug(create_string)
        return create_string

    def create_table(self):
        """Create a table if it doesn't exist"""
        create_string = self.build_create_string()
        self.cursor.execute(create_string)
        self.conn.commit()

    def execute_query(
        self, query: str, params: tuple = (), return_results: bool = False
    ):
        """Execute a query"""
        logger.debug(f"Executing query: {query}")
        for i in range(3):
            try:
                self.cursor.execute(query, params)
                self.conn.commit()
                return True
            except sqlite3.OperationalError as e:
                logger.error(f"Integrity error: {e}")
                self.conn.rollback()
                time.sleep(1)
        return False
