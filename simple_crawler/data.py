from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from threading import Event

from config.configuration import _get_table_details, get_logger
from helper_classes import BaseListener

logger = get_logger("data")
exit_event = Event()


class UrlBulkWriter(BaseListener):
    def __init__(self, pubsub, db_file, batch_size=5):
        """Initialize with a Redis pubsub object"""
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.pubsub = pubsub
        self.batch_size = batch_size
        self.urls_to_write = []
        self.url_db = UrlTable(self.conn)
        self.url_db.create_table()
        self.running: bool = True

    def store_url(self, url_data: dict) -> None:
        """Store URL data in database"""
        self.urls_to_write.append(url_data)
        logger.debug(f"Currently {len(self.urls_to_write)} urls to write")
        if len(self.urls_to_write) > self.batch_size:
            self.url_db.store_urls(self.urls_to_write)
            self.urls_to_write = []

    def get_urls_for_seed_url(self, seed_url: str) -> list[dict]:
        """Get all URL records for a given seed URL"""
        return self.url_db.get_urls_for_seed_url(seed_url)

    def get_urls_for_run(self, run_id: str) -> list[dict]:
        """Get all URL records for a given run ID"""
        return self.url_db.get_urls_for_run(run_id)

    def flush_urls(self) -> None:
        """Flush the URLs to the database"""
        logger.debug("Flushing URLs to database")
        self.url_db.store_urls(self.urls_to_write)
        self.urls_to_write = []
        logger.debug("Closing connection to database")

    def handle_message(self):
        while self.running:
            for message in self.pubsub.listen():
                logger.debug(f"Received message: {message}")
                if message["type"] == "message":
                    if message["data"] == b"exit":
                        self.running = False
                        break
                    else:
                        url_data = json.loads(message.get("data", ""))
                        self.store_url(url_data)
        self.flush_urls()
        exit_event.set()


class DatabaseManager:
    def __init__(self, url_pubsub, db_file="data/db.sqlite"):
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.db_file = db_file
        self.redis_conn = redis_conn
        self.tables = {}
        self._init_db()
        self.urls_to_write = []
        self.write_every = 10

    def _init_db(self):
        # Initialize databases
        self.table_details = _get_table_details()
        await self.create_tables()
        missing_tables = set(self.tables.keys()) - {"runs", "urls", "sitemaps"}
        if missing_tables:
            raise Exception(f"Missing tables: {missing_tables}")
        self.listeners = []
        self.add_listener(self.url_pubsub, UrlBulkWriter, (self.db_file,))

    def shutdown(self):
        """Shutdown the database manager"""
        logger.info("Shutting down database manager")
        self.conn.close()

    def add_listener(self, pubsub, listener_cls, args=()):
        logger.info(f"Subscribed to {pubsub}. Waiting for messages...")
        handler = listener_cls(pubsub, *args)
        handler.start()
        self.listeners.append(handler)

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
        self.string_functions = {
            "insert": self.build_insert_string,
            "update": self.build_update_string,
            "create": self.build_create_string,
        }

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

    def execute_query(self, query: str, params: tuple = ()):
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