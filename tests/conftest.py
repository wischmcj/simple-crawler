from __future__ import annotations

import logging
import os
import sys
from threading import Thread
from unittest.mock import Mock

from fakeredis import TcpFakeServer
import pytest
from redis import Redis

server_address = ("localhost", 7777)
server = TcpFakeServer(server_address, server_type="redis")
t = Thread(target=server.serve_forever, daemon=True)
t.start()

logger = logging.getLogger(__name__)

# Read in environment variables, set defaults if not present
root_loc = os.path.dirname(os.path.dirname(__file__))
loc = os.path.join(root_loc, "simple_crawler")
sys.path.append(loc)
sys.path.append(root_loc)

from simple_crawler.manager import Manager  # noqa


DATA_DIR = "data"



def create_dir(dir_name, exist_ok=False):
    try:
        os.makedirs(dir_name, exist_ok=exist_ok)
    except FileExistsError:
        if exist_ok:
            pass
        else:
            raise FileExistsError(f"Directory '{dir_name}' already exists.")


@pytest.fixture
def redis_conn():
    r = Redis(host=server_address[0], port=server_address[1])
    yield r
    r.close()


class MockManager(Manager):
    def _init_db(self):
        # Initialize databases
        print(self.data_dir)
        self.db_manager = Mock()

    def _init_dirs(self):
        self.data_dir = os.path.join(os.path.dirname(__file__), DATA_DIR)
        create_dir(self.data_dir, exist_ok=True)
        self.data_dir = os.path.join(self.data_dir, f"{self.run_id}")
        create_dir(self.data_dir, exist_ok=True)
        self.rdb_path = os.path.join(self.data_dir, self.rdb_file)
        self.sqlite_path = os.path.join(self.data_dir, self.db_file)

    def _init_redis(self, host=None, port=None, redis_conn=None):
        self.rdb = redis_conn

    def _init_cache(self):
        self.cache = Mock()
        self.crawl_tracker = Mock()

    def _init_pubsub(self):
        url_channel = "db"
        # Initialize databases
        # url_pubsub = self.rdb.pubsub()
        url_pubsub = Mock()
        url_pubsub.subscribe(url_channel)
        self.url_pubsub = url_pubsub
        return url_pubsub

    def save_cache(self):
        pass


@pytest.fixture
def manager(redis_conn):
    manager = MockManager(
        seed_url="https://example.com",
        max_pages=10,
        retries=3,
        debug=False,
        db_file="test.db",
        redis_conn=redis_conn,
    )
    yield manager
    manager.shutdown()
