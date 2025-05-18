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
loc = os.path.join(root_loc, "mr_crawly")
sys.path.append(loc)
sys.path.append(root_loc)

from mr_crawly.manager import Manager  # noqa


# from mr_crawly.config.configuration import get_logger  # noqa


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

    def _init_redis(self, host=None, port=None, redis_conn=None):
        self.rdb = redis_conn


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
