from __future__ import annotations

import logging
import os
import sys
from threading import Thread

from fakeredis import TcpFakeServer
import pytest
import redis

server_address = ("localhost", 7777)
server = TcpFakeServer(server_address, server_type="redis")
t = Thread(target=server.serve_forever, daemon=True)
t.start()


@pytest.fixture
def redis_conn():
    r = redis.Redis(host=server_address[0], port=server_address[1])
    yield r
    r.close()


cwd = os.getcwd()

# from mr_crawly.config.configuration import get_logger  # noqa

# logger = get_logger(__name__)
logger = logging.getLogger(__name__)

# Read in environment variables, set defaults if not present
root_loc = os.path.dirname(__file__)
loc = os.path.join(root_loc, "simple_crawler")
sys.path.append(loc)
sys.path.append(root_loc)
