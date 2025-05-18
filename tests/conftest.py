from __future__ import annotations

import logging
import os
import sys

cwd = os.getcwd()

# from mr_crawly.config.configuration import get_logger  # noqa

# logger = get_logger(__name__)
logger = logging.getLogger(__name__)

# Read in environment variables, set defaults if not present
root_loc = os.path.dirname(__file__)
loc = os.path.join(root_loc, "simple_crawler")
sys.path.append(loc)
sys.path.append(root_loc)
