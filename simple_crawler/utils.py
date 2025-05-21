from __future__ import annotations

import json
import os
from urllib.parse import urlparse

from config.configuration import get_logger

logger = get_logger(__name__)


def parse_url(url: str):
    """Parse a url and return the page elements"""
    parsed_url = urlparse(url)
    return parsed_url.scheme, parsed_url.netloc, parsed_url.path


def create_dir(dir_name, exist_ok=False):
    # Method 1: Using os.mkdir() to create a single directory
    try:
        os.mkdir(dir_name)
        logger.info(f"Directory '{dir_name}' created successfully.")
    except FileExistsError:
        if exist_ok:
            logger.info(f"Directory '{dir_name}' already exists.")
        else:
            raise FileExistsError(f"Directory '{dir_name}' already exists.")
    except PermissionError as err:
        logger.info(f"Permission denied: Unable to create '{dir_name}'.")
        raise err


async def deserialize(in_obj):
    """
    Recursively decode a redis hgetall response.
    """
    if isinstance(in_obj, list):
        ret = list()
        for item in in_obj:
            await ret.append(deserialize(item))
        return ret
    elif isinstance(in_obj, dict):
        ret = dict()
        for key in in_obj:
            ret[key.decode()] = await deserialize(in_obj[key])
        return ret
    elif isinstance(in_obj, bytes):
        # Assume string encoded w/ utf-8
        return in_obj.decode("utf-8")
    else:
        raise Exception(f"type not handled: {type(in_obj)}")


async def serialize(mapping: dict):
    for k, v in mapping.items():
        if isinstance(v, dict) or isinstance(v, list):
            mapping[k] = json.dumps(v)
        if isinstance(v, str):
            mapping[k] = v.encode("utf-8")
    return mapping
