from __future__ import annotations

import logging
import logging.config
import os
import sys

import yaml

cwd = os.getcwd()
loc = os.path.dirname(os.path.dirname(__file__))
sys.path.append(loc)
# sys.path.append(os.path.dirname(cwd))

log_config = os.environ.get(
    "SIMPLE_CRAWLER_LOG_CONFIG", f"{loc}/config/logging_config.yml"
)
sqlite_config = os.environ.get(
    "SIMPLE_CRAWLER_SQLITE_CONFIG", f"{loc}/config/sqlite.yml"
)

REDIS_PORT = os.environ.get("REDIS_PORT", 7777)
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
SQLITE_DB_FILE = os.environ.get("SQLITE_DB_FILE", "sqlite.db")
RDB_FILE = os.environ.get("RDB_FILE", "data.rdb")
DATA_DIR = os.environ.get("DATA_DIR", "data")

MAX_PAGES = os.environ.get("MAX_PAGES", 10)
RETRIES = os.environ.get("RETRIES", 3)
WRITE_TO_DB = os.environ.get("WRITE_TO_DB", True)
CHECK_EVERY = os.environ.get("CHECK_EVERY", 0.5)


def _load_console_log():
    with open(log_config) as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)


def get_logger(logger_name: str, log_file: str = None, log_level: int = logging.INFO):
    """
    Returns a logger with at least the default console handler.
    If log_file is provided, it will either:
     - add a file handler to the logger if one doesn't exist, or
     - repoint the file handler to a new file
    Likewise, if the log level is provided, it will update the log level of both handlers.
    """
    logger = logging.getLogger(logger_name)
    return_logger = logger
    has_file_handler = any(
        isinstance(handler, logging.FileHandler) for handler in logger.handlers
    )
    has_console_handler = any(
        isinstance(handler, logging.StreamHandler) for handler in logger.handlers
    )
    if not has_console_handler:
        _load_console_log()
        return_logger = logging.getLogger(logger_name)
        for handler in return_logger.handlers:
            handler.setLevel(log_level)
    if log_file is not None:
        if has_file_handler:
            file_handler = [
                x for x in logger.handlers if isinstance(x, logging.FileHandler)
            ][0]
            if file_handler.stream.name != log_file:
                file_handler.stream = open(log_file, "w")
            if file_handler.level != log_level:
                file_handler.setLevel(log_level)
            return return_logger
        else:
            file_handler = logging.FileHandler(log_file, "w")
            file_handler.setLevel(log_level)
            return_logger.addHandler(file_handler)
            return return_logger
    else:
        return return_logger


def _get_table_details():
    with open(sqlite_config) as f:
        config = yaml.safe_load(f.read())
    table_details = []
    for table_name, table_data in config["tables"].items():
        column_data = table_data.get("columns", {})
        columns = [x for x in column_data.keys()]
        types = [meta["sqlite_type"] for meta in column_data.values()]
        primary_key = [x for x in columns if column_data[x].get("primary_key", False)]
        unique_keys = [x for x in columns if column_data[x].get("unique", False)]

        table_details.append(
            {
                "db_file": table_data.get("db_file", "data/db.sqlite"),
                "table_name": table_name,
                "columns": columns,
                "types": types,
                "primary_key": primary_key,
                "unique_keys": unique_keys,
            }
        )
    return table_details
