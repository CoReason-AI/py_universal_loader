# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

from typing import Any, Dict, Type

from loguru import logger

from .base import BaseLoader
from .duckdb_loader import DuckDBLoader
from .sqlite_loader import SQLiteLoader

# Mapping of db_type to loader class
LOADER_MAPPING: Dict[str, Type[BaseLoader]] = {
    "sqlite": SQLiteLoader,
    "duckdb": DuckDBLoader,
}


def get_loader(config: Dict[str, Any]) -> BaseLoader:
    """
    Factory function to get the correct database-specific loader object.

    Args:
        config: A dictionary containing the configuration for the loader.
                Must include a 'db_type' key.

    Returns:
        An instance of a BaseLoader subclass.

    Raises:
        ValueError: If the 'db_type' is not supported.
    """
    db_type = config.get("db_type")
    logger.info(f"Attempting to get loader for db_type: {db_type}")

    if db_type is None:
        raise ValueError("Configuration dictionary must contain a 'db_type' key.")

    loader_class = LOADER_MAPPING.get(db_type)

    if loader_class:
        return loader_class(config)

    raise ValueError(f"Unsupported database type: {db_type}")
