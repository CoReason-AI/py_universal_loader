# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

import sqlite3
from typing import Any, Dict

import pandas as pd
from loguru import logger

from .base import BaseLoader


class SQLiteLoader(BaseLoader):
    """
    Loader for SQLite databases.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None

    def connect(self):
        """
        Establish and open the database connection.
        """
        db_path = self.config.get("db_path", ":memory:")
        logger.info(f"Connecting to SQLite database at: {db_path}")
        self.connection = sqlite3.connect(db_path)

    def close(self):
        """
        Terminate the database connection.
        """
        if self.connection:
            logger.info("Closing SQLite connection.")
            self.connection.close()
            self.connection = None

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Execute the entire data ingestion process.
        """
        if not self.connection:
            raise ConnectionError("Database connection is not established.")

        logger.info(f"Loading dataframe into table: {table_name}")
        df.to_sql(
            table_name, self.connection, if_exists="replace", index=False
        )
        logger.info("Dataframe loaded successfully.")
