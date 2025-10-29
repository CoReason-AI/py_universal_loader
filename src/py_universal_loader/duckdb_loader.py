# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_parquet_forget

import duckdb
from typing import Any, Dict

import pandas as pd
from loguru import logger

from .base import BaseLoader


class DuckDBLoader(BaseLoader):
    """
    Loader for DuckDB databases.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection: duckdb.DuckDBPyConnection | None = None

    def connect(self):
        """
        Establish and open the database connection.
        """
        db_path = self.config.get("db_path", ":memory:")
        logger.info(f"Connecting to DuckDB database at: {db_path}")
        self.connection = duckdb.connect(database=db_path, read_only=False)

    def close(self):
        """
        Terminate the database connection.
        """
        if self.connection:
            logger.info("Closing DuckDB connection.")
            self.connection.close()
            self.connection = None

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Execute the entire data ingestion process using DuckDB's efficient loading.
        """
        if not self.connection:
            raise ConnectionError("Database connection is not established.")

        logger.info(f"Loading dataframe into table: {table_name}")
        # Register the DataFrame as a virtual table
        self.connection.register("temp_df", df)

        # Use INSERT INTO ... SELECT * FROM to load data
        self.connection.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM temp_df WHERE 1=0"
        )
        self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        logger.info("Dataframe loaded successfully.")
