# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

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

        if df.empty:
            logger.info("DataFrame is empty. Skipping load.")
            return

        if_exists = self.config.get("if_exists", "replace")
        if if_exists not in ["replace", "append"]:
            raise ValueError(f"Unsupported if_exists option: {if_exists}")

        logger.info(
            f"Loading dataframe into table: {table_name} with if_exists='{if_exists}'"
        )

        # Register the DataFrame as a virtual table to leverage DuckDB's performance
        self.connection.register("temp_df_view", df)

        # Ensure the target table exists with the correct schema
        self.connection.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM temp_df_view WHERE 1=0"
        )

        if if_exists == "replace":
            logger.info(f"Truncating table {table_name}.")
            self.connection.execute(f"TRUNCATE TABLE {table_name}")

        # Use INSERT INTO ... SELECT * FROM to load data
        self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df_view")

        # Unregister the virtual table to clean up
        self.connection.unregister("temp_df_view")

        logger.info("Dataframe loaded successfully.")
