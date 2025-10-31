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
        super().__init__(config)
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
        Load a DataFrame into a DuckDB table using a virtual table.
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
            f"Loading dataframe into table: \"{table_name}\" with if_exists='{if_exists}'"
        )

        view_name = f"__view_{table_name}_{id(df)}"

        try:
            # Register the DataFrame as a virtual table (view)
            self.connection.register(view_name, df)

            if if_exists == "replace":
                # Atomically create or replace the table with the data from the view
                self.connection.execute(
                    f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM {view_name}'
                )
            elif if_exists == "append":
                # Ensure the table exists, creating it from the view schema if it doesn't
                self.connection.execute(
                    f'CREATE TABLE IF NOT EXISTS "{table_name}" AS SELECT * FROM {view_name} WHERE 1=0'
                )
                # Insert the data from the view into the existing or new table
                self.connection.execute(
                    f'INSERT INTO "{table_name}" SELECT * FROM {view_name}'
                )

        finally:
            # Clean up the registered view
            self.connection.unregister(view_name)

        logger.info("Dataframe loaded successfully.")
