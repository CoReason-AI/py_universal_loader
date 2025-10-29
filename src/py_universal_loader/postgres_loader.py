# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_parquet_forget

from io import StringIO
from typing import Any, Dict

import numpy as np
import pandas as pd
import psycopg2
from loguru import logger

from .base import BaseLoader


class PostgresLoader(BaseLoader):
    """
    Loader for PostgreSQL databases.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None

    def connect(self):
        """
        Establish and open the database connection.
        """
        conn_info = {
            "dbname": self.config.get("dbname"),
            "user": self.config.get("user"),
            "password": self.config.get("password"),
            "host": self.config.get("host"),
            "port": self.config.get("port", 5432),
        }
        logger.info(
            f"Connecting to PostgreSQL database at: {conn_info['host']}:{conn_info['port']}"
        )
        self.connection = psycopg2.connect(**conn_info)

    def close(self):
        """
        Terminate the database connection.
        """
        if self.connection:
            logger.info("Closing PostgreSQL connection.")
            self.connection.close()
            self.connection = None

    def _get_sql_schema(self, df: pd.DataFrame, table_name: str) -> str:
        """
        Generate a CREATE TABLE statement from a DataFrame.
        """
        type_mapping = {
            np.dtype("int64"): "BIGINT",
            np.dtype("int32"): "INTEGER",
            np.dtype("float64"): "DOUBLE PRECISION",
            np.dtype("float32"): "REAL",
            np.dtype("bool"): "BOOLEAN",
            np.dtype("datetime64[ns]"): "TIMESTAMP",
            np.dtype("object"): "TEXT",
        }

        cols = []
        for col_name, dtype in df.dtypes.items():
            sql_type = type_mapping.get(dtype, "TEXT")
            cols.append(f'"{col_name}" {sql_type}')

        return f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});'

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Load a DataFrame into a PostgreSQL table using COPY FROM STDIN.
        """
        if not self.connection:
            raise ConnectionError("Database connection is not established.")

        if df.empty:
            logger.info("DataFrame is empty. Skipping load.")
            return

        logger.info(f"Loading dataframe into table: {table_name}")

        with self.connection.cursor() as cursor:
            # Create table if it doesn't exist
            create_table_sql = self._get_sql_schema(df, table_name)
            cursor.execute(create_table_sql)

            # Handle if_exists logic
            if_exists = self.config.get("if_exists", "replace")
            if if_exists == "replace":
                logger.info(f"Truncating table {table_name}.")
                cursor.execute(f'TRUNCATE TABLE "{table_name}";')
            elif if_exists != "append":
                raise ValueError(f"Unsupported if_exists option: {if_exists}")

            # Use an in-memory buffer for the CSV data
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, sep=",")
            buffer.seek(0)

            try:
                cursor.copy_expert(
                    sql=f'COPY "{table_name}" FROM STDIN WITH (FORMAT CSV)',
                    file=buffer,
                )
                self.connection.commit()
                logger.info("Dataframe loaded successfully.")
            except Exception as e:
                self.connection.rollback()
                logger.error(f"Failed to load dataframe: {e}")
                raise
