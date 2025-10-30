# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

import io
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
        super().__init__(config)
        self.connection = None

    def connect(self):
        """
        Establish and open the database connection.
        """
        conn_str = (
            f"dbname='{self.config['db']}' user='{self.config['user']}' "
            f"host='{self.config['host']}' password='{self.config['password']}' "
            f"port='{self.config.get('port', 5432)}'"
        )
        logger.info(f"Connecting to PostgreSQL database at: {self.config['host']}")
        self.connection = psycopg2.connect(conn_str)

    def close(self):
        """
        Terminate the database connection.
        """
        if self.connection:
            logger.info("Closing PostgreSQL connection.")
            self.connection.close()
            self.connection = None

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Load a DataFrame into a PostgreSQL table using COPY FROM STDIN.
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

        with self.connection.cursor() as cursor:
            if if_exists == "replace":
                cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                create_table_sql = self._get_create_table_sql(df, table_name)
                cursor.execute(create_table_sql)
            elif if_exists == "append":
                # Ensure table exists for append
                create_table_sql = self._get_create_table_sql(
                    df, table_name, if_not_exists=True
                )
                cursor.execute(create_table_sql)

            # Use COPY FROM STDIN
            buffer = io.StringIO()
            # Use a more robust delimiter that is less likely to be in the data
            df.to_csv(buffer, index=False, header=False, sep='	', na_rep='\\N')
            buffer.seek(0)
            try:
                cursor.copy_expert(
                    f'COPY "{table_name}" FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\\\N\')',
                    buffer,
                )
                self.connection.commit()
                logger.info("DataFrame loaded successfully.")
            except Exception as e:
                self.connection.rollback()
                logger.error(f"Failed to load dataframe: {e}")
                raise

    def _get_create_table_sql(
        self, df: pd.DataFrame, table_name: str, if_not_exists: bool = False
    ) -> str:
        """
        Generate a CREATE TABLE SQL statement from a DataFrame.
        """
        type_mapping = {
            "int64": "BIGINT",
            "int32": "INTEGER",
            "float64": "DOUBLE PRECISION",
            "float32": "REAL",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
            "object": "TEXT",
        }
        columns = []
        for col_name, dtype in df.dtypes.items():
            sql_type = type_mapping.get(str(dtype), "TEXT")
            columns.append(f'"{col_name}" {sql_type}')

        create_clause = (
            "CREATE TABLE IF NOT EXISTS" if if_not_exists else "CREATE TABLE"
        )
        return f'{create_clause} "{table_name}" ({", ".join(columns)})'
