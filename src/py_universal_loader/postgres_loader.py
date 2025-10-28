# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

from io import StringIO
from typing import Any, Dict

import pandas as pd
import psycopg2
from loguru import logger
import numpy as np

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
        conn_str = f"dbname='{self.config['dbname']}' user='{self.config['user']}' host='{self.config['host']}' password='{self.config['password']}'"
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

    def _get_sql_schema(self, df: pd.DataFrame, table_name: str) -> str:
        """
        Get the SQL schema for a given DataFrame.
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
        columns = ", ".join(
            [f"{col} {type_mapping.get(df[col].dtype, 'TEXT')}" for col in df.columns]
        )
        return f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Execute the entire data ingestion process.
        """
        if not self.connection:
            raise ConnectionError("Database connection is not established.")

        logger.info(f"Loading dataframe into table: {table_name}")

        # Create a string buffer
        sio = StringIO()
        sio.write(df.to_csv(index=None, header=False, sep="\t"))
        sio.seek(0)

        with self.connection.cursor() as cursor:
            # Create table
            create_table_sql = self._get_sql_schema(df, table_name)
            cursor.execute(create_table_sql)

            if_exists = self.config.get("if_exists", "replace")
            if if_exists == "replace":
                cursor.execute(f"TRUNCATE TABLE {table_name};")
            elif if_exists != "append":
                raise ValueError(f"Unsupported if_exists option: {if_exists}")

            # Use COPY FROM STDIN
            with sio as f:
                cursor.copy_expert(
                    f"COPY {table_name} FROM STDIN WITH CSV HEADER FALSE DELIMITER '\t'",
                    f,
                )

        self.connection.commit()
        logger.info("Dataframe loaded successfully.")
