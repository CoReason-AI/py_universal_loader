# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

import os
import tempfile
from typing import Any, Dict

import mysql.connector
import numpy as np
import pandas as pd
from loguru import logger

from .base import BaseLoader


class MySQLLoader(BaseLoader):
    """
    Loader for MySQL/MariaDB databases.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection = None
        self._temp_file_path = None

    def connect(self):
        """
        Establish and open the database connection.
        """
        conn_info = {
            "user": self.config.get("user"),
            "password": self.config.get("password"),
            "host": self.config.get("host"),
            "database": self.config.get("database"),
            "port": self.config.get("port", 3306),
            "allow_local_infile": True,
        }
        logger.info(
            f"Connecting to MySQL database at: {conn_info['host']}:{conn_info['port']}"
        )
        self.connection = mysql.connector.connect(**conn_info)

    def close(self):
        """
        Terminate the database connection.
        """
        if self.connection:
            logger.info("Closing MySQL connection.")
            self.connection.close()
            self.connection = None

    def _cleanup_temp_file(self):
        """
        Remove the temporary file if it exists.
        """
        if self._temp_file_path and os.path.exists(self._temp_file_path):
            os.remove(self._temp_file_path)
            self._temp_file_path = None

    def _get_sql_schema(self, df: pd.DataFrame, table_name: str) -> str:
        """
        Generate a CREATE TABLE statement from a DataFrame's dtypes.
        """
        type_mapping = {
            np.dtype("int64"): "BIGINT",
            np.dtype("int32"): "INTEGER",
            np.dtype("float64"): "DOUBLE",
            np.dtype("float32"): "FLOAT",
            np.dtype("bool"): "BOOLEAN",
            np.dtype("datetime64[ns]"): "DATETIME",
            np.dtype("object"): "TEXT",
        }

        cols = []
        for col_name, dtype in df.dtypes.items():
            sql_type = type_mapping.get(dtype, "TEXT")
            cols.append(f"`{col_name}` {sql_type}")

        return f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(cols)});"

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Load a DataFrame into a MySQL table using LOAD DATA LOCAL INFILE.
        """
        if not self.connection:
            raise ConnectionError("Database connection is not established.")

        if df.empty:
            logger.info("DataFrame is empty. Skipping load.")
            return

        logger.info(f"Loading dataframe into table: {table_name}")

        with self.connection.cursor() as cursor:
            create_table_sql = self._get_sql_schema(df, table_name)
            cursor.execute(create_table_sql)

            if_exists = self.config.get("if_exists", "replace")
            if if_exists == "replace":
                logger.info(f"Truncating table {table_name}.")
                cursor.execute(f"TRUNCATE TABLE `{table_name}`;")
            elif if_exists != "append":
                raise ValueError(f"Unsupported if_exists option: {if_exists}")

            with tempfile.NamedTemporaryFile(
                mode="w+", delete=False, suffix=".csv", encoding="utf-8"
            ) as temp_f:
                self._temp_file_path = temp_f.name
                df.to_csv(self._temp_file_path, index=False, header=False)

            load_sql = f"""
                LOAD DATA LOCAL INFILE '{self._temp_file_path}'
                INTO TABLE `{table_name}`
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
            """

            try:
                cursor.execute(load_sql)
                self.connection.commit()
                logger.info("Dataframe loaded successfully.")
            except Exception as e:
                self.connection.rollback()
                logger.error(f"Failed to load dataframe: {e}")
                raise
            finally:
                self._cleanup_temp_file()
