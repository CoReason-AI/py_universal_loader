# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_parquet_forget

import os
import tempfile
from typing import Any, Dict

import numpy as np
import pandas as pd
import mysql.connector
from loguru import logger

from .base import BaseLoader


class MySQLLoader(BaseLoader):
    """
    Loader for MySQL/MariaDB databases.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection = None

    def connect(self):
        """
        Establish and open the database connection.
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.config["host"],
                port=self.config.get("port", 3306),
                user=self.config["user"],
                password=self.config["password"],
                database=self.config["database"],
                allow_local_infile=True,
            )
        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            raise

    def close(self):
        """
        Terminate the database connection.
        """
        if self.connection:
            self.connection.close()
            self.connection = None

    def _get_sql_schema(self, df: pd.DataFrame, table_name: str) -> str:
        """
        Generate a CREATE TABLE statement from a DataFrame.
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
            cols.append(f'`{col_name}` {sql_type}')

        return f'CREATE TABLE IF NOT EXISTS `{table_name}` ({", ".join(cols)});'

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Execute the entire data ingestion process.
        """
        if not self.connection:
            raise ConnectionError("Database connection is not established.")

        if df.empty:
            logger.info("DataFrame is empty. Skipping load.")
            return

        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as tmp:
            df.to_csv(tmp, index=False, header=False)
            tmp_path = tmp.name

        try:
            with self.connection.cursor() as cursor:
                create_table_sql = self._get_sql_schema(df, table_name)
                cursor.execute(create_table_sql)

                if_exists = self.config.get("if_exists", "replace")
                if if_exists == "replace":
                    logger.info(f"Truncating table {table_name}.")
                    cursor.execute(f'TRUNCATE TABLE `{table_name}`;')
                elif if_exists != "append":
                    raise ValueError(f"Unsupported if_exists option: {if_exists}")

                cursor.execute(
                    f"""
                    LOAD DATA LOCAL INFILE '{tmp_path}'
                    INTO TABLE `{table_name}`
                    FIELDS TERMINATED BY ','
                    LINES TERMINATED BY '\\n'
                    """
                )
            self.connection.commit()
        except Exception as e:
            logger.error(f"Failed to load data to MySQL: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            os.remove(tmp_path)
