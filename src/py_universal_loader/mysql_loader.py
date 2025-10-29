# Copyright (c) 2025 CoReason, Inc.
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

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Execute the entire data ingestion process.
        """
        if not self.connection:
            raise ConnectionError("Database connection is not established.")

        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as tmp:
            df.to_csv(tmp, index=False, header=False)
            tmp_path = tmp.name

        try:
            with self.connection.cursor() as cursor:
                # Assuming the table already exists
                cursor.execute(
                    f"""
                    LOAD DATA LOCAL INFILE '{tmp_path}'
                    INTO TABLE {table_name}
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
