# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

from typing import Any, Dict

import pandas as pd
import pyodbc
from loguru import logger

from .base import BaseLoader


class MSSQLLoader(BaseLoader):
    """
    Loader for Microsoft SQL Server databases.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection = None

    def connect(self):
        """
        Establish and open the database connection.
        """
        try:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.config['server']};DATABASE={self.config['database']};UID={self.config['username']};PWD={self.config['password']}"
            self.connection = pyodbc.connect(conn_str)
        except Exception as e:
            logger.error(f"Failed to connect to MSSQL: {e}")
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

        staging_file_path = self.config.get("staging_file_path")
        if not staging_file_path:
            raise ValueError(
                "'staging_file_path' must be provided in the configuration for MSSQLLoader."
            )

        df.to_csv(staging_file_path, index=False, header=False)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    BULK INSERT {table_name}
                    FROM '{staging_file_path}'
                    WITH (
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '\\n'
                    )
                    """
                )
            self.connection.commit()
        except Exception as e:
            logger.error(f"Failed to load data to MSSQL: {e}")
            if self.connection:
                self.connection.rollback()
            raise
