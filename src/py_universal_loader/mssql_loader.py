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
import numpy as np

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

    def _get_create_table_sql(
        self, df: pd.DataFrame, table_name: str, if_not_exists: bool = False
    ) -> str:
        """
        Generate a CREATE TABLE statement from a DataFrame for SQL Server.
        """
        type_mapping = {
            np.dtype("int64"): "BIGINT",
            np.dtype("int32"): "INT",
            np.dtype("float64"): "FLOAT",
            np.dtype("float32"): "REAL",
            np.dtype("bool"): "BIT",
            np.dtype("datetime64[ns]"): "DATETIME2",
            np.dtype("object"): "NVARCHAR(MAX)",
        }

        cols = []
        for col_name, dtype in df.dtypes.items():
            sql_type = type_mapping.get(dtype, "NVARCHAR(MAX)")
            cols.append(f"[{col_name}] {sql_type}")

        # SQL Server doesn't have "CREATE TABLE IF NOT EXISTS" prior to 2016.
        # A check for existence is better done separately.
        if if_not_exists:
            return f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' and xtype='U')
            CREATE TABLE [{table_name}] ({", ".join(cols)});
            """
        return f"CREATE TABLE [{table_name}] ({', '.join(cols)});"

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Execute the entire data ingestion process.
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

        staging_file_path = self.config.get("staging_file_path")
        if not staging_file_path:
            raise ValueError(
                "'staging_file_path' must be provided in the configuration for MSSQLLoader."
            )

        # Use a more robust separator and handle potential quotes in data
        df.to_csv(staging_file_path, index=False, header=True, sep="|", quotechar='"')

        try:
            with self.connection.cursor() as cursor:
                if if_exists == "replace":
                    logger.info(f"Dropping table {table_name} if it exists.")
                    cursor.execute(f"DROP TABLE IF EXISTS [{table_name}]")
                    create_table_sql = self._get_create_table_sql(df, table_name)
                    cursor.execute(create_table_sql)
                elif if_exists == "append":
                    create_table_sql = self._get_create_table_sql(
                        df, table_name, if_not_exists=True
                    )
                    cursor.execute(create_table_sql)

                bulk_insert_sql = f"""
                    BULK INSERT [{table_name}]
                    FROM '{staging_file_path}'
                    WITH (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = '|',
                        ROWTERMINATOR = '\\n',
                        TABLOCK
                    );
                """
                cursor.execute(bulk_insert_sql)
            self.connection.commit()
            logger.info("Successfully loaded data to MSSQL.")
        except Exception as e:
            logger.error(f"Failed to load data to MSSQL: {e}")
            if self.connection:
                self.connection.rollback()
            raise
