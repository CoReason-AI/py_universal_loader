# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_parquet_forget

import io
import uuid
from typing import Any, Dict

import boto3
import numpy as np
import pandas as pd
import pyodbc
from loguru import logger

from .base import BaseLoader


class DatabricksLoader(BaseLoader):
    """
    Loader for Databricks SQL.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection = None
        self.s3_client = None

    def connect(self):
        """
        Establish and open the database connection using pyodbc.
        """
        driver_path = self.config.get(
            "driver_path", "/Library/simba/spark/lib/libsparkodbc_sb64-universal.dylib"
        )
        conn_str = (
            f"Driver={driver_path};"
            f"Host={self.config['server_hostname']};"
            "Port=443;"
            f"HTTPPath={self.config['http_path']};"
            "SSL=1;"
            "ThriftTransport=2;"
            "AuthMech=3;"
            "UID=token;"
            f"PWD={self.config['access_token']}"
        )
        try:
            logger.info(f"Connecting to Databricks at host: {self.config['server_hostname']}")
            self.connection = pyodbc.connect(conn_str, autocommit=True)
            s3_config = self.config.get("s3", {})
            self.s3_client = boto3.client("s3", **s3_config)
            logger.info("Connections to Databricks and S3 established.")
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            logger.error(f"Failed to connect to Databricks: {sqlstate}")
            raise ConnectionError("Failed to connect to Databricks") from ex
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            raise ConnectionError("Failed to connect to S3") from e

    def close(self):
        """
        Terminate the database connection.
        """
        if self.connection:
            logger.info("Closing Databricks connection.")
            self.connection.close()
            self.connection = None

    def _get_sql_schema(self, df: pd.DataFrame, table_name: str) -> str:
        """
        Generate a CREATE TABLE statement from a DataFrame for Databricks.
        """
        type_mapping = {
            np.dtype("int64"): "BIGINT",
            np.dtype("int32"): "INT",
            np.dtype("float64"): "DOUBLE",
            np.dtype("float32"): "FLOAT",
            np.dtype("bool"): "BOOLEAN",
            np.dtype("datetime64[ns]"): "TIMESTAMP",
            np.dtype("object"): "STRING",
        }

        cols = []
        for col_name, dtype in df.dtypes.items():
            sql_type = type_mapping.get(dtype, "STRING")
            cols.append(f"`{col_name}` {sql_type}")

        return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)});"

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Stages a DataFrame to S3 as Parquet and loads it into Databricks using COPY INTO.
        """
        if not self.connection or not self.s3_client:
            raise ConnectionError("Connection is not established. Call connect() first.")

        if df.empty:
            logger.info("DataFrame is empty. Skipping load.")
            return

        s3_bucket = self.config.get("s3_bucket")
        if not s3_bucket:
            raise ValueError("'s3_bucket' must be specified in the config")

        iam_role_arn = self.config.get("iam_role_arn")
        if not iam_role_arn:
            raise ValueError("'iam_role_arn' must be specified in the config")

        s3_key = f"staging/{table_name}_{uuid.uuid4()}.parquet"
        s3_path = f"s3://{s3_bucket}/{s3_key}"
        logger.info(f"Staging DataFrame to S3 at: {s3_path}")

        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            self.s3_client.upload_fileobj(buffer, s3_bucket, s3_key)
            logger.info("Successfully uploaded staged file to S3.")
        except Exception as e:
            logger.error(f"Failed to upload staged file to S3: {e}")
            raise IOError("Failed to upload staged file to S3") from e

        try:
            with self.connection.cursor() as cursor:
                create_table_sql = self._get_sql_schema(df, table_name)
                cursor.execute(create_table_sql)

                copy_sql = f"""
                    COPY INTO {table_name}
                    FROM '{s3_path}'
                    WITH (CREDENTIAL (AWS_IAM_ROLE = '{iam_role_arn}'))
                    FILEFORMAT = PARQUET
                """
                logger.info(f"Executing COPY INTO command for table {table_name}.")
                cursor.execute(copy_sql)

            logger.info(f"Successfully loaded data into {table_name}. Deleting staged S3 file.")
            self.s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
        except Exception as e:
            logger.error(
                f"Failed to load data into Databricks. Staged file remains in S3: {s3_path}",
                exc_info=True,
            )
            raise IOError("Failed to load data into Databricks from S3") from e
