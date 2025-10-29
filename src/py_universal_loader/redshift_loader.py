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
import psycopg2
from loguru import logger

from .base import BaseLoader


class RedshiftLoader(BaseLoader):
    """
    Loader for Amazon Redshift.

    Stages a pandas DataFrame as a Parquet file to a specified S3 bucket and
    executes a Redshift COPY command to ingest the data.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection = None
        self.s3_client = None

    def connect(self):
        """
        Establish connections to Redshift and S3.
        """
        conn_info = {
            "dbname": self.config.get("dbname"),
            "user": self.config.get("user"),
            "password": self.config.get("password"),
            "host": self.config.get("host"),
            "port": self.config.get("port", 5439),
        }
        logger.info(
            f"Connecting to Redshift database at: {conn_info['host']}:{conn_info['port']}"
        )
        try:
            self.connection = psycopg2.connect(**conn_info)
            s3_config = self.config.get("s3", {})
            self.s3_client = boto3.client("s3", **s3_config)
            logger.info("Connections to Redshift and S3 established.")
        except Exception as e:
            logger.error(f"Failed to connect to Redshift or S3: {e}")
            raise ConnectionError("Failed to connect to Redshift or S3") from e

    def close(self):
        """
        Terminate the Redshift database connection.
        """
        if self.connection:
            logger.info("Closing Redshift connection.")
            self.connection.close()
            self.connection = None

    def _get_sql_schema(self, df: pd.DataFrame, table_name: str) -> str:
        """
        Generate a CREATE TABLE statement from a DataFrame for Redshift.
        """
        type_mapping = {
            np.dtype("int64"): "BIGINT",
            np.dtype("int32"): "INTEGER",
            np.dtype("float64"): "FLOAT8",
            np.dtype("float32"): "FLOAT4",
            np.dtype("bool"): "BOOLEAN",
            np.dtype("datetime64[ns]"): "TIMESTAMP",
            np.dtype("object"): "VARCHAR(65535)",  # Max varchar size in Redshift
        }

        cols = []
        for col_name, dtype in df.dtypes.items():
            sql_type = type_mapping.get(dtype, "VARCHAR(65535)")
            cols.append(f'"{col_name}" {sql_type}')

        return f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});'

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Stages a DataFrame to S3 as Parquet and loads it into Redshift using COPY.

        The temporary Parquet file in S3 is deleted only after a successful
        COPY operation.
        """
        if not self.connection or not self.s3_client:
            raise ConnectionError("Connection is not established. Call connect() first.")

        if df.empty:
            logger.info("DataFrame is empty. Skipping load.")
            return

        s3_bucket = self.config.get("s3_bucket")
        if not s3_bucket:
            raise ValueError("s3_bucket must be specified in the config")

        s3_key = f"staging/{table_name}_{uuid.uuid4()}.parquet"
        s3_path = f"s3://{s3_bucket}/{s3_key}"
        logger.info(f"Staging DataFrame to S3 at: {s3_path}")

        try:
            # Stage DataFrame as a Parquet file to S3
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
                # Create table schema if it doesn't exist
                create_table_sql = self._get_sql_schema(df, table_name)
                cursor.execute(create_table_sql)

                # Build and execute the COPY command
                iam_role_arn = self.config.get("iam_role_arn")
                if not iam_role_arn:
                    raise ValueError("iam_role_arn must be specified in the config")

                copy_sql = f"""
                    COPY "{table_name}"
                    FROM '{s3_path}'
                    IAM_ROLE '{iam_role_arn}'
                    FORMAT AS PARQUET;
                """
                logger.info(f"Executing COPY command for table {table_name}.")
                cursor.execute(copy_sql)

            # If COPY is successful, commit and then delete S3 object
            self.connection.commit()
            logger.info(
                f"Successfully loaded data into {table_name}. Deleting staged S3 file."
            )
            self.s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)

        except Exception as e:
            logger.error(f"Failed to load data into Redshift. Staged file remains in S3: {s3_path}", exc_info=True)
            if self.connection:
                self.connection.rollback()
            raise IOError("Failed to load data into Redshift from S3") from e
