# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_parquet_forget

from typing import Any, Dict
import pandas as pd
import snowflake.connector
import boto3
from .base import BaseLoader
from loguru import logger
import io


class SnowflakeLoader(BaseLoader):
    """
    Loader for Snowflake databases.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection = None
        self.s3_client = None

    def connect(self):
        """
        Establish and open the database connection.
        """
        try:
            self.connection = snowflake.connector.connect(
                user=self.config["user"],
                password=self.config["password"],
                account=self.config["account"],
                warehouse=self.config["warehouse"],
                database=self.config["database"],
                schema=self.config["schema"],
            )
            self.s3_client = boto3.client("s3")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake or S3: {e}")
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
        if not self.connection or not self.s3_client:
            raise ConnectionError("Database connection is not established.")

        bucket_name = self.config["s3_bucket"]
        s3_key = f"tmp/{table_name}.parquet"
        s3_path = f"s3://{bucket_name}/{s3_key}"

        try:
            # Stage DataFrame as a Parquet file to S3
            with io.BytesIO() as buffer:
                df.to_parquet(buffer, index=False)
                buffer.seek(0)
                self.s3_client.upload_fileobj(buffer, bucket_name, s3_key)

            # Execute Snowflake COPY command
            with self.connection.cursor() as cursor:
                iam_role_arn = self.config.get("iam_role_arn", "")
                copy_sql = f"""
                    COPY INTO {table_name}
                    FROM '@{s3_path}'
                    credentials=(aws_role='{iam_role_arn}')
                    file_format = (type = parquet);
                """
                cursor.execute(copy_sql)
            self.connection.commit()

        except Exception as e:
            logger.error(f"Failed to load data to Snowflake: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            # Delete temporary file from S3
            try:
                self.s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
            except Exception as e:
                logger.error(f"Failed to delete temporary file from S3: {e}")
