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
import snowflake.connector
import boto3
from .base import BaseLoader
from loguru import logger
import io
import uuid
import numpy as np


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

    def _get_create_table_sql(
        self, df: pd.DataFrame, table_name: str, if_not_exists: bool = False
    ) -> str:
        """
        Generate a CREATE TABLE statement from a DataFrame for Snowflake.
        """
        type_mapping = {
            np.dtype("int64"): "BIGINT",
            np.dtype("int32"): "INTEGER",
            np.dtype("float64"): "FLOAT",
            np.dtype("float32"): "FLOAT",
            np.dtype("bool"): "BOOLEAN",
            np.dtype("datetime64[ns]"): "TIMESTAMP_NTZ",
            np.dtype("object"): "VARCHAR",
        }

        cols = []
        for col_name, dtype in df.dtypes.items():
            # Snowflake identifiers are case-insensitive by default, but quoting them makes them case-sensitive.
            # It's best practice to quote identifiers to avoid issues.
            sql_type = type_mapping.get(dtype, "VARCHAR")
            cols.append(f'"{col_name}" {sql_type}')

        create_clause = (
            "CREATE TABLE IF NOT EXISTS" if if_not_exists else "CREATE TABLE"
        )
        return f'{create_clause} "{table_name}" ({", ".join(cols)});'

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Execute the entire data ingestion process.
        """
        if not self.connection or not self.s3_client:
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

        bucket_name = self.config.get("s3_bucket")
        if not bucket_name:
            raise ValueError("s3_bucket must be specified in the config")

        s3_key = f"staging/{table_name}_{uuid.uuid4()}.parquet"
        s3_path = f"s3://{bucket_name}/{s3_key}"

        try:
            # Stage DataFrame as a Parquet file to S3
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            self.s3_client.upload_fileobj(buffer, bucket_name, s3_key)
            logger.info(f"Successfully staged dataframe to {s3_path}")

            # Execute Snowflake COPY command
            with self.connection.cursor() as cursor:
                if if_exists == "replace":
                    logger.info(f"Dropping table {table_name} if it exists.")
                    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                    create_table_sql = self._get_create_table_sql(df, table_name)
                    cursor.execute(create_table_sql)
                elif if_exists == "append":
                    create_table_sql = self._get_create_table_sql(
                        df, table_name, if_not_exists=True
                    )
                    cursor.execute(create_table_sql)

                iam_role_arn = self.config.get("iam_role_arn")
                if not iam_role_arn:
                    raise ValueError("iam_role_arn must be specified in the config")

                copy_sql = f"""
                    COPY INTO "{table_name}"
                    FROM '{s3_path}'
                    CREDENTIALS=(AWS_ROLE='{iam_role_arn}')
                    FILE_FORMAT = (TYPE = PARQUET);
                """
                cursor.execute(copy_sql)
            self.connection.commit()
            logger.info(f"Successfully loaded data into {table_name}")

        except Exception as e:
            logger.error(f"Failed to load data to Snowflake: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            # Delete temporary file from S3
            try:
                logger.info(f"Deleting temporary S3 file: {s3_path}")
                self.s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
            except Exception as e:
                logger.warning(
                    f"Failed to delete temporary file from S3: {s3_path}. Error: {e}"
                )
