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
from google.cloud import bigquery
from loguru import logger

from .base import BaseLoader


class BigQueryLoader(BaseLoader):
    """
    Loader for Google BigQuery.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.client = None

    def connect(self):
        """
        Establish and open the database connection.
        """
        project_id = self.config.get("project_id")
        logger.info(f"Connecting to BigQuery project: {project_id}")
        self.client = bigquery.Client(project=project_id)

    def close(self):
        """
        Terminate the database connection.
        """
        if self.client:
            logger.info("Closing BigQuery client.")
            self.client.close()
            self.client = None

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Load a DataFrame into a BigQuery table using load_table_from_dataframe().
        """
        if not self.client:
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

        table_id = f"{self.config.get('dataset_id')}.{table_name}"

        job_config = bigquery.LoadJobConfig()
        if if_exists == "replace":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        job_config.autodetect = True

        try:
            job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()
            logger.info(f"Loaded {job.output_rows} rows into {table_id}.")
        except Exception as e:
            logger.error(f"Failed to load dataframe to BigQuery: {e}")
            raise
