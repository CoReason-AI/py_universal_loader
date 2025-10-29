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
        self.config = config
        self.client = None

    def connect(self):
        """
        Establish and open the database connection.
        """
        logger.info("Connecting to BigQuery.")
        self.client = bigquery.Client()

    def close(self):
        """
        Terminate the database connection.
        """
        if self.client:
            logger.info("Closing BigQuery connection.")
            self.client.close()
            self.client = None

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Execute the entire data ingestion process.
        """
        if not self.client:
            raise ConnectionError("Database connection is not established.")

        logger.info(f"Loading dataframe into table: {table_name}")
        job_config = self.config.get("job_config", {})
        job = self.client.load_table_from_dataframe(
            df, table_name, job_config=job_config
        )
        job.result()
        logger.info("Dataframe loaded successfully.")
