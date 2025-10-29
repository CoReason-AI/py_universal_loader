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

from .base import BaseLoader


class DatabricksLoader(BaseLoader):
    """
    Loader for Databricks SQL.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection = None

    def connect(self):
        """
        Establish and open the database connection.
        """
        raise NotImplementedError("DatabricksLoader is not yet implemented.")

    def close(self):
        """
        Terminate the database connection.
        """
        raise NotImplementedError("DatabricksLoader is not yet implemented.")

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Execute the entire data ingestion process.
        """
        raise NotImplementedError("DatabricksLoader is not yet implemented.")
