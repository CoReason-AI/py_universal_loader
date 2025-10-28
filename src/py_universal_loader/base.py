# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

from abc import ABC, abstractmethod
import pandas as pd


class BaseLoader(ABC):
    """
    Abstract base class for all database loaders.
    """

    @abstractmethod
    def connect(self):
        """
        Establish and open the database connection.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self):
        """
        Terminate the database connection.
        """
        raise NotImplementedError

    @abstractmethod
    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Execute the entire data ingestion process.
        """
        raise NotImplementedError
