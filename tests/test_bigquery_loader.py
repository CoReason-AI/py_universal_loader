# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from py_universal_loader.main import get_loader
from py_universal_loader.bigquery_loader import BigQueryLoader


@pytest.fixture
def sample_df():
    """
    Fixture for a sample pandas DataFrame.
    """
    return pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})


def test_get_loader_bigquery():
    """
    Test that get_loader returns a BigQueryLoader instance for db_type 'bigquery'.
    """
    config = {"db_type": "bigquery"}
    loader = get_loader(config)
    assert isinstance(loader, BigQueryLoader)


@patch("py_universal_loader.bigquery_loader.bigquery.Client")
def test_bigquery_loader_connect(mock_client):
    """
    Test the connect method.
    """
    config = {"db_type": "bigquery"}
    loader = BigQueryLoader(config)
    loader.connect()
    mock_client.assert_called_once()
    assert loader.client is not None


@patch("py_universal_loader.bigquery_loader.bigquery.Client")
def test_bigquery_loader_close(mock_client):
    """
    Test that the close method correctly closes the connection.
    """
    config = {"db_type": "bigquery"}
    loader = BigQueryLoader(config)
    loader.connect()
    client = loader.client
    assert client is not None
    loader.close()
    client.close.assert_called_once()
    assert loader.client is None


@patch("py_universal_loader.bigquery_loader.bigquery.Client")
def test_bigquery_loader_load_dataframe(mock_client, sample_df):
    """
    Test the load_dataframe method with a sample DataFrame.
    """
    config = {"db_type": "bigquery"}
    loader = BigQueryLoader(config)
    loader.connect()

    mock_job = MagicMock()
    loader.client.load_table_from_dataframe.return_value = mock_job

    loader.load_dataframe(sample_df, "test_table")

    loader.client.load_table_from_dataframe.assert_called_once_with(
        sample_df, "test_table", job_config={}
    )
    mock_job.result.assert_called_once()


def test_bigquery_loader_load_dataframe_no_connection(sample_df):
    """
    Test that load_dataframe raises a ConnectionError if connect has not been called.
    """
    config = {"db_type": "bigquery"}
    loader = BigQueryLoader(config)
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")


@patch("py_universal_loader.bigquery_loader.bigquery.Client")
def test_bigquery_loader_load_dataframe_after_close(mock_client, sample_df):
    """
    Test that load_dataframe raises an error if the connection has been closed.
    """
    config = {"db_type": "bigquery"}
    loader = BigQueryLoader(config)
    loader.connect()
    loader.close()
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")
