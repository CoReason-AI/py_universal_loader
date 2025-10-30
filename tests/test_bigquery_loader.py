# Copyright (c) 2025 CoReason, Inc
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
from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions

from py_universal_loader.main import get_loader
from py_universal_loader.bigquery_loader import BigQueryLoader


@pytest.fixture
def sample_df():
    """
    Fixture for a sample pandas DataFrame.
    """
    return pd.DataFrame({"col_int": [1, 2], "col_str": ["A", "B"]})


@pytest.fixture
def bq_config():
    """
    Fixture for a sample BigQuery config.
    """
    return {
        "db_type": "bigquery",
        "project_id": "test-project",
        "dataset_id": "test_dataset",
    }


def test_get_loader_bigquery(bq_config):
    """
    Test that get_loader returns a BigQueryLoader instance.
    """
    loader = get_loader(bq_config)
    assert isinstance(loader, BigQueryLoader)


@patch("py_universal_loader.bigquery_loader.bigquery.Client")
def test_bigquery_loader_connect(mock_client, bq_config):
    """
    Test the connect method establishes a client connection.
    """
    loader = BigQueryLoader(bq_config)
    loader.connect()
    mock_client.assert_called_once_with(project="test-project")
    assert isinstance(loader.client, MagicMock)
    loader.close()


@patch("py_universal_loader.bigquery_loader.bigquery.Client")
def test_bigquery_loader_close(mock_client, bq_config):
    """
    Test that the close method correctly terminates the client.
    """
    loader = BigQueryLoader(bq_config)
    loader.connect()
    client = loader.client
    loader.close()
    assert loader.client is None
    client.close.assert_called_once()


@patch("py_universal_loader.bigquery_loader.bigquery.Client")
def test_bigquery_loader_load_dataframe(mock_client, bq_config, sample_df):
    """
    Test the load_dataframe method with default if_exists='replace'.
    """
    table_name = "test_replace"
    loader = BigQueryLoader(bq_config)
    loader.connect()

    mock_job = MagicMock()
    mock_job.output_rows = len(sample_df)
    loader.client.load_table_from_dataframe.return_value = mock_job

    loader.load_dataframe(sample_df, table_name)

    loader.client.load_table_from_dataframe.assert_called_once()
    args, kwargs = loader.client.load_table_from_dataframe.call_args
    pd.testing.assert_frame_equal(args[0], sample_df)
    assert args[1] == "test_dataset.test_replace"
    job_config = kwargs["job_config"]
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    mock_job.result.assert_called_once()
    loader.close()


@patch("py_universal_loader.bigquery_loader.bigquery.Client")
def test_bigquery_loader_load_dataframe_append(mock_client, bq_config, sample_df):
    """
    Test the load_dataframe method with if_exists='append'.
    """
    table_name = "test_append"
    config = {**bq_config, "if_exists": "append"}
    loader = BigQueryLoader(config)
    loader.connect()

    loader.load_dataframe(sample_df, table_name)

    _, kwargs = loader.client.load_table_from_dataframe.call_args
    job_config = kwargs["job_config"]
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    loader.close()


@patch("py_universal_loader.bigquery_loader.bigquery.Client")
def test_bigquery_loader_load_dataframe_empty(mock_client, bq_config):
    """
    Test that load_dataframe skips execution for an empty DataFrame.
    """
    table_name = "test_empty"
    loader = BigQueryLoader(bq_config)
    loader.connect()
    empty_df = pd.DataFrame({"col1": []})
    loader.load_dataframe(empty_df, table_name)

    loader.client.load_table_from_dataframe.assert_not_called()
    loader.close()


def test_bigquery_loader_load_dataframe_no_connection(bq_config, sample_df):
    """
    Test that load_dataframe raises a ConnectionError if not connected.
    """
    loader = BigQueryLoader(bq_config)
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")


@patch("py_universal_loader.bigquery_loader.bigquery.Client")
def test_bigquery_loader_api_error(mock_client, bq_config, sample_df):
    """
    Test that exceptions from the BigQuery client are propagated.
    """
    loader = BigQueryLoader(bq_config)
    loader.connect()

    loader.client.load_table_from_dataframe.side_effect = google_exceptions.NotFound(
        "Dataset not found"
    )

    with pytest.raises(google_exceptions.NotFound):
        loader.load_dataframe(sample_df, "test_table")

    loader.close()
