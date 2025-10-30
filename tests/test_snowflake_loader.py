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

from py_universal_loader.main import get_loader
from py_universal_loader.snowflake_loader import SnowflakeLoader


@pytest.fixture
def sample_df():
    """Fixture for a sample pandas DataFrame."""
    return pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})


@pytest.fixture
def snowflake_config():
    """Fixture for a sample Snowflake loader configuration."""
    return {
        "db_type": "snowflake",
        "user": "test_user",
        "password": "test_password",
        "account": "test_account",
        "warehouse": "test_warehouse",
        "database": "test_database",
        "schema": "test_schema",
        "s3_bucket": "test_bucket",
        "iam_role_arn": "test_iam_role",
    }


def test_get_loader_snowflake(snowflake_config):
    """Test that get_loader returns a SnowflakeLoader instance."""
    loader = get_loader(snowflake_config)
    assert isinstance(loader, SnowflakeLoader)


@patch("boto3.client")
@patch("snowflake.connector.connect")
def test_snowflake_loader_connect(mock_snowflake_connect, mock_boto3_client, snowflake_config):
    """Test the connect method establishes both Snowflake and S3 connections."""
    mock_snowflake_connect.return_value = MagicMock()
    mock_boto3_client.return_value = MagicMock()

    loader = SnowflakeLoader(snowflake_config)
    loader.connect()

    mock_snowflake_connect.assert_called_once_with(
        user="test_user",
        password="test_password",
        account="test_account",
        warehouse="test_warehouse",
        database="test_database",
        schema="test_schema",
    )
    mock_boto3_client.assert_called_once_with("s3")
    assert loader.connection is not None
    assert loader.s3_client is not None
    loader.close()


@patch("boto3.client")
@patch("snowflake.connector.connect")
def test_snowflake_loader_close(mock_snowflake_connect, mock_boto3_client, snowflake_config):
    """Test that the close method correctly terminates the connection."""
    mock_conn = MagicMock()
    mock_snowflake_connect.return_value = mock_conn

    loader = SnowflakeLoader(snowflake_config)
    loader.connect()
    connection = loader.connection
    loader.close()

    connection.close.assert_called_once()
    assert loader.connection is None


@patch("boto3.client")
@patch("snowflake.connector.connect")
def test_snowflake_loader_load_dataframe(
    mock_snowflake_connect, mock_boto3_client, snowflake_config, sample_df
):
    """Test a successful data load, verifying S3 upload, COPY, and S3 delete."""
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_snowflake_connect.return_value = mock_conn

    loader = SnowflakeLoader(snowflake_config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    mock_s3.upload_fileobj.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_conn.commit.assert_called_once()
    mock_s3.delete_object.assert_called_once()


def test_snowflake_loader_load_dataframe_no_connection(snowflake_config, sample_df):
    """Test that load_dataframe raises ConnectionError if not connected."""
    loader = SnowflakeLoader(snowflake_config)
    with pytest.raises(ConnectionError, match="Database connection is not established."):
        loader.load_dataframe(sample_df, "test_table")
