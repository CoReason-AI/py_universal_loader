# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from py_universal_loader.redshift_loader import RedshiftLoader


@pytest.fixture
def sample_df():
    """
    Fixture for a sample pandas DataFrame.
    """
    return pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})


@pytest.fixture
def redshift_config():
    """
    Fixture for a sample Redshift loader configuration.
    """
    return {
        "db_type": "redshift",
        "host": "test_host",
        "port": 1234,
        "user": "test_user",
        "password": "test_password",
        "dbname": "test_db",
        "s3_bucket": "test_bucket",
        "aws_access_key_id": "test_access_key",
        "aws_secret_access_key": "test_secret_key",
        "iam_role_arn": "test_iam_role",
    }


@patch("psycopg2.connect")
@patch("boto3.client")
def test_redshift_loader_connect(mock_boto3_client, mock_psycopg2_connect, redshift_config):
    """
    Test the connect method for the RedshiftLoader.
    """
    mock_conn = MagicMock()
    mock_psycopg2_connect.return_value = mock_conn
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3

    loader = RedshiftLoader(redshift_config)
    loader.connect()

    mock_psycopg2_connect.assert_called_once_with(
        host="test_host",
        port=1234,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    mock_boto3_client.assert_called_once_with(
        "s3",
        aws_access_key_id="test_access_key",
        aws_secret_access_key="test_secret_key",
    )
    assert loader.connection == mock_conn
    assert loader.s3_client == mock_s3
    loader.close()


@patch("psycopg2.connect")
@patch("boto3.client")
def test_redshift_loader_close(mock_boto3_client, mock_psycopg2_connect, redshift_config):
    """
    Test that the close method correctly closes the connection.
    """
    mock_conn = MagicMock()
    mock_psycopg2_connect.return_value = mock_conn

    loader = RedshiftLoader(redshift_config)
    loader.connect()
    assert loader.connection is not None
    loader.close()
    mock_conn.close.assert_called_once()
    assert loader.connection is None


@patch("psycopg2.connect")
@patch("boto3.client")
def test_redshift_loader_load_dataframe(
    mock_boto3_client, mock_psycopg2_connect, redshift_config, sample_df
):
    """
    Test the load_dataframe method with a sample DataFrame.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg2_connect.return_value = mock_conn
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3

    loader = RedshiftLoader(redshift_config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    # Verify S3 upload
    mock_s3.upload_fileobj.assert_called_once()
    # Verify COPY command
    mock_cursor.execute.assert_called_once()
    assert "COPY test_table" in mock_cursor.execute.call_args[0][0]
    assert "s3://test_bucket/tmp/test_table.parquet" in mock_cursor.execute.call_args[0][0]
    assert "IAM_ROLE 'test_iam_role'" in mock_cursor.execute.call_args[0][0]
    # Verify S3 delete
    mock_s3.delete_object.assert_called_once_with(
        Bucket="test_bucket", Key="tmp/test_table.parquet"
    )
    loader.close()


def test_redshift_loader_load_dataframe_no_connection(redshift_config, sample_df):
    """
    Test that load_dataframe raises a ConnectionError if connect has not been called.
    """
    loader = RedshiftLoader(redshift_config)
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")
