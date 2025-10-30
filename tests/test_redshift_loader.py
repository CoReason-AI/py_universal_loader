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
from py_universal_loader.redshift_loader import RedshiftLoader


@pytest.fixture
def sample_df():
    """Fixture for a sample pandas DataFrame."""
    return pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})


@pytest.fixture
def redshift_config():
    """Fixture for a sample Redshift loader configuration."""
    return {
        "db_type": "redshift",
        "host": "test_host",
        "port": 5439,
        "user": "test_user",
        "password": "test_password",
        "dbname": "test_db",
        "s3_bucket": "test-bucket",
        "iam_role_arn": "arn:aws:iam::123456789012:role/test-role",
    }


def test_get_loader_redshift(redshift_config):
    """Test that get_loader returns a RedshiftLoader instance."""
    loader = get_loader(redshift_config)
    assert isinstance(loader, RedshiftLoader)


@patch("boto3.client")
@patch("psycopg2.connect")
def test_redshift_loader_connect(
    mock_psycopg2_connect, mock_boto3_client, redshift_config
):
    """Test the connect method establishes both Redshift and S3 connections."""
    mock_psycopg2_connect.return_value = MagicMock()
    mock_boto3_client.return_value = MagicMock()

    loader = RedshiftLoader(redshift_config)
    loader.connect()

    mock_psycopg2_connect.assert_called_once_with(
        dbname="test_db",
        user="test_user",
        password="test_password",
        host="test_host",
        port=5439,
    )
    mock_boto3_client.assert_called_once_with("s3", **{})
    assert loader.connection is not None
    assert loader.s3_client is not None
    loader.close()


@patch("boto3.client")
@patch("psycopg2.connect")
def test_redshift_loader_close(
    mock_psycopg2_connect, mock_boto3_client, redshift_config
):
    """Test that the close method correctly terminates the connection."""
    mock_conn = MagicMock()
    mock_psycopg2_connect.return_value = mock_conn

    loader = RedshiftLoader(redshift_config)
    loader.connect()
    connection = loader.connection  # Capture connection before it's set to None
    loader.close()

    connection.close.assert_called_once()
    assert loader.connection is None


@patch("boto3.client")
@patch("psycopg2.connect")
def test_redshift_loader_load_dataframe_success(
    mock_psycopg2_connect, mock_boto3_client, redshift_config, sample_df
):
    """Test a successful data load, verifying S3 upload, COPY, and S3 delete."""
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg2_connect.return_value = mock_conn

    loader = RedshiftLoader(redshift_config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    # Verify S3 upload was called
    mock_s3.upload_fileobj.assert_called_once()
    args, _ = mock_s3.upload_fileobj.call_args
    assert args[1] == "test-bucket"
    assert args[2].startswith("staging/test_table_")

    # Verify COPY command was executed with correct parameters
    assert mock_cursor.execute.call_count == 3  # DROP + CREATE TABLE + COPY
    copy_sql = mock_cursor.execute.call_args_list[2][0][0]
    assert 'COPY "test_table"' in copy_sql
    assert "s3://test-bucket/staging/test_table_" in copy_sql
    assert "IAM_ROLE 'arn:aws:iam::123456789012:role/test-role'" in copy_sql
    assert "FORMAT AS PARQUET" in copy_sql
    mock_conn.commit.assert_called_once()

    # Verify S3 object was deleted after successful load
    mock_s3.delete_object.assert_called_once()
    _, kwargs = mock_s3.delete_object.call_args
    assert kwargs["Bucket"] == "test-bucket"
    assert kwargs["Key"].startswith("staging/test_table_")

    loader.close()


@patch("boto3.client")
@patch("psycopg2.connect")
def test_redshift_loader_s3_upload_fails(
    mock_psycopg2_connect, mock_boto3_client, redshift_config, sample_df
):
    """Test that the load fails if S3 upload throws an error."""
    mock_s3 = MagicMock()
    mock_s3.upload_fileobj.side_effect = Exception("S3 Upload Error")
    mock_boto3_client.return_value = mock_s3

    loader = RedshiftLoader(redshift_config)
    loader.connect()

    with pytest.raises(IOError, match="Failed to upload staged file to S3"):
        loader.load_dataframe(sample_df, "test_table")

    mock_s3.delete_object.assert_not_called()
    loader.close()


@patch("boto3.client")
@patch("psycopg2.connect")
def test_redshift_loader_copy_fails(
    mock_psycopg2_connect, mock_boto3_client, redshift_config, sample_df
):
    """Test that the S3 object is NOT deleted if the COPY command fails."""
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = [
        None,  # First execute (CREATE TABLE) succeeds
        Exception("Redshift COPY Error"),  # Second execute (COPY) fails
    ]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg2_connect.return_value = mock_conn

    loader = RedshiftLoader(redshift_config)
    loader.connect()

    with pytest.raises(IOError, match="Failed to load data into Redshift from S3"):
        loader.load_dataframe(sample_df, "test_table")

    mock_conn.rollback.assert_called_once()
    mock_s3.delete_object.assert_not_called()  # Crucial check
    loader.close()


def test_redshift_loader_load_dataframe_no_connection(redshift_config, sample_df):
    """Test that load_dataframe raises ConnectionError if not connected."""
    loader = RedshiftLoader(redshift_config)
    with pytest.raises(ConnectionError, match="Connection is not established"):
        loader.load_dataframe(sample_df, "test_table")


@patch("boto3.client")
@patch("psycopg2.connect")
def test_redshift_loader_if_exists_replace(
    mock_psycopg2_connect, mock_boto3_client, redshift_config, sample_df
):
    """Test the 'replace' functionality for if_exists."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg2_connect.return_value = mock_conn

    config = {**redshift_config, "if_exists": "replace"}
    loader = RedshiftLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    # Check that DROP TABLE and CREATE TABLE were called
    assert mock_cursor.execute.call_count == 3  # DROP, CREATE, COPY
    drop_sql = mock_cursor.execute.call_args_list[0][0][0]
    create_sql = mock_cursor.execute.call_args_list[1][0][0]
    assert 'DROP TABLE IF EXISTS "test_table"' in drop_sql
    assert 'CREATE TABLE "test_table"' in create_sql
    loader.close()


@patch("boto3.client")
@patch("psycopg2.connect")
def test_redshift_loader_if_exists_append(
    mock_psycopg2_connect, mock_boto3_client, redshift_config, sample_df
):
    """Test the 'append' functionality for if_exists."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg2_connect.return_value = mock_conn

    config = {**redshift_config, "if_exists": "append"}
    loader = RedshiftLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    # Check that CREATE TABLE IF NOT EXISTS was called
    assert mock_cursor.execute.call_count == 2  # CREATE IF NOT EXISTS, COPY
    create_sql = mock_cursor.execute.call_args_list[0][0][0]
    assert "CREATE TABLE IF NOT EXISTS" in create_sql
    loader.close()


@patch("boto3.client")
@patch("psycopg2.connect")
def test_redshift_loader_if_exists_invalid(
    mock_psycopg2_connect, mock_boto3_client, redshift_config, sample_df
):
    """Test that an invalid if_exists option raises a ValueError."""
    config = {**redshift_config, "if_exists": "invalid_option"}
    loader = RedshiftLoader(config)
    loader.connect()
    with pytest.raises(ValueError, match="Unsupported if_exists option: invalid_option"):
        loader.load_dataframe(sample_df, "test_table")
    loader.close()
