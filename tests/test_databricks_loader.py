# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

from unittest.mock import patch, MagicMock
import pytest
import pandas as pd
import pyodbc

from py_universal_loader.databricks_loader import DatabricksLoader


@pytest.fixture
def config():
    """Provides a standard configuration for the DatabricksLoader."""
    return {
        "db_type": "databricks",
        "server_hostname": "test_hostname",
        "http_path": "test_http_path",
        "access_token": "test_access_token",
        "s3_bucket": "test_bucket",
        "iam_role_arn": "test_iam_role",
    }


@pytest.fixture
def loader(config):
    """Provides an instance of DatabricksLoader."""
    return DatabricksLoader(config)


@patch("py_universal_loader.databricks_loader.pyodbc.connect")
def test_connect_success(mock_connect, loader):
    """
    Tests that the connect method constructs the correct connection string and calls pyodbc.connect.
    """
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    loader.connect()

    expected_conn_str = (
        "Driver=/Library/simba/spark/lib/libsparkodbc_sb64-universal.dylib;"
        "Host=test_hostname;"
        "Port=443;"
        "HTTPPath=test_http_path;"
        "SSL=1;"
        "ThriftTransport=2;"
        "AuthMech=3;"
        "UID=token;"
        "PWD=test_access_token"
    )
    mock_connect.assert_called_once_with(expected_conn_str, autocommit=True)
    assert loader.connection == mock_conn


@patch("py_universal_loader.databricks_loader.pyodbc.connect")
def test_connect_failure(mock_connect, loader):
    """
    Tests that a ConnectionError is raised if pyodbc.connect fails.
    """
    mock_connect.side_effect = pyodbc.Error("Connection Failed")

    with pytest.raises(ConnectionError, match="Failed to connect to Databricks"):
        loader.connect()


def test_close(loader):
    """
    Tests that the close method calls the connection's close method.
    """
    mock_conn = MagicMock()
    loader.connection = mock_conn

    loader.close()

    mock_conn.close.assert_called_once()
    assert loader.connection is None


@patch("py_universal_loader.databricks_loader.uuid.uuid4", return_value="test-uuid")
@patch("py_universal_loader.databricks_loader.boto3.client")
def test_load_dataframe_success(mock_boto3_client, mock_uuid, loader):
    """
    Tests the successful execution of the load_dataframe method.
    """
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    loader.s3_client = mock_s3

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    loader.connection = mock_conn

    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    table_name = "test_table"

    loader.load_dataframe(df, table_name)

    mock_s3.upload_fileobj.assert_called_once()
    # Verify DROP TABLE and CREATE TABLE are called for default "replace"
    mock_cursor.execute.assert_any_call("DROP TABLE IF EXISTS test_table")
    mock_cursor.execute.assert_any_call(
        "CREATE TABLE test_table (`col1` BIGINT, `col2` STRING);"
    )
    expected_copy_sql = """
                    COPY INTO test_table
                    FROM 's3://test_bucket/staging/test_table_test-uuid.parquet'
                    WITH (CREDENTIAL (AWS_IAM_ROLE = 'test_iam_role'))
                    FILEFORMAT = PARQUET
                """
    mock_cursor.execute.assert_any_call(expected_copy_sql)
    mock_s3.delete_object.assert_called_once_with(
        Bucket="test_bucket", Key="staging/test_table_test-uuid.parquet"
    )


@patch("py_universal_loader.databricks_loader.boto3.client")
def test_load_dataframe_s3_upload_fails(mock_boto3_client, loader):
    """
    Tests that an IOError is raised if the S3 upload fails.
    """
    mock_s3 = MagicMock()
    mock_s3.upload_fileobj.side_effect = Exception("S3 Upload Error")
    mock_boto3_client.return_value = mock_s3
    loader.s3_client = mock_s3
    loader.connection = MagicMock()

    df = pd.DataFrame({"col1": [1, 2]})
    with pytest.raises(IOError, match="Failed to upload staged file to S3"):
        loader.load_dataframe(df, "test_table")


@patch("py_universal_loader.databricks_loader.uuid.uuid4", return_value="test-uuid")
@patch("py_universal_loader.databricks_loader.boto3.client")
def test_load_dataframe_copy_fails(mock_boto3_client, mock_uuid, loader):
    """
    Tests that an IOError is raised and the S3 file is not deleted if the COPY command fails.
    """
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    loader.s3_client = mock_s3

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = [
        None,  # DROP TABLE
        None,  # CREATE TABLE
        Exception("COPY failed"),
    ]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    loader.connection = mock_conn

    df = pd.DataFrame({"col1": [1, 2]})
    with pytest.raises(IOError, match="Failed to load data into Databricks from S3"):
        loader.load_dataframe(df, "test_table")

    mock_s3.delete_object.assert_not_called()


@patch("py_universal_loader.databricks_loader.uuid.uuid4", return_value="test-uuid")
@patch("py_universal_loader.databricks_loader.boto3.client")
def test_load_dataframe_if_exists_append(mock_boto3_client, mock_uuid, config):
    """
    Tests that DROP TABLE is not called when if_exists is 'append'.
    """
    config["if_exists"] = "append"
    loader = DatabricksLoader(config)
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    loader.s3_client = mock_s3

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    loader.connection = mock_conn

    df = pd.DataFrame({"col1": [1, 2]})
    table_name = "test_table"

    loader.load_dataframe(df, table_name)

    # Assert that DROP TABLE was NOT called
    for call in mock_cursor.execute.call_args_list:
        assert "DROP TABLE" not in call.args[0]

    # Assert that CREATE TABLE IF NOT EXISTS was called
    mock_cursor.execute.assert_any_call(
        "CREATE TABLE IF NOT EXISTS test_table (`col1` BIGINT);"
    )


def test_load_dataframe_if_exists_invalid(config):
    """
    Tests that a ValueError is raised for an invalid if_exists option.
    """
    config["if_exists"] = "invalid_option"
    loader = DatabricksLoader(config)
    loader.connection = MagicMock()
    loader.s3_client = MagicMock()

    df = pd.DataFrame({"col1": [1, 2]})
    with pytest.raises(ValueError, match="Unsupported if_exists option: invalid_option"):
        loader.load_dataframe(df, "test_table")
