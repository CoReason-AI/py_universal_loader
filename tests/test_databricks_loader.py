# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd

from py_universal_loader.databricks_loader import DatabricksLoader

@pytest.fixture
def mock_pyodbc_connect():
    with patch('pyodbc.connect') as mock_connect:
        yield mock_connect

@pytest.fixture
def mock_boto3_client():
    with patch('boto3.client') as mock_client:
        yield mock_client

@pytest.fixture
def databricks_loader(mock_pyodbc_connect, mock_boto3_client):
    config = {
        'db_type': 'databricks',
        'server_hostname': 'test-hostname',
        'http_path': 'test-path',
        'access_token': 'test-token',
        's3_bucket': 'test-bucket',
        'iam_role_arn': 'test-role'
    }
    loader = DatabricksLoader(config)

    mock_connection = mock_pyodbc_connect.return_value
    mock_cursor = mock_connection.cursor.return_value.__enter__.return_value

    loader.connection = mock_connection
    loader.s3_client = mock_boto3_client.return_value

    loader.mock_cursor = mock_cursor
    return loader

def test_connect(mock_pyodbc_connect, mock_boto3_client):
    config = {
        'server_hostname': 'test-hostname',
        'http_path': 'test-path',
        'access_token': 'test-token',
        'driver_path': 'test-driver'
    }
    loader = DatabricksLoader(config)
    loader.connect()
    mock_pyodbc_connect.assert_called_once()
    mock_boto3_client.assert_called_once_with('s3')

def test_close(databricks_loader):
    databricks_loader.connect()
    databricks_loader.close()
    assert databricks_loader.connection is None

def test_load_dataframe_replace(databricks_loader):
    df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    table_name = 'test_table'

    databricks_loader.load_dataframe(df, table_name)

    databricks_loader.s3_client.upload_fileobj.assert_called_once()
    databricks_loader.mock_cursor.execute.assert_any_call('DROP TABLE IF EXISTS test_table')
    databricks_loader.s3_client.delete_object.assert_called_once()

def test_load_dataframe_append(databricks_loader):
    databricks_loader.config['if_exists'] = 'append'
    df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    table_name = 'test_table'

    databricks_loader.load_dataframe(df, table_name)

    databricks_loader.s3_client.upload_fileobj.assert_called_once()
    databricks_loader.mock_cursor.execute.assert_any_call('CREATE TABLE IF NOT EXISTS test_table (`a` BIGINT, `b` STRING);')
    databricks_loader.s3_client.delete_object.assert_called_once()

def test_load_dataframe_empty(databricks_loader):
    df = pd.DataFrame()
    table_name = 'test_table'

    databricks_loader.load_dataframe(df, table_name)

    databricks_loader.s3_client.upload_fileobj.assert_not_called()

def test_load_dataframe_no_connection(databricks_loader):
    databricks_loader.connection = None
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ConnectionError, match=r"Connection is not established. Call connect\(\) first."):
        databricks_loader.load_dataframe(df, 'test_table')

def test_load_dataframe_invalid_if_exists(databricks_loader):
    databricks_loader.config['if_exists'] = 'fail'
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ValueError, match="Unsupported if_exists option: fail"):
        databricks_loader.load_dataframe(df, 'test_table')

def test_load_dataframe_no_s3_bucket(databricks_loader):
    databricks_loader.config['s3_bucket'] = None
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ValueError, match="'s3_bucket' must be specified in the config"):
        databricks_loader.load_dataframe(df, 'test_table')

def test_load_dataframe_no_iam_role(databricks_loader):
    databricks_loader.config['iam_role_arn'] = None
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ValueError, match="'iam_role_arn' must be specified in the config"):
        databricks_loader.load_dataframe(df, 'test_table')
