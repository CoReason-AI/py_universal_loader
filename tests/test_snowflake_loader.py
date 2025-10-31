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

from py_universal_loader.snowflake_loader import SnowflakeLoader

@pytest.fixture
def mock_snowflake_connector():
    with patch('snowflake.connector.connect') as mock_connect:
        yield mock_connect

@pytest.fixture
def mock_boto3_client():
    with patch('boto3.client') as mock_client:
        yield mock_client

@pytest.fixture
def snowflake_loader(mock_snowflake_connector, mock_boto3_client):
    config = {
        'db_type': 'snowflake',
        'user': 'test-user',
        'password': 'test-password',
        'account': 'test-account',
        'warehouse': 'test-warehouse',
        'database': 'test-database',
        'schema': 'test-schema',
        's3_bucket': 'test-bucket',
        'iam_role_arn': 'test-role'
    }
    loader = SnowflakeLoader(config)

    # Set up mock connection and cursor correctly for context manager
    mock_connection = mock_snowflake_connector.return_value
    mock_cursor = mock_connection.cursor.return_value.__enter__.return_value

    loader.connection = mock_connection
    loader.s3_client = mock_boto3_client.return_value

    # Store mock_cursor for easy access in tests
    loader.mock_cursor = mock_cursor
    return loader

def test_connect(mock_snowflake_connector, mock_boto3_client):
    config = {
        'user': 'test-user',
        'password': 'test-password',
        'account': 'test-account',
        'warehouse': 'test-warehouse',
        'database': 'test-database',
        'schema': 'test-schema'
    }
    loader = SnowflakeLoader(config)
    loader.connect()
    mock_snowflake_connector.assert_called_once_with(
        user='test-user',
        password='test-password',
        account='test-account',
        warehouse='test-warehouse',
        database='test-database',
        schema='test-schema'
    )
    mock_boto3_client.assert_called_once_with('s3')

def test_close(snowflake_loader):
    snowflake_loader.connect()
    snowflake_loader.close()
    assert snowflake_loader.connection is None

def test_load_dataframe_replace(snowflake_loader):
    df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    table_name = 'test_table'

    snowflake_loader.load_dataframe(df, table_name)

    snowflake_loader.s3_client.upload_fileobj.assert_called_once()
    snowflake_loader.mock_cursor.execute.assert_any_call('DROP TABLE IF EXISTS "test_table"')
    snowflake_loader.s3_client.delete_object.assert_called_once()

def test_load_dataframe_append(snowflake_loader):
    snowflake_loader.config['if_exists'] = 'append'
    df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    table_name = 'test_table'

    snowflake_loader.load_dataframe(df, table_name)

    snowflake_loader.s3_client.upload_fileobj.assert_called_once()
    snowflake_loader.mock_cursor.execute.assert_any_call('CREATE TABLE IF NOT EXISTS "test_table" ("a" BIGINT, "b" VARCHAR);')
    snowflake_loader.s3_client.delete_object.assert_called_once()

def test_load_dataframe_empty(snowflake_loader):
    df = pd.DataFrame()
    table_name = 'test_table'

    snowflake_loader.load_dataframe(df, table_name)

    snowflake_loader.s3_client.upload_fileobj.assert_not_called()

def test_load_dataframe_no_connection(snowflake_loader):
    snowflake_loader.connection = None
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ConnectionError, match="Database connection is not established."):
        snowflake_loader.load_dataframe(df, 'test_table')

def test_load_dataframe_invalid_if_exists(snowflake_loader):
    snowflake_loader.config['if_exists'] = 'fail'
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ValueError, match="Unsupported if_exists option: fail"):
        snowflake_loader.load_dataframe(df, 'test_table')

def test_load_dataframe_no_s3_bucket(snowflake_loader):
    snowflake_loader.config['s3_bucket'] = None
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ValueError, match="s3_bucket must be specified in the config"):
        snowflake_loader.load_dataframe(df, 'test_table')

def test_load_dataframe_no_iam_role(snowflake_loader):
    snowflake_loader.config['iam_role_arn'] = None
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ValueError, match="iam_role_arn must be specified in the config"):
        snowflake_loader.load_dataframe(df, 'test_table')
