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
from unittest.mock import MagicMock, patch, mock_open
import pandas as pd

from py_universal_loader.mssql_loader import MSSQLLoader

@pytest.fixture
def mock_pyodbc_connect():
    with patch('pyodbc.connect') as mock_connect:
        yield mock_connect

@pytest.fixture
def mssql_loader(mock_pyodbc_connect):
    config = {
        'db_type': 'mssql',
        'server': 'test-server',
        'database': 'test-db',
        'username': 'test-user',
        'password': 'test-password',
        'staging_file_path': '/path/to/staging/file.csv'
    }
    loader = MSSQLLoader(config)

    mock_connection = mock_pyodbc_connect.return_value
    mock_cursor = mock_connection.cursor.return_value.__enter__.return_value

    loader.connection = mock_connection
    loader.mock_cursor = mock_cursor
    return loader

def test_connect(mock_pyodbc_connect):
    config = {
        'server': 'test-server',
        'database': 'test-db',
        'username': 'test-user',
        'password': 'test-password',
    }
    loader = MSSQLLoader(config)
    loader.connect()
    mock_pyodbc_connect.assert_called_once()

def test_close(mssql_loader):
    mssql_loader.connect()
    mssql_loader.close()
    assert mssql_loader.connection is None

@patch('pandas.DataFrame.to_csv')
def test_load_dataframe_replace(mock_to_csv, mssql_loader):
    df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    table_name = 'test_table'

    mssql_loader.load_dataframe(df, table_name)

    mock_to_csv.assert_called_once_with('/path/to/staging/file.csv', index=False, header=True, sep='|', quotechar='"')
    mssql_loader.mock_cursor.execute.assert_any_call('DROP TABLE IF EXISTS [test_table]')

@patch('pandas.DataFrame.to_csv')
def test_load_dataframe_append(mock_to_csv, mssql_loader):
    mssql_loader.config['if_exists'] = 'append'
    df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    table_name = 'test_table'

    mssql_loader.load_dataframe(df, table_name)

    mock_to_csv.assert_called_once_with('/path/to/staging/file.csv', index=False, header=True, sep='|', quotechar='"')
    mssql_loader.mock_cursor.execute.assert_any_call("\n            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='test_table' and xtype='U')\n            CREATE TABLE [test_table] ([a] BIGINT, [b] NVARCHAR(MAX));\n            ")

@patch('pandas.DataFrame.to_csv')
def test_load_dataframe_empty(mock_to_csv, mssql_loader):
    df = pd.DataFrame()
    table_name = 'test_table'

    mssql_loader.load_dataframe(df, table_name)

    mock_to_csv.assert_not_called()

def test_load_dataframe_no_connection(mssql_loader):
    mssql_loader.connection = None
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ConnectionError, match="Database connection is not established."):
        mssql_loader.load_dataframe(df, 'test_table')

def test_load_dataframe_invalid_if_exists(mssql_loader):
    mssql_loader.config['if_exists'] = 'fail'
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ValueError, match="Unsupported if_exists option: fail"):
        mssql_loader.load_dataframe(df, 'test_table')

def test_load_dataframe_no_staging_path(mssql_loader):
    mssql_loader.config['staging_file_path'] = None
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ValueError, match="'staging_file_path' must be provided in the configuration for MSSQLLoader."):
        mssql_loader.load_dataframe(df, 'test_table')
