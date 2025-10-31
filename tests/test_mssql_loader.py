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
from py_universal_loader.mssql_loader import MSSQLLoader


@pytest.fixture
def sample_df():
    """
    Fixture for a sample pandas DataFrame.
    """
    return pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})


def test_get_loader_mssql():
    """
    Test that get_loader returns a MSSQLLoader instance for db_type 'mssql'.
    """
    config = {"db_type": "mssql"}
    loader = get_loader(config)
    assert isinstance(loader, MSSQLLoader)


@patch("pyodbc.connect")
def test_mssql_loader_connect(mock_connect):
    """
    Test the connect method.
    """
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    config = {
        "db_type": "mssql",
        "server": "localhost",
        "database": "test_db",
        "username": "user",
        "password": "password",
    }
    loader = MSSQLLoader(config)
    loader.connect()
    expected_conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=test_db;UID=user;PWD=password"
    mock_connect.assert_called_once_with(expected_conn_str)
    assert loader.connection == mock_connection


@patch("pyodbc.connect")
def test_mssql_loader_close(mock_connect):
    """
    Test that the close method correctly closes the connection.
    """
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    config = {
        "db_type": "mssql",
        "server": "localhost",
        "database": "test_db",
        "username": "user",
        "password": "password",
    }
    loader = MSSQLLoader(config)
    loader.connect()
    assert loader.connection is not None
    loader.close()
    mock_connection.close.assert_called_once()
    assert loader.connection is None


@patch("pyodbc.connect")
@patch("pandas.DataFrame.to_csv")
def test_mssql_loader_load_dataframe_replace(mock_to_csv, mock_connect, sample_df):
    """
    Test the load_dataframe method with if_exists='replace'.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    staging_path = "/path/to/staging/file.csv"
    config = {
        "db_type": "mssql",
        "server": "localhost",
        "database": "test_db",
        "username": "user",
        "password": "password",
        "staging_file_path": staging_path,
        "if_exists": "replace",
    }
    loader = MSSQLLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    mock_to_csv.assert_called_once_with(
        staging_path, index=False, header=True, sep="|", quotechar='"'
    )

    # Check for DROP, CREATE, and BULK INSERT calls
    mock_cursor.execute.assert_any_call("DROP TABLE IF EXISTS [test_table]")
    mock_cursor.execute.assert_any_call(
        "CREATE TABLE [test_table] ([col1] BIGINT, [col2] NVARCHAR(MAX));"
    )
    execute_call = mock_cursor.execute.call_args[0][0]
    assert "BULK INSERT [test_table]" in execute_call
    assert f"FROM '{staging_path}'" in execute_call
    mock_connection.commit.assert_called_once()


@patch("pyodbc.connect")
@patch("pandas.DataFrame.to_csv")
def test_mssql_loader_load_dataframe_append(mock_to_csv, mock_connect, sample_df):
    """
    Test the load_dataframe method with if_exists='append'.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    staging_path = "/path/to/staging/file.csv"
    config = {
        "db_type": "mssql",
        "server": "localhost",
        "database": "test_db",
        "username": "user",
        "password": "password",
        "staging_file_path": staging_path,
        "if_exists": "append",
    }
    loader = MSSQLLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    # Check that CREATE IF NOT EXISTS is called, but not DROP
    for call in mock_cursor.execute.call_args_list:
        assert "DROP TABLE" not in call.args[0]
    create_if_not_exists_call = [
        call
        for call in mock_cursor.execute.call_args_list
        if "IF NOT EXISTS" in call[0][0]
    ]
    assert len(create_if_not_exists_call) == 1


def test_mssql_loader_invalid_if_exists(sample_df):
    """
    Test that a ValueError is raised for an invalid if_exists option.
    """
    config = {
        "db_type": "mssql",
        "server": "localhost",
        "database": "test_db",
        "username": "user",
        "password": "password",
        "staging_file_path": "/path/to/staging/file.csv",
        "if_exists": "invalid",
    }
    loader = MSSQLLoader(config)
    loader.connection = MagicMock()  # Mock connection

    with pytest.raises(ValueError, match="Unsupported if_exists option: invalid"):
        loader.load_dataframe(sample_df, "test_table")


@patch("pyodbc.connect")
def test_mssql_loader_load_dataframe_no_staging_path(mock_connect, sample_df):
    """
    Test that load_dataframe raises a ValueError if 'staging_file_path' is missing.
    """
    config = {
        "db_type": "mssql",
        "server": "localhost",
        "database": "test_db",
        "username": "user",
        "password": "password",
    }
    loader = MSSQLLoader(config)
    loader.connect()
    with pytest.raises(
        ValueError,
        match="'staging_file_path' must be provided in the configuration for MSSQLLoader.",
    ):
        loader.load_dataframe(sample_df, "test_table")


def test_mssql_loader_load_dataframe_no_connection(sample_df):
    """
    Test that load_dataframe raises a ConnectionError if connect has not been called.
    """
    config = {"db_type": "mssql"}
    loader = MSSQLLoader(config)
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")
