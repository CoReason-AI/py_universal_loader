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
from py_universal_loader.mysql_loader import MySQLLoader


@pytest.fixture
def sample_df():
    """
    Fixture for a sample pandas DataFrame.
    """
    return pd.DataFrame({"col_int": [1, 2], "col_str": ["A", "B"]})


@pytest.fixture
def mysql_config():
    """
    Fixture for a sample MySQL config.
    Using placeholders for credentials.
    """
    return {
        "db_type": "mysql",
        "user": "test_user",
        "password": "test_password",
        "host": "localhost",
        "database": "test_db",
    }


def test_get_loader_mysql(mysql_config):
    """
    Test that get_loader returns a MySQLLoader instance for db_type 'mysql'.
    """
    loader = get_loader(mysql_config)
    assert isinstance(loader, MySQLLoader)


@patch("mysql.connector.connect")
def test_mysql_loader_connect(mock_connect, mysql_config):
    """
    Test the connect method establishes a connection.
    """
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    loader = MySQLLoader(mysql_config)
    loader.connect()

    mock_connect.assert_called_once_with(
        user="test_user",
        password="test_password",
        host="localhost",
        database="test_db",
        port=3306,
        allow_local_infile=True,
    )
    assert loader.connection == mock_connection


def test_mysql_loader_close(mysql_config):
    """
    Test that the close method correctly closes the connection.
    """
    loader = MySQLLoader(mysql_config)
    loader.connection = MagicMock()
    connection = loader.connection

    loader.close()

    assert loader.connection is None
    connection.close.assert_called_once()


def test_mysql_loader_load_dataframe_no_connection(mysql_config, sample_df):
    """
    Test that load_dataframe raises a ConnectionError if connect has not been called.
    """
    loader = MySQLLoader(mysql_config)
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")


@patch("mysql.connector.connect")
def test_mysql_loader_load_dataframe_replace(mock_connect, mysql_config, sample_df):
    """
    Test the load_dataframe method with if_exists='replace'.
    """
    # Setup mocks
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Configure and run loader
    config = {**mysql_config, "if_exists": "replace"}
    loader = MySQLLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_replace")

    # Assertions
    create_sql = (
        "CREATE TABLE IF NOT EXISTS `test_replace` (`col_int` BIGINT, `col_str` TEXT);"
    )
    truncate_sql = "TRUNCATE TABLE `test_replace`;"

    mock_cursor.execute.assert_any_call(create_sql)
    mock_cursor.execute.assert_any_call(truncate_sql)

    # Check that LOAD DATA was called without checking the temp file path
    load_data_call_found = any(
        "LOAD DATA LOCAL INFILE" in call.args[0]
        for call in mock_cursor.execute.call_args_list
    )
    assert load_data_call_found, "LOAD DATA LOCAL INFILE was not executed."

    mock_connection.commit.assert_called_once()
    loader.close()


@patch("mysql.connector.connect")
def test_mysql_loader_load_dataframe_append(mock_connect, mysql_config, sample_df):
    """
    Test the load_dataframe method with if_exists='append'.
    """
    # Setup mocks
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Configure and run loader
    config = {**mysql_config, "if_exists": "append"}
    loader = MySQLLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_append")

    # Assertions
    create_sql = (
        "CREATE TABLE IF NOT EXISTS `test_append` (`col_int` BIGINT, `col_str` TEXT);"
    )
    truncate_sql = "TRUNCATE TABLE `test_append`;"

    mock_cursor.execute.assert_any_call(create_sql)
    # Ensure TRUNCATE was NOT called
    for call_args in mock_cursor.execute.call_args_list:
        assert truncate_sql not in str(call_args)

    # Check that LOAD DATA was called
    load_data_call_found = any(
        "LOAD DATA LOCAL INFILE" in call.args[0]
        for call in mock_cursor.execute.call_args_list
    )
    assert load_data_call_found, "LOAD DATA LOCAL INFILE was not executed."

    mock_connection.commit.assert_called_once()
    loader.close()


@patch("mysql.connector.connect")
def test_mysql_loader_load_dataframe_empty(mock_connect, mysql_config):
    """
    Test that load_dataframe skips execution for an empty DataFrame.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    loader = MySQLLoader(mysql_config)
    loader.connect()

    empty_df = pd.DataFrame({"col1": []})
    loader.load_dataframe(empty_df, "test_empty")

    # Verify that no SQL commands were executed
    mock_cursor.execute.assert_not_called()
    loader.close()


@patch("mysql.connector.connect")
def test_mysql_loader_load_dataframe_rollback_on_error(
    mock_connect, mysql_config, sample_df
):
    """
    Test that the connection is rolled back if an error occurs during load.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    # Simulate a failure on the third execute call (LOAD DATA)
    mock_cursor.execute.side_effect = [
        None,  # CREATE TABLE
        None,  # TRUNCATE
        Exception("LOAD DATA failed"),
    ]

    loader = MySQLLoader({**mysql_config, "if_exists": "replace"})
    loader.connect()

    with pytest.raises(Exception, match="LOAD DATA failed"):
        loader.load_dataframe(sample_df, "test_error")

    mock_connection.commit.assert_not_called()
    mock_connection.rollback.assert_called_once()
    loader.close()
