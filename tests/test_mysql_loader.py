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
import pandas as pd
import pytest

from py_universal_loader.main import get_loader
from py_universal_loader.mysql_loader import MySQLLoader


@pytest.fixture
def sample_df():
    """
    Fixture for a sample pandas DataFrame.
    """
    return pd.DataFrame(
        {
            "col_int": [1, 2],
            "col_float": [1.1, 2.2],
            "col_bool": [True, False],
            "col_str": ["A", "B"],
            "col_date": pd.to_datetime(["2025-01-01", "2025-01-02"]),
        }
    )


@pytest.fixture
def mysql_config():
    """
    Fixture for a sample mysql config.
    """
    return {
        "db_type": "mysql",
        "host": "localhost",
        "user": "user",
        "password": "password",
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
    Test the connect method.
    """
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    loader = MySQLLoader(mysql_config)
    loader.connect()
    mock_connect.assert_called_once_with(
        host="localhost",
        port=3306,
        user="user",
        password="password",
        database="test_db",
        allow_local_infile=True,
    )
    assert loader.connection == mock_connection


@patch("mysql.connector.connect")
def test_mysql_loader_close(mock_connect, mysql_config):
    """
    Test that the close method correctly closes the connection.
    """
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    loader = MySQLLoader(mysql_config)
    loader.connect()
    assert loader.connection is not None
    loader.close()
    mock_connection.close.assert_called_once()
    assert loader.connection is None


@patch("mysql.connector.connect")
@patch("tempfile.NamedTemporaryFile")
@patch("os.remove")
def test_mysql_loader_load_dataframe_replace(
    mock_remove, mock_tmp_file, mock_connect, mysql_config, sample_df
):
    """
    Test the load_dataframe method with if_exists='replace'.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_tmp_file.return_value.__enter__.return_value.name = "dummy_path.csv"

    config = {**mysql_config, "if_exists": "replace"}
    loader = MySQLLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    expected_schema = "CREATE TABLE IF NOT EXISTS `test_table` (`col_int` BIGINT, `col_float` DOUBLE, `col_bool` BOOLEAN, `col_str` TEXT, `col_date` DATETIME);"
    mock_cursor.execute.assert_any_call(expected_schema)
    mock_cursor.execute.assert_any_call("TRUNCATE TABLE `test_table`;")

    load_data_call = [
        c for c in mock_cursor.execute.call_args_list if "LOAD DATA" in c[0][0]
    ][0]
    assert "LOAD DATA LOCAL INFILE 'dummy_path.csv'" in load_data_call[0][0]
    assert "INTO TABLE `test_table`" in load_data_call[0][0]

    mock_connection.commit.assert_called_once()
    mock_remove.assert_called_once_with("dummy_path.csv")


@patch("mysql.connector.connect")
@patch("tempfile.NamedTemporaryFile")
@patch("os.remove")
def test_mysql_loader_load_dataframe_append(
    mock_remove, mock_tmp_file, mock_connect, mysql_config, sample_df
):
    """
    Test the load_dataframe method with if_exists='append'.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_tmp_file.return_value.__enter__.return_value.name = "dummy_path.csv"

    config = {**mysql_config, "if_exists": "append"}
    loader = MySQLLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    expected_schema = "CREATE TABLE IF NOT EXISTS `test_table` (`col_int` BIGINT, `col_float` DOUBLE, `col_bool` BOOLEAN, `col_str` TEXT, `col_date` DATETIME);"
    mock_cursor.execute.assert_any_call(expected_schema)

    for a_call in mock_cursor.execute.call_args_list:
        assert "TRUNCATE TABLE" not in a_call[0][0]

    load_data_call = [
        c for c in mock_cursor.execute.call_args_list if "LOAD DATA" in c[0][0]
    ][0]
    assert "LOAD DATA LOCAL INFILE 'dummy_path.csv'" in load_data_call[0][0]
    assert "INTO TABLE `test_table`" in load_data_call[0][0]

    mock_connection.commit.assert_called_once()
    mock_remove.assert_called_once_with("dummy_path.csv")


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
    empty_df = pd.DataFrame()
    loader.load_dataframe(empty_df, "test_table")

    mock_cursor.execute.assert_not_called()


@patch("mysql.connector.connect")
def test_mysql_loader_load_dataframe_after_close(mock_connect, mysql_config, sample_df):
    """
    Test that load_dataframe raises an error if the connection has been closed.
    """
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    loader = MySQLLoader(mysql_config)
    loader.connect()
    loader.close()

    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")


@patch("mysql.connector.connect")
@patch("tempfile.NamedTemporaryFile")
@patch("os.remove")
def test_mysql_loader_load_dataframe_exception_rollbacks(
    mock_remove, mock_tmp_file, mock_connect, mysql_config, sample_df
):
    """
    Test that the connection is rolled back if an exception occurs.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("Test exception")

    mock_tmp_file.return_value.__enter__.return_value.name = "dummy_path.csv"

    loader = MySQLLoader(mysql_config)
    loader.connect()

    with pytest.raises(Exception, match="Test exception"):
        loader.load_dataframe(sample_df, "test_table")

    mock_connection.rollback.assert_called_once()
    mock_connection.commit.assert_not_called()
    mock_remove.assert_called_once_with("dummy_path.csv")
