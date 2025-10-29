# Copyright (c) 2025 CoReason, Inc.
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
    return pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})


def test_get_loader_mysql():
    """
    Test that get_loader returns a MySQLLoader instance for db_type 'mysql'.
    """
    config = {"db_type": "mysql"}
    loader = get_loader(config)
    assert isinstance(loader, MySQLLoader)


@patch("mysql.connector.connect")
def test_mysql_loader_connect(mock_connect):
    """
    Test the connect method.
    """
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    config = {
        "db_type": "mysql",
        "host": "localhost",
        "user": "user",
        "password": "password",
        "database": "test_db",
    }
    loader = MySQLLoader(config)
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
def test_mysql_loader_close(mock_connect):
    """
    Test that the close method correctly closes the connection.
    """
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    config = {
        "db_type": "mysql",
        "host": "localhost",
        "user": "user",
        "password": "password",
        "database": "test_db",
    }
    loader = MySQLLoader(config)
    loader.connect()
    assert loader.connection is not None
    loader.close()
    mock_connection.close.assert_called_once()
    assert loader.connection is None


@patch("mysql.connector.connect")
@patch("tempfile.NamedTemporaryFile")
@patch("os.remove")
def test_mysql_loader_load_dataframe(mock_remove, mock_tmp_file, mock_connect, sample_df):
    """
    Test the load_dataframe method.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_tmp_file.return_value.__enter__.return_value.name = "dummy_path.csv"

    config = {
        "db_type": "mysql",
        "host": "localhost",
        "user": "user",
        "password": "password",
        "database": "test_db",
    }
    loader = MySQLLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    execute_call = mock_cursor.execute.call_args[0][0]
    assert "LOAD DATA LOCAL INFILE 'dummy_path.csv'" in execute_call
    assert "INTO TABLE test_table" in execute_call
    mock_connection.commit.assert_called_once()
    mock_remove.assert_called_once_with("dummy_path.csv")


def test_mysql_loader_load_dataframe_no_connection(sample_df):
    """
    Test that load_dataframe raises a ConnectionError if connect has not been called.
    """
    config = {"db_type": "mysql"}
    loader = MySQLLoader(config)
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")
