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
from pandas.testing import assert_frame_equal

from py_universal_loader.main import get_loader
from py_universal_loader.sqlite_loader import SQLiteLoader


@pytest.fixture
def sample_df():
    """
    Fixture for a sample pandas DataFrame.
    """
    return pd.DataFrame(
        {
            "col_int": [1, 2],
            "col_str": ["A", "B"],
        }
    )


@pytest.fixture
def sqlite_config():
    """
    Fixture for a sample SQLite config for an in-memory database.
    """
    return {
        "db_type": "sqlite",
        "db_path": ":memory:",
    }


@pytest.fixture
def loader(sqlite_config):
    """
    Fixture to provide a connected and disconnected SQLiteLoader instance.
    """
    loader = SQLiteLoader(sqlite_config)
    loader.connect()
    yield loader
    loader.close()


def test_get_loader_sqlite(sqlite_config):
    """
    Test that get_loader returns a SQLiteLoader instance for db_type 'sqlite'.
    """
    loader = get_loader(sqlite_config)
    assert isinstance(loader, SQLiteLoader)


@patch("sqlite3.connect")
def test_sqlite_loader_connect(mock_connect, sqlite_config):
    """
    Test the connect method with a mock connection.
    """
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    loader = SQLiteLoader(sqlite_config)
    loader.connect()
    mock_connect.assert_called_once_with(":memory:")
    assert loader.connection == mock_connection


def test_sqlite_loader_close(sqlite_config):
    """
    Test that the close method correctly closes the connection.
    """
    loader = SQLiteLoader(sqlite_config)
    mock_connection = MagicMock()
    loader.connection = mock_connection
    loader.close()
    mock_connection.close.assert_called_once()
    assert loader.connection is None


def test_sqlite_loader_load_dataframe_replace(loader, sample_df):
    """
    Test the load_dataframe method with if_exists='replace' using a real in-memory db.
    """
    table_name = "test_replace"
    loader.config["if_exists"] = "replace"

    # First load
    loader.load_dataframe(sample_df, table_name)
    loaded_df = pd.read_sql(f"SELECT * FROM {table_name}", loader.connection)
    assert_frame_equal(loaded_df, sample_df)

    # Second load (should replace the first)
    df_new = pd.DataFrame({"col_int": [3, 4], "col_str": ["C", "D"]})
    loader.load_dataframe(df_new, table_name)
    loaded_df_new = pd.read_sql(f"SELECT * FROM {table_name}", loader.connection)
    assert_frame_equal(loaded_df_new, df_new)


def test_sqlite_loader_load_dataframe_append(loader, sample_df):
    """
    Test the load_dataframe method with if_exists='append' using a real in-memory db.
    """
    table_name = "test_append"
    loader.config["if_exists"] = "append"

    # First load
    loader.load_dataframe(sample_df, table_name)
    # Second load (should append)
    loader.load_dataframe(sample_df, table_name)

    loaded_df = pd.read_sql(f"SELECT * FROM {table_name}", loader.connection)
    expected_df = pd.concat([sample_df, sample_df], ignore_index=True)
    assert_frame_equal(loaded_df, expected_df)


def test_sqlite_loader_load_dataframe_empty(loader):
    """
    Test that load_dataframe skips execution for an empty DataFrame.
    """
    # This test doesn't need to patch to_sql, just verify no table is created
    table_name = "test_empty"
    empty_df = pd.DataFrame({"col1": []})
    loader.load_dataframe(empty_df, table_name)

    with pytest.raises(Exception):
        # We expect an exception because the table should not exist
        pd.read_sql(f"SELECT * FROM {table_name}", loader.connection)


def test_sqlite_loader_load_dataframe_no_connection(sqlite_config, sample_df):
    """
    Test that load_dataframe raises a ConnectionError if connect has not been called.
    """
    loader = SQLiteLoader(sqlite_config)
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")


def test_sqlite_loader_load_dataframe_after_close(loader, sample_df):
    """
    Test that load_dataframe raises an error if the connection has been closed.
    """
    # The 'loader' fixture provides a connected loader, so we close it here
    loader.close()
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")
