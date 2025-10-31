# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

import sqlite3

import pandas as pd
from pandas import errors as pd_errors
import pytest

from py_universal_loader.main import get_loader
from py_universal_loader.sqlite_loader import SQLiteLoader


@pytest.fixture
def sample_df():
    """
    Fixture for a sample pandas DataFrame.
    """
    return pd.DataFrame({"col_int": [1, 2], "col_str": ["A", "B"]})


@pytest.fixture
def sqlite_config():
    """
    Fixture for a sample SQLite config (in-memory).
    """
    return {"db_type": "sqlite", "db_path": ":memory:"}


def test_get_loader_sqlite(sqlite_config):
    """
    Test that get_loader returns a SQLiteLoader instance for db_type 'sqlite'.
    """
    loader = get_loader(sqlite_config)
    assert isinstance(loader, SQLiteLoader)


def test_sqlite_loader_connect(sqlite_config):
    """
    Test the connect method establishes a connection.
    """
    loader = SQLiteLoader(sqlite_config)
    loader.connect()
    assert isinstance(loader.connection, sqlite3.Connection)
    loader.close()


def test_sqlite_loader_close(sqlite_config):
    """
    Test that the close method correctly closes the connection.
    """
    loader = SQLiteLoader(sqlite_config)
    loader.connect()
    connection = loader.connection
    loader.close()
    assert loader.connection is None
    with pytest.raises(
        sqlite3.ProgrammingError, match="Cannot operate on a closed database."
    ):
        connection.execute("SELECT 1")


def test_sqlite_loader_load_dataframe_replace(sqlite_config, sample_df):
    """
    Test the load_dataframe method with if_exists='replace'.
    """
    table_name = "test_replace"
    loader = SQLiteLoader({**sqlite_config, "if_exists": "replace"})
    loader.connect()

    # First load
    loader.load_dataframe(sample_df, table_name)
    with loader.connection:
        result_df = pd.read_sql(f"SELECT * FROM {table_name}", loader.connection)
        pd.testing.assert_frame_equal(result_df, sample_df)

    # Second load (should replace)
    df2 = pd.DataFrame({"col_int": [3, 4], "col_str": ["C", "D"]})
    loader.load_dataframe(df2, table_name)
    with loader.connection:
        result_df2 = pd.read_sql(f"SELECT * FROM {table_name}", loader.connection)
        pd.testing.assert_frame_equal(result_df2, df2)

    loader.close()


def test_sqlite_loader_load_dataframe_append(sqlite_config, sample_df):
    """
    Test the load_dataframe method with if_exists='append'.
    """
    table_name = "test_append"
    loader = SQLiteLoader({**sqlite_config, "if_exists": "append"})
    loader.connect()

    # First load
    loader.load_dataframe(sample_df, table_name)

    # Second load (should append)
    df2 = pd.DataFrame({"col_int": [3, 4], "col_str": ["C", "D"]})
    loader.load_dataframe(df2, table_name)

    expected_df = pd.concat([sample_df, df2], ignore_index=True)
    with loader.connection:
        result_df = pd.read_sql(f"SELECT * FROM {table_name}", loader.connection)
        pd.testing.assert_frame_equal(result_df, expected_df)

    loader.close()


def test_sqlite_loader_load_dataframe_empty(sqlite_config):
    """
    Test that load_dataframe skips execution for an empty DataFrame.
    """
    table_name = "test_empty"
    loader = SQLiteLoader(sqlite_config)
    loader.connect()
    empty_df = pd.DataFrame({"col1": []})
    loader.load_dataframe(empty_df, table_name)

    # Check that the table was not created
    with loader.connection:
        with pytest.raises(pd_errors.DatabaseError, match="no such table"):
            pd.read_sql(f"SELECT * FROM {table_name}", loader.connection)

    loader.close()


def test_sqlite_loader_load_dataframe_no_connection(sqlite_config, sample_df):
    """
    Test that load_dataframe raises a ConnectionError if connect has not been called.
    """
    loader = SQLiteLoader(sqlite_config)
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")


def test_sqlite_loader_load_dataframe_after_close(sqlite_config, sample_df):
    """
    Test that load_dataframe raises an error if the connection has been closed.
    """
    loader = SQLiteLoader(sqlite_config)
    loader.connect()
    loader.close()

    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")
