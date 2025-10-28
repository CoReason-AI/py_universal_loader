# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from py_universal_loader.main import get_loader
from py_universal_loader.sqlite_loader import SQLiteLoader


@pytest.fixture
def sample_df():
    """
    Fixture for a sample pandas DataFrame.
    """
    return pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})


def test_get_loader_sqlite():
    """
    Test that get_loader returns a SQLiteLoader instance for db_type 'sqlite'.
    """
    config = {"db_type": "sqlite"}
    loader = get_loader(config)
    assert isinstance(loader, SQLiteLoader)


def test_sqlite_loader_connect_in_memory():
    """
    Test the connect method with an in-memory SQLite database.
    """
    config = {"db_type": "sqlite", "db_path": ":memory:"}
    loader = SQLiteLoader(config)
    loader.connect()
    assert isinstance(loader.connection, sqlite3.Connection)
    loader.close()


def test_sqlite_loader_connect_on_disk(tmp_path: Path):
    """
    Test the connect method with a file-based SQLite database.
    """
    db_file = tmp_path / "test.db"
    config = {"db_type": "sqlite", "db_path": str(db_file)}
    loader = SQLiteLoader(config)
    loader.connect()
    assert db_file.exists()
    assert isinstance(loader.connection, sqlite3.Connection)
    loader.close()


def test_sqlite_loader_close():
    """
    Test that the close method correctly closes the connection.
    """
    config = {"db_type": "sqlite", "db_path": ":memory:"}
    loader = SQLiteLoader(config)
    loader.connect()
    assert loader.connection is not None
    loader.close()
    assert loader.connection is None


def test_sqlite_loader_load_dataframe(sample_df: pd.DataFrame):
    """
    Test the load_dataframe method with a sample DataFrame.
    """
    config = {"db_type": "sqlite", "db_path": ":memory:"}
    loader = SQLiteLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    # Verify the data was loaded correctly
    assert loader.connection is not None
    cursor = loader.connection.cursor()
    cursor.execute("SELECT * FROM test_table")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1, "A")
    assert rows[1] == (2, "B")
    loader.close()


def test_sqlite_loader_load_dataframe_empty():
    """
    Test the load_dataframe method with an empty DataFrame.
    """
    config = {"db_type": "sqlite", "db_path": ":memory:"}
    loader = SQLiteLoader(config)
    loader.connect()
    empty_df = pd.DataFrame({"col1": [], "col2": []})
    loader.load_dataframe(empty_df, "test_table_empty")

    # Verify the table is empty
    assert loader.connection is not None
    cursor = loader.connection.cursor()
    cursor.execute("SELECT * FROM test_table_empty")
    rows = cursor.fetchall()
    assert len(rows) == 0
    loader.close()


def test_sqlite_loader_load_dataframe_no_connection(sample_df: pd.DataFrame):
    """
    Test that load_dataframe raises a ConnectionError if connect has not been called.
    """
    config = {"db_type": "sqlite"}
    loader = SQLiteLoader(config)
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")


def test_sqlite_loader_load_dataframe_after_close(sample_df: pd.DataFrame):
    """
    Test that load_dataframe raises an error if the connection has been closed.
    """
    config = {"db_type": "sqlite", "db_path": ":memory:"}
    loader = SQLiteLoader(config)
    loader.connect()
    loader.close()
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")
