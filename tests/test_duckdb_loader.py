# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from py_universal_loader.duckdb_loader import DuckDBLoader
from py_universal_loader.main import get_loader


@pytest.fixture
def sample_df():
    """
    Fixture for a sample pandas DataFrame.
    """
    return pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})


def test_get_loader_duckdb():
    """
    Test that get_loader returns a DuckDBLoader instance for db_type 'duckdb'.
    """
    config = {"db_type": "duckdb"}
    loader = get_loader(config)
    assert isinstance(loader, DuckDBLoader)


def test_duckdb_loader_connect_in_memory():
    """
    Test the connect method with an in-memory DuckDB database.
    """
    config = {"db_type": "duckdb", "db_path": ":memory:"}
    loader = DuckDBLoader(config)
    loader.connect()
    assert isinstance(loader.connection, duckdb.DuckDBPyConnection)
    loader.close()


def test_duckdb_loader_connect_on_disk(tmp_path: Path):
    """
    Test the connect method with a file-based DuckDB database.
    """
    db_file = tmp_path / "test.db"
    config = {"db_type": "duckdb", "db_path": str(db_file)}
    loader = DuckDBLoader(config)
    loader.connect()
    assert db_file.exists()
    assert isinstance(loader.connection, duckdb.DuckDBPyConnection)
    loader.close()


def test_duckdb_loader_close():
    """
    Test that the close method correctly closes the connection.
    """
    config = {"db_type": "duckdb", "db_path": ":memory:"}
    loader = DuckDBLoader(config)
    loader.connect()
    assert loader.connection is not None
    loader.close()
    assert loader.connection is None


def test_duckdb_loader_load_dataframe(sample_df: pd.DataFrame):
    """
    Test the load_dataframe method with a sample DataFrame.
    """
    config = {"db_type": "duckdb", "db_path": ":memory:"}
    loader = DuckDBLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    # Verify the data was loaded correctly
    assert loader.connection is not None
    result_df = loader.connection.execute("SELECT * FROM test_table").fetchdf()
    pd.testing.assert_frame_equal(sample_df, result_df)
    loader.close()


def test_duckdb_loader_load_dataframe_empty():
    """
    Test the load_dataframe method with an empty DataFrame.
    """
    config = {"db_type": "duckdb", "db_path": ":memory:"}
    loader = DuckDBLoader(config)
    loader.connect()
    empty_df = pd.DataFrame({"col1": [], "col2": []})
    loader.load_dataframe(empty_df, "test_table_empty")

    # Verify the table is empty
    assert loader.connection is not None
    result_df = loader.connection.execute("SELECT * FROM test_table_empty").fetchdf()
    assert result_df.empty
    loader.close()


def test_duckdb_loader_load_dataframe_no_connection(sample_df: pd.DataFrame):
    """
    Test that load_dataframe raises a ConnectionError if connect has not been called.
    """
    config = {"db_type": "duckdb"}
    loader = DuckDBLoader(config)
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")


def test_duckdb_loader_load_dataframe_append(sample_df: pd.DataFrame):
    """
    Test that loading a DataFrame appends to an existing table.
    """
    config = {"db_type": "duckdb", "db_path": ":memory:"}
    loader = DuckDBLoader(config)
    loader.connect()

    # Load the first DataFrame
    loader.load_dataframe(sample_df, "test_append_table")

    # Load a second DataFrame to append
    append_df = pd.DataFrame({"col1": [3, 4], "col2": ["C", "D"]})
    loader.load_dataframe(append_df, "test_append_table")

    # Verify the data was appended correctly
    assert loader.connection is not None
    result_df = loader.connection.execute("SELECT * FROM test_append_table").fetchdf()

    expected_df = pd.concat([sample_df, append_df], ignore_index=True)
    pd.testing.assert_frame_equal(result_df, expected_df)

    loader.close()


def test_duckdb_loader_load_dataframe_after_close(sample_df: pd.DataFrame):
    """
    Test that load_dataframe raises an error if the connection has been closed.
    """
    config = {"db_type": "duckdb", "db_path": ":memory:"}
    loader = DuckDBLoader(config)
    loader.connect()
    loader.close()
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")
