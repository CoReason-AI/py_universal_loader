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


@pytest.fixture
def sample_df():
    """
    Fixture for a sample pandas DataFrame.
    """
    return pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})


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
