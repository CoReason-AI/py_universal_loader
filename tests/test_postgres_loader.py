# Copyright (c) 2025 CoReason, Inc.
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
from py_universal_loader.postgres_loader import PostgresLoader


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
def postgres_config():
    """
    Fixture for a sample PostgreSQL config.
    """
    return {
        "db_type": "postgres",
        "dbname": "testdb",
        "user": "testuser",
        "password": "testpassword",
        "host": "localhost",
    }


def test_get_loader_postgres(postgres_config):
    """
    Test that get_loader returns a PostgresLoader instance for db_type 'postgres'.
    """
    loader = get_loader(postgres_config)
    assert isinstance(loader, PostgresLoader)


@patch("psycopg2.connect")
def test_postgres_loader_connect(mock_connect, postgres_config):
    """
    Test the connect method with a mock connection.
    """
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    loader = PostgresLoader(postgres_config)
    loader.connect()

    mock_connect.assert_called_once_with(
        "dbname='testdb' user='testuser' host='localhost' password='testpassword'"
    )
    assert loader.connection == mock_connection


def test_postgres_loader_close(postgres_config):
    """
    Test that the close method correctly closes the connection.
    """
    loader = PostgresLoader(postgres_config)
    mock_connection = MagicMock()
    loader.connection = mock_connection
    loader.close()
    mock_connection.close.assert_called_once()
    assert loader.connection is None


@patch("psycopg2.connect")
def test_postgres_loader_load_dataframe_replace(
    mock_connect, postgres_config, sample_df
):
    """
    Test the load_dataframe method with if_exists='replace'.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    config = {**postgres_config, "if_exists": "replace"}
    loader = PostgresLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    expected_schema = "CREATE TABLE IF NOT EXISTS test_table (col_int BIGINT, col_float DOUBLE PRECISION, col_bool BOOLEAN, col_str TEXT, col_date TIMESTAMP);"
    mock_cursor.execute.assert_any_call(expected_schema)
    mock_cursor.execute.assert_any_call("TRUNCATE TABLE test_table;")
    assert mock_cursor.copy_expert.call_count == 1
    mock_connection.commit.assert_called_once()


@patch("psycopg2.connect")
def test_postgres_loader_load_dataframe_append(
    mock_connect, postgres_config, sample_df
):
    """
    Test the load_dataframe method with if_exists='append'.
    """
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    config = {**postgres_config, "if_exists": "append"}
    loader = PostgresLoader(config)
    loader.connect()
    loader.load_dataframe(sample_df, "test_table")

    expected_schema = "CREATE TABLE IF NOT EXISTS test_table (col_int BIGINT, col_float DOUBLE PRECISION, col_bool BOOLEAN, col_str TEXT, col_date TIMESTAMP);"
    mock_cursor.execute.assert_any_call(expected_schema)

    # Check that "TRUNCATE TABLE" was not called
    for a_call in mock_cursor.execute.call_args_list:
        assert "TRUNCATE TABLE" not in a_call[0][0]

    assert mock_cursor.copy_expert.call_count == 1
    mock_connection.commit.assert_called_once()


def test_postgres_loader_load_dataframe_no_connection(postgres_config, sample_df):
    """
    Test that load_dataframe raises a ConnectionError if connect has not been called.
    """
    loader = PostgresLoader(postgres_config)
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")
