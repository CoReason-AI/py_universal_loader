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
from py_universal_loader.postgres_loader import PostgresLoader


@pytest.fixture
def sample_df():
    """
    Fixture for a sample pandas DataFrame.
    """
    return pd.DataFrame(
        {
            "col_int": [1, 2],
            "col_str": ["A", "B"],
            "col_float": [1.1, 2.2],
            "col_bool": [True, False],
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
        "host": "localhost",
        "port": 5432,
        "db": "testdb",
        "user": "testuser",
        "password": "testpassword",
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
    Test the connect method establishes a connection.
    """
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    loader = PostgresLoader(postgres_config)
    loader.connect()

    assert loader.connection == mock_conn
    mock_connect.assert_called_once()
    loader.close()


@patch("psycopg2.connect")
def test_postgres_loader_close(mock_connect, postgres_config):
    """
    Test that the close method correctly closes the connection.
    """
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    loader = PostgresLoader(postgres_config)
    loader.connect()
    connection = loader.connection
    loader.close()

    assert loader.connection is None
    connection.close.assert_called_once()


@patch("psycopg2.connect")
def test_postgres_loader_load_dataframe_replace(
    mock_connect, postgres_config, sample_df
):
    """
    Test the load_dataframe method with if_exists='replace'.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    table_name = "test_replace"
    loader = PostgresLoader({**postgres_config, "if_exists": "replace"})
    loader.connect()

    loader.load_dataframe(sample_df, table_name)

    mock_cursor.execute.assert_any_call(f'DROP TABLE IF EXISTS "{table_name}"')
    mock_cursor.execute.assert_any_call(
        'CREATE TABLE "test_replace" ("col_int" BIGINT, "col_str" TEXT, "col_float" DOUBLE PRECISION, "col_bool" BOOLEAN, "col_date" TIMESTAMP)'
    )
    mock_cursor.copy_expert.assert_called_once()
    mock_conn.commit.assert_called_once()

    loader.close()


@patch("psycopg2.connect")
def test_postgres_loader_load_dataframe_append(
    mock_connect, postgres_config, sample_df
):
    """
    Test the load_dataframe method with if_exists='append'.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    table_name = "test_append"
    loader = PostgresLoader({**postgres_config, "if_exists": "append"})
    loader.connect()

    loader.load_dataframe(sample_df, table_name)

    mock_cursor.execute.assert_called_once_with(
        'CREATE TABLE IF NOT EXISTS "test_append" ("col_int" BIGINT, "col_str" TEXT, "col_float" DOUBLE PRECISION, "col_bool" BOOLEAN, "col_date" TIMESTAMP)'
    )
    mock_cursor.copy_expert.assert_called_once()
    mock_conn.commit.assert_called_once()

    loader.close()


@patch("psycopg2.connect")
def test_postgres_loader_load_dataframe_exception_rollbacks(
    mock_connect, postgres_config, sample_df
):
    """
    Test that a failed load operation rolls back the transaction.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy_expert.side_effect = Exception("Test Exception")

    loader = PostgresLoader(postgres_config)
    loader.connect()

    with pytest.raises(Exception, match="Test Exception"):
        loader.load_dataframe(sample_df, "test_table")

    mock_conn.commit.assert_not_called()
    mock_conn.rollback.assert_called_once()

    loader.close()


@patch("psycopg2.connect")
def test_postgres_loader_load_dataframe_empty(mock_connect, postgres_config, sample_df):
    """
    Test that load_dataframe skips execution for an empty DataFrame.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    table_name = "test_empty"
    loader = PostgresLoader(postgres_config)
    loader.connect()
    empty_df = pd.DataFrame({"col1": []})
    loader.load_dataframe(empty_df, table_name)

    mock_cursor.execute.assert_not_called()
    mock_cursor.copy_expert.assert_not_called()

    loader.close()


def test_postgres_loader_load_dataframe_no_connection(postgres_config, sample_df):
    """
    Test that load_dataframe raises a ConnectionError if connect has not been called.
    """
    loader = PostgresLoader(postgres_config)
    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")


@patch("psycopg2.connect")
def test_postgres_loader_load_dataframe_after_close(
    mock_connect, postgres_config, sample_df
):
    """
    Test that load_dataframe raises an error if the connection has been closed.
    """
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    loader = PostgresLoader(postgres_config)
    loader.connect()
    loader.close()

    with pytest.raises(
        ConnectionError, match="Database connection is not established."
    ):
        loader.load_dataframe(sample_df, "test_table")
