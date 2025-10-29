# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from py_universal_loader.snowflake_loader import SnowflakeLoader


class TestSnowflakeLoader(unittest.TestCase):
    def setUp(self):
        self.config = {
            "user": "test_user",
            "password": "test_password",
            "account": "test_account",
            "warehouse": "test_warehouse",
            "database": "test_database",
            "schema": "test_schema",
            "s3_bucket": "test_bucket",
            "iam_role_arn": "test_iam_role",
        }
        self.loader = SnowflakeLoader(self.config)

    @patch("snowflake.connector.connect")
    @patch("boto3.client")
    def test_connect(self, mock_boto3_client, mock_snowflake_connect):
        self.loader.connect()
        mock_snowflake_connect.assert_called_once_with(
            user="test_user",
            password="test_password",
            account="test_account",
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema",
        )
        mock_boto3_client.assert_called_once_with("s3")
        self.assertIsNotNone(self.loader.connection)
        self.assertIsNotNone(self.loader.s3_client)

    def test_close(self):
        connection_mock = MagicMock()
        self.loader.connection = connection_mock
        self.loader.close()
        connection_mock.close.assert_called_once()
        self.assertIsNone(self.loader.connection)

    @patch("snowflake.connector.connect")
    @patch("boto3.client")
    def test_load_dataframe(self, mock_boto3_client, mock_snowflake_connect):
        self.loader.connect()
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        table_name = "test_table"

        mock_cursor = MagicMock()
        self.loader.connection.cursor.return_value.__enter__.return_value = mock_cursor

        self.loader.load_dataframe(df, table_name)

        mock_cursor.execute.assert_called_once()
        self.loader.connection.commit.assert_called_once()
        self.loader.s3_client.upload_fileobj.assert_called_once()
        self.loader.s3_client.delete_object.assert_called_once()

    def test_load_dataframe_no_connection(self):
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        table_name = "test_table"
        with self.assertRaises(ConnectionError):
            self.loader.load_dataframe(df, table_name)


if __name__ == "__main__":
    unittest.main()
