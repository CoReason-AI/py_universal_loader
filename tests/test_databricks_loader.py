# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

import unittest
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd

from py_universal_loader.databricks_loader import DatabricksLoader


class TestDatabricksLoader(unittest.TestCase):
    def setUp(self):
        self.config = {
            "db_type": "databricks",
            "server_hostname": "test_hostname",
            "http_path": "test_http_path",
            "access_token": "test_access_token",
            "s3_bucket": "test_bucket",
            "iam_role_arn": "test_iam_role",
        }
        self.loader = DatabricksLoader(self.config)

    @patch("py_universal_loader.databricks_loader.pyodbc.connect")
    def test_connect_success(self, mock_connect):
        """
        Tests that the connect method constructs the correct connection string and calls pyodbc.connect.
        """
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        self.loader.connect()

        expected_conn_str = (
            "Driver=/Library/simba/spark/lib/libsparkodbc_sb64-universal.dylib;"
            "Host=test_hostname;"
            "Port=443;"
            "HTTPPath=test_http_path;"
            "SSL=1;"
            "ThriftTransport=2;"
            "AuthMech=3;"
            "UID=token;"
            "PWD=test_access_token"
        )
        mock_connect.assert_called_once_with(expected_conn_str, autocommit=True)
        self.assertEqual(self.loader.connection, mock_conn)

    def test_close(self):
        """
        Tests that the close method calls the connection's close method.
        """
        mock_conn = MagicMock()
        self.loader.connection = mock_conn

        self.loader.close()

        mock_conn.close.assert_called_once()
        self.assertIsNone(self.loader.connection)

    @patch("py_universal_loader.databricks_loader.uuid.uuid4", return_value="test-uuid")
    @patch("py_universal_loader.databricks_loader.boto3.client")
    def test_load_dataframe_success(self, mock_boto3_client, mock_uuid):
        """
        Tests the successful execution of the load_dataframe method.
        """
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        self.loader.s3_client = mock_s3

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        self.loader.connection = mock_conn

        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        table_name = "test_table"

        self.loader.load_dataframe(df, table_name)

        mock_s3.upload_fileobj.assert_called_once()
        mock_cursor.execute.assert_any_call(
            "CREATE TABLE IF NOT EXISTS test_table (`col1` BIGINT, `col2` STRING);"
        )
        expected_copy_sql = """
                    COPY INTO test_table
                    FROM 's3://test_bucket/staging/test_table_test-uuid.parquet'
                    WITH (CREDENTIAL (AWS_IAM_ROLE = 'test_iam_role'))
                    FILEFORMAT = PARQUET
                """
        mock_cursor.execute.assert_any_call(expected_copy_sql)
        mock_s3.delete_object.assert_called_once_with(
            Bucket="test_bucket", Key="staging/test_table_test-uuid.parquet"
        )

    @patch("py_universal_loader.databricks_loader.boto3.client")
    def test_load_dataframe_s3_upload_fails(self, mock_boto3_client):
        """
        Tests that an IOError is raised if the S3 upload fails.
        """
        mock_s3 = MagicMock()
        mock_s3.upload_fileobj.side_effect = Exception("S3 Upload Error")
        mock_boto3_client.return_value = mock_s3
        self.loader.s3_client = mock_s3
        self.loader.connection = MagicMock()

        df = pd.DataFrame({"col1": [1, 2]})
        with pytest.raises(IOError, match="Failed to upload staged file to S3"):
            self.loader.load_dataframe(df, "test_table")

    @patch("py_universal_loader.databricks_loader.uuid.uuid4", return_value="test-uuid")
    @patch("py_universal_loader.databricks_loader.boto3.client")
    def test_load_dataframe_copy_fails(self, mock_boto3_client, mock_uuid):
        """
        Tests that an IOError is raised and the S3 file is not deleted if the COPY command fails.
        """
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        self.loader.s3_client = mock_s3

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = [None, Exception("COPY failed")]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        self.loader.connection = mock_conn

        df = pd.DataFrame({"col1": [1, 2]})
        with pytest.raises(
            IOError, match="Failed to load data into Databricks from S3"
        ):
            self.loader.load_dataframe(df, "test_table")

        mock_s3.delete_object.assert_not_called()


if __name__ == "__main__":
    unittest.main()
