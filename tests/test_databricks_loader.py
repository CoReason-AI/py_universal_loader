# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

import unittest
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

    def test_connect_raises_not_implemented_error(self):
        with pytest.raises(NotImplementedError):
            self.loader.connect()

    def test_close_raises_not_implemented_error(self):
        with pytest.raises(NotImplementedError):
            self.loader.close()

    def test_load_dataframe_raises_not_implemented_error(self):
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        with pytest.raises(NotImplementedError):
            self.loader.load_dataframe(df, "test_table")


if __name__ == "__main__":
    unittest.main()
