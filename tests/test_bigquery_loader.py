# Copyright (c) 2025 CoReason, Inc
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from google.cloud import bigquery

from py_universal_loader.bigquery_loader import BigQueryLoader

@pytest.fixture
def mock_bigquery_client():
    with patch('google.cloud.bigquery.Client') as mock_client:
        yield mock_client

@pytest.fixture
def bigquery_loader(mock_bigquery_client):
    config = {
        'db_type': 'bigquery',
        'project_id': 'test-project',
        'dataset_id': 'test-dataset'
    }
    loader = BigQueryLoader(config)
    loader.client = mock_bigquery_client
    return loader

def test_connect(mock_bigquery_client):
    config = {'project_id': 'test-project'}
    loader = BigQueryLoader(config)
    loader.connect()
    mock_bigquery_client.assert_called_once_with(project='test-project')

def test_close(bigquery_loader):
    bigquery_loader.connect()
    bigquery_loader.close()
    assert bigquery_loader.client is None

def test_load_dataframe_replace(bigquery_loader):
    df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    table_name = 'test_table'

    bigquery_loader.load_dataframe(df, table_name)

    bigquery_loader.client.load_table_from_dataframe.assert_called_once()
    call_args = bigquery_loader.client.load_table_from_dataframe.call_args
    assert call_args[0][0].equals(df)
    assert call_args[0][1] == 'test-dataset.test_table'
    job_config = call_args[1]['job_config']
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE

def test_load_dataframe_append(bigquery_loader):
    bigquery_loader.config['if_exists'] = 'append'
    df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    table_name = 'test_table'

    bigquery_loader.load_dataframe(df, table_name)

    bigquery_loader.client.load_table_from_dataframe.assert_called_once()
    call_args = bigquery_loader.client.load_table_from_dataframe.call_args
    job_config = call_args[1]['job_config']
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND

def test_load_dataframe_empty(bigquery_loader):
    df = pd.DataFrame()
    table_name = 'test_table'

    bigquery_loader.load_dataframe(df, table_name)

    bigquery_loader.client.load_table_from_dataframe.assert_not_called()

def test_load_dataframe_no_connection(bigquery_loader):
    bigquery_loader.client = None
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ConnectionError, match="Database connection is not established."):
        bigquery_loader.load_dataframe(df, 'test_table')

def test_load_dataframe_invalid_if_exists(bigquery_loader):
    bigquery_loader.config['if_exists'] = 'fail'
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(ValueError, match="Unsupported if_exists option: fail"):
        bigquery_loader.load_dataframe(df, 'test_table')
