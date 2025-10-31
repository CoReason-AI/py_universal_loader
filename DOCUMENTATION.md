# py-universal-loader Documentation

## Introduction

`py-universal-loader` is a powerful Python package designed to simplify and accelerate the process of loading `pandas.DataFrame` objects into a wide variety of SQL databases and data warehouses. It provides a single, configuration-driven API that abstracts the most efficient native bulk-loading methods (e.g., `COPY`, `BULK INSERT`) for each backend, ensuring optimal performance without requiring database-specific code.

## Core Concepts

The package is built around two key components:

- **`BaseLoader`:** An abstract base class that defines the common interface for all database-specific loaders.
- **`get_loader`:** A factory function that takes a configuration dictionary and returns an instance of the appropriate loader.

This design allows you to switch between different databases by simply changing the configuration, without altering your application code.

## Supported Databases

`py-universal-loader` supports a wide range of databases, each with its own optimized loading strategy:

- **PostgreSQL:** Uses the `COPY FROM STDIN` command to stream data directly from an in-memory CSV buffer.
- **Amazon Redshift:** Stages the DataFrame as a Parquet file in an S3 bucket and uses the `COPY` command to load it.
- **Databricks SQL:** Stages the DataFrame to a cloud storage location and uses the `COPY INTO` command.
- **Google BigQuery:** Leverages the `google-cloud-bigquery` client library's `load_table_from_dataframe()` method.
- **Snowflake:** Stages the DataFrame as a Parquet file in a cloud storage location and uses the `COPY INTO` command.
- **MySQL / MariaDB:** Saves the DataFrame to a temporary local CSV file and uses the `LOAD DATA LOCAL INFILE` command.
- **Microsoft SQL Server:** Stages the DataFrame as a CSV file in a location accessible to the SQL Server instance and uses the `BULK INSERT` command.
- **SQLite:** Uses the `pandas.DataFrame.to_sql()` method, which is highly optimized for SQLite.
- **DuckDB:** Registers the DataFrame as a virtual table using `duckdb.register()` and then uses an `INSERT INTO ... SELECT * FROM` statement.

## API Reference

### `get_loader(config: dict)`

A factory function that returns a database-specific loader instance.

- **Parameters:**
  - `config` (dict): A dictionary containing the database type and connection parameters.
- **Returns:**
  - An instance of a `BaseLoader` subclass.

### `loader.connect()`

Establishes a connection to the database.

### `loader.close()`

Closes the database connection.

### `loader.load_dataframe(df: pd.DataFrame, table_name: str)`

Loads a pandas DataFrame into the specified table.

- **Parameters:**
  - `df` (pd.DataFrame): The DataFrame to load.
  - `table_name` (str): The name of the target table.

## Configuration

The `config` dictionary is used to specify the database type and connection parameters. Here are some examples:

### PostgreSQL

```python
config = {
    "db_type": "postgres",
    "host": "localhost",
    "port": 5432,
    "user": "myuser",
    "password": "mypassword",
    "database": "mydatabase",
}
```

### Amazon Redshift

```python
config = {
    "db_type": "redshift",
    "host": "my-redshift-cluster.us-east-1.redshift.amazonaws.com",
    "port": 5439,
    "user": "myuser",
    "password": "mypassword",
    "database": "mydatabase",
    "s3_bucket": "my-s3-bucket",
    "aws_access_key_id": "...",
    "aws_secret_access_key": "...",
    "iam_role_arn": "arn:aws:iam::123456789012:role/my-redshift-role",
}
```

### SQLite

```python
config = {
    "db_type": "sqlite",
    "db_path": ":memory:",  # Or a file path
}
```

## Usage Example

Here's a complete example of how to use `py-universal-loader` to load a pandas DataFrame into a PostgreSQL database:

```python
import pandas as pd
from py_universal_loader import get_loader

# Sample DataFrame
data = {'col1': [1, 2], 'col2': [3, 4]}
df = pd.DataFrame(data)

# Configuration for PostgreSQL
config = {
    "db_type": "postgres",
    "host": "localhost",
    "port": 5432,
    "user": "myuser",
    "password": "mypassword",
    "database": "mydatabase",
}

# Get the loader and load the data
loader = get_loader(config)
try:
    loader.connect()
    loader.load_dataframe(df, "my_table")
finally:
    loader.close()
```
