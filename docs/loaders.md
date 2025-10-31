# Supported Databases

`py-universal-loader` supports a wide range of databases, each with its own optimized loading strategy:

- **PostgreSQL:** Uses the `COPY FROM STDIN` command to stream data directly from an in-memory CSV buffer.
- **Amazon Redshift:** Stages the DataFrame as a Parquet file in an S3 bucket and uses the `COPY` command to load it. The temporary Parquet file is automatically deleted from S3 after a successful `COPY` operation.
- **Databricks SQL:** Stages the DataFrame to a cloud storage location and uses the `COPY INTO` command.
- **Google BigQuery:** Leverages the `google-cloud-bigquery` client library's `load_table_from_dataframe()` method.
- **Snowflake:** Stages the DataFrame as a Parquet file in a cloud storage location and uses the `COPY INTO` command. The temporary Parquet file is automatically deleted from cloud storage after a successful load.
- **MySQL / MariaDB:** Saves the DataFrame to a temporary local CSV file and uses the `LOAD DATA LOCAL INFILE` command.
- **Microsoft SQL Server:** Stages the DataFrame as a CSV file in a location accessible to the SQL Server instance and uses the `BULK INSERT` command.
- **SQLite:** Uses the `pandas.DataFrame.to_sql()` method, which is highly optimized for SQLite.
- **DuckDB:** Registers the DataFrame as a virtual table using `duckdb.register()` and then uses an `INSERT INTO ... SELECT * FROM` statement.

# Configuration

The `config` dictionary is used to specify the database type and connection parameters. Here are some examples:

## PostgreSQL

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

## Amazon Redshift

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

## Databricks SQL

```python
config = {
    "db_type": "databricks",
    "server_hostname": "...",
    "http_path": "...",
    "access_token": "...",
    "s3_bucket": "my-s3-bucket",
    "aws_access_key_id": "...",
    "aws_secret_access_key": "...",
}
```

## Google BigQuery

```python
config = {
    "db_type": "bigquery",
    "project_id": "my-gcp-project",
    "credentials": "/path/to/my/credentials.json",
}
```

## Snowflake

```python
config = {
    "db_type": "snowflake",
    "user": "myuser",
    "password": "mypassword",
    "account": "myaccount",
    "warehouse": "mywarehouse",
    "database": "mydatabase",
    "schema": "myschema",
    "s3_bucket": "my-s3-bucket",
    "aws_access_key_id": "...",
    "aws_secret_access_key": "...",
}
```

## MySQL / MariaDB

```python
config = {
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "user": "myuser",
    "password": "mypassword",
    "database": "mydatabase",
}
```

## Microsoft SQL Server

```python
config = {
    "db_type": "mssql",
    "server": "myserver.database.windows.net",
    "database": "mydatabase",
    "user": "myuser",
    "password": "mypassword",
    "driver": "{ODBC Driver 17 for SQL Server}",
    "azure_storage_account": "mystorageaccount",
    "azure_storage_container": "mycontainer",
    "azure_storage_key": "...",
}
```

## SQLite

```python
config = {
    "db_type": "sqlite",
    "db_path": ":memory:",  # Or a file path
}
```

## DuckDB

```python
config = {
    "db_type": "duckdb",
    "db_path": "my-database.duckdb",
}
```
