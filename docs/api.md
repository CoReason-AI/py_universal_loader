# API Reference

## `get_loader(config: dict)`

A factory function that returns a database-specific loader instance.

- **Parameters:**
  - `config` (dict): A dictionary containing the database type and connection parameters.
- **Returns:**
  - An instance of a `BaseLoader` subclass.

## `loader.connect()`

Establishes a connection to the database.

## `loader.close()`

Closes the database connection.

## `loader.load_dataframe(df: pd.DataFrame, table_name: str)`

Loads a pandas DataFrame into the specified table.

- **Parameters:**
  - `df` (pd.DataFrame): The DataFrame to load.
  - `table_name` (str): The name of the target table.
