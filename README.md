# py-universal-loader

A unified, high-performance Python package for bulk-loading pandas DataFrames into diverse SQL databases and data warehouses.It provides a single, configuration-driven API that abstracts the most efficient native bulk-loading methods (e.g., `COPY`, `BULK INSERT`) for each backend.

[![CI](https://github.com/CoReason-AI/py_universal_loader/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/CoReason-AI/py_universal_loader/actions/workflows/ci-cd.yml)
[![PyPI version](https://badge.fury.io/py/py-universal-loader.svg)](https://badge.fury.io/py/py-universal-loader)

## Installation

You can install `py-universal-loader` from PyPI:

```sh
pip install py-universal-loader
```

## Usage

Here's a simple example of how to use `py-universal-loader` to load a pandas DataFrame into a SQLite database:

```python
import pandas as pd
from py_universal_loader import get_loader

# Sample DataFrame
data = {'col1': [1, 2], 'col2': [3, 4]}
df = pd.DataFrame(data)

# Configuration for SQLite
config = {
    "db_type": "sqlite",
    "db_path": ":memory:",
}

# Get the loader and load the data
loader = get_loader(config)
loader.connect()
loader.load_dataframe(df, "my_table")
loader.close()
```
