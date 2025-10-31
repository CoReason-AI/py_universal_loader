# Introduction

`py-universal-loader` is a powerful Python package designed to simplify and accelerate the process of loading `pandas.DataFrame` objects into a wide variety of SQL databases and data warehouses. It provides a single, configuration-driven API that abstracts the most efficient native bulk-loading methods (e.g., `COPY`, `BULK INSERT`) for each backend, ensuring optimal performance without requiring database-specific code.

## Core Concepts

The package is built around two key components:

- **`BaseLoader`:** An abstract base class that defines the common interface for all database-specific loaders.
- **`get_loader`:** A factory function that takes a configuration dictionary and returns an instance of the appropriate loader.

This design allows you to switch between different databases by simply changing the configuration, without altering your application code.
