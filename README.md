# py-universal-loader

A unified, high-performance Python package for bulk-loading pandas DataFrames into diverse SQL databases and data warehouses.It provides a single, configuration-driven API that abstracts the most efficient native bulk-loading methods (e.g., `COPY`, `BULK INSERT`) for each backend.

[![CI](https://github.com/CoReason-AI/py_universal_loader/actions/workflows/ci.yml/badge.svg)](https://github.com/CoReason-AI/py_universal_loader/actions/workflows/ci.yml)

## Getting Started

### Prerequisites

- Python 3.10+
- Poetry

### Installation

1.  Clone the repository:
    ```sh
    git clone https://github.com/example/example.git
    cd my_python_project
    ```
2.  Install dependencies:
    ```sh
    poetry install
    ```

### Usage

-   Run the linter:
    ```sh
    poetry run pre-commit run --all-files
    ```
-   Run the tests:
    ```sh
    poetry run pytest
    ```
