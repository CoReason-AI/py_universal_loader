# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

"""
A unified, high-performance Python package for bulk-loading pandas DataFrames into diverse SQL databases and data warehouses.It provides a single, configuration-driven API that abstracts the most efficient native bulk-loading methods (e.g., `COPY`, `BULK INSERT`) for each backend.
"""

__version__ = "0.1.0"
__author__ = "Gowtham A Rao"
__email__ = "gowtham.rao@coreason.ai"

from .main import hello_world

__all__ = ["hello_world"]
