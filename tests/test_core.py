# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/py_universal_loader

import pytest

from py_universal_loader.main import get_loader


def test_get_loader_unsupported_db_type():
    """
    Test that get_loader raises a ValueError for an unsupported db_type.
    """
    with pytest.raises(ValueError, match="Unsupported database type: unsupported_db"):
        get_loader({"db_type": "unsupported_db"})


def test_get_loader_no_db_type():
    """
    Test that get_loader raises a ValueError if 'db_type' is not in the config.
    """
    with pytest.raises(
        ValueError, match="Configuration dictionary must contain a 'db_type' key."
    ):
        get_loader({})
