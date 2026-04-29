"""Pytest helpers for the local src-layout package."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
TMP_ROOT = Path(__file__).resolve().parents[1] / ".pytest_tmp"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@pytest.fixture
def tmp_path() -> Path:
    """Use a repo-local temp tree instead of blocked Windows temp locations."""
    TMP_ROOT.mkdir(exist_ok=True)
    return Path(tempfile.mkdtemp(prefix="pytest-", dir=TMP_ROOT))
