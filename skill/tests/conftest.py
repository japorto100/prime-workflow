import pytest
from pathlib import Path

@pytest.fixture
def tmp(tmp_path: Path) -> Path:
    """Map tmp fixture to pytest's standard tmp_path fixture."""
    return tmp_path
