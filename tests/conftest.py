"""
tests/conftest.py — Pytest fixtures for unit, integration, and E2E validation tests.
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from common.serving_db import connect, init_schema
from streaming.common.session import get_spark


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Shared SparkSession across test suite."""
    s = get_spark(app_name="LNTA-PytestShared")
    yield s


@pytest.fixture
def tmp_stream_dir(tmp_path: Path) -> Path:
    """Temporary input stream directory for CSV files."""
    stream_dir = tmp_path / "stream_in"
    stream_dir.mkdir(parents=True, exist_ok=True)
    return stream_dir


@pytest.fixture
def tmp_checkpoint_dir(tmp_path: Path) -> Path:
    """Temporary checkpoint directory for streaming queries."""
    chk_dir = tmp_path / "checkpoints"
    chk_dir.mkdir(parents=True, exist_ok=True)
    return chk_dir


@pytest.fixture
def tmp_serving_db(tmp_path: Path) -> str:
    """Temporary SQLite serving database initialized with serving schema."""
    db_file = tmp_path / "serving.db"
    db_path = str(db_file)
    conn = connect(db_path)
    try:
        init_schema(conn)
    finally:
        conn.close()
    return db_path


@pytest.fixture
def group_a_config(
    tmp_stream_dir: Path,
    tmp_checkpoint_dir: Path,
    tmp_serving_db: str,
) -> dict[str, Any]:
    """Complete system configuration dictionary configured for Group A testing."""
    return {
        "spark": {
            "master": "local[2]",
            "app_name": "LNTA-GroupA-E2E",
            "trigger_s": 1,
            "watermark_s": 30,
            "checkpoint_root": str(tmp_checkpoint_dir),
            "stream_in_path": str(tmp_stream_dir),
            "max_files_per_trigger": 10,
            "max_rows_per_batch": 50000,
            "top_n_ports": 10,
            "enabled_analytics": [
                "decay",
                "sampling",
                "moments",
                "fm",
                "counting_ones",
            ],
            "sampling": {
                "k": 1000,
                "p": 0.1,
                "seed": 42,
            },
            "predicates": [
                {"name": "tcp_traffic", "expr": "protocol = 'TCP'"},
                {"name": "large_packets", "expr": "packet_length > 1000"},
            ],
            "filters": [
                {"name": "tcp_only", "protocol": "TCP"},
                {"name": "udp_only", "protocol": "UDP"},
                {"name": "web_traffic", "dst_ports": [80, 443]},
                {"name": "dns_traffic", "dst_ports": 53},
            ],
        },
        "serving": {
            "db_path": tmp_serving_db,
        },
    }
