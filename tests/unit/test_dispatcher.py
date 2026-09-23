"""
tests/unit/test_dispatcher.py — Unit tests for plugin registry and Lane B foreachBatch dispatcher.
"""

from pathlib import Path

import pytest
from pyspark.sql import DataFrame

from common.serving_db import connect, init_schema
from contracts.record_schema import to_spark_schema
from streaming.analytics.base import Analytic, BatchContext, NoopAnalytic
from streaming.common.cleaning import clean
from streaming.common.session import get_spark
from streaming.registry import clear_registry, get_enabled, register
from streaming.stream_app import StreamingApplication


class FailingAnalytic(Analytic):
    name = "failing_test_plugin"

    def process_batch(self, batch_df: DataFrame, batch_id: int, ctx: BatchContext) -> None:
        raise ValueError("Simulated plugin failure")


@pytest.fixture(scope="module")
def spark():
    s = get_spark("LNTA-TestDispatcher")
    yield s


@pytest.fixture(autouse=True)
def clean_reg():
    clear_registry()
    yield
    clear_registry()


def test_registry_registration_and_enabled():
    noop = NoopAnalytic()
    register(noop)
    assert get_enabled({}) == [noop]

    cfg_specific = {"spark": {"plugins_enabled": ["noop"]}}
    assert get_enabled(cfg_specific) == [noop]

    cfg_disabled = {"spark": {"plugins_enabled": []}}
    assert get_enabled(cfg_disabled) == []


def test_dispatcher_plugin_isolation_and_health_record(spark, tmp_path: Path):
    db_path = tmp_path / "test_analytics.db"
    conn = connect(db_path)
    init_schema(conn)
    conn.close()

    noop = NoopAnalytic()
    failing = FailingAnalytic()
    register(noop)
    register(failing)

    cfg = {
        "spark": {"trigger_s": 5, "plugins_enabled": ["noop", "failing_test_plugin"]},
        "serving": {"db_path": str(db_path)},
    }

    app = StreamingApplication(cfg)
    dispatcher = app.create_dispatcher()

    sample_data = [
        (
            "2026-09-22 12:00:00.100",
            "192.168.1.10",
            "8.8.8.8",
            54321,
            443,
            "TCP",
            1000,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-22 12:00:00.200",
            "192.168.1.10",
            "8.8.8.8",
            54321,
            443,
            "TCP",
            2000,
            None,
            None,
            None,
            1.0,
        ),
    ]
    raw_df = spark.createDataFrame(sample_data, schema=to_spark_schema())
    cleaned_df = clean(raw_df)

    # Calling dispatcher should not raise even if FailingAnalytic fails
    dispatcher(cleaned_df, batch_id=0)

    # Assert noop plugin ran successfully
    assert 0 in noop.processed_batches

    # Verify pipeline_health records in SQLite
    conn = connect(db_path, read_only=True)
    rows = conn.execute("SELECT metric, value FROM pipeline_health").fetchall()
    conn.close()

    metrics_map = {r["metric"]: r["value"] for r in rows}
    assert "input_rows" in metrics_map
    assert metrics_map["input_rows"] == 2.0
    assert "plugin_error" in metrics_map
    assert metrics_map["plugin_error"] == 1.0
    assert "batch_duration_s" in metrics_map
