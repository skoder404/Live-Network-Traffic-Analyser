"""
tests/unit/test_sampling.py — Unit tests for reservoir and Bernoulli sampling and SamplingAnalytic.

Verifies:
  1. Uniformity of ReservoirSampler (Algorithm R) via statistical distribution test.
  2. Convergence of sample mean to population mean as reservoir capacity k increases.
  3. Determinism, snapshot, and restore behavior.
  4. Bernoulli sampling fidelity.
  5. SamplingAnalytic (Lane B plugin) micro-batch execution and SQLite comparison persistence.
Reference: TECH_RULES §3.5, todo.md T4-007
"""

from __future__ import annotations

from collections import Counter

import pytest

from common.serving_db import connect, init_schema
from contracts.record_schema import to_spark_schema
from streaming.analytics.base import BatchContext
from streaming.analytics.sampling import (
    ReservoirSampler,
    SamplingAnalytic,
    bernoulli_sample,
)
from streaming.common.cleaning import clean
from streaming.common.session import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("LNTA-TestSampling")
    yield s


def test_reservoir_sampler_uniformity_statistical():
    """
    Statistical uniformity test for ReservoirSampler:
    Over multiple trials of N stream items into reservoir size k,
    every item should appear with probability ~ k / N.
    """
    n_items = 50
    k = 10
    n_trials = 3000
    expected_count = n_trials * (k / n_items)  # 600

    counts = Counter()
    for trial in range(n_trials):
        sampler = ReservoirSampler(k=k, seed=1000 + trial)
        for item in range(n_items):
            sampler.add(item)
        sample = sampler.sample()
        assert len(sample) == k
        counts.update(sample)

    # Check that each item's observed frequency is within tolerance of expected
    for item in range(n_items):
        obs = counts[item]
        # 3 standard deviations for binomial(n=3000, p=0.2) is ~ 3 * sqrt(3000 * 0.2 * 0.8) = ~65
        assert abs(obs - expected_count) < 90, (
            f"Item {item} count {obs} deviated significantly from expected {expected_count}"
        )


def test_reservoir_sampler_convergence():
    """Verifies that sample mean converges to population mean as k increases."""
    # Population 1..1000
    population = list(range(1, 1001))
    pop_mean = sum(population) / len(population)  # 500.5

    # Run small k vs large k across trials
    errs_small = []
    errs_large = []
    for trial in range(50):
        s_small = ReservoirSampler(k=20, seed=trial)
        s_large = ReservoirSampler(k=300, seed=trial)

        for x in population:
            s_small.add(x)
            s_large.add(x)

        mean_small = sum(s_small.sample()) / len(s_small.sample())
        mean_large = sum(s_large.sample()) / len(s_large.sample())

        errs_small.append(abs(mean_small - pop_mean))
        errs_large.append(abs(mean_large - pop_mean))

    avg_err_small = sum(errs_small) / len(errs_small)
    avg_err_large = sum(errs_large) / len(errs_large)

    # Large reservoir should have substantially smaller mean absolute error
    assert avg_err_large < avg_err_small / 2


def test_reservoir_sampler_snapshot_restore():
    """Verifies snapshot and state restore fidelity."""
    s1 = ReservoirSampler(k=5, seed=42)
    for i in range(20):
        s1.add(i)

    snap = s1.snapshot()
    assert snap["k"] == 5
    assert snap["n_seen"] == 20
    assert len(snap["sample"]) == 5

    s2 = ReservoirSampler(k=10)
    s2.restore(snap)
    assert s2.k == 5
    assert s2.n_seen == 20
    assert s2.sample() == s1.sample()


def test_bernoulli_sample():
    """Verifies Bernoulli sampling size and value selection."""
    data = list(range(10000))
    p = 0.25
    sample = bernoulli_sample(data, p=p, seed=123)

    # Expected ~ 2500 items
    assert 2250 <= len(sample) <= 2750
    # Every sampled item must be from original dataset
    assert set(sample).issubset(set(data))

    # Invalid probability
    with pytest.raises(ValueError):
        bernoulli_sample(data, p=-0.1)
    with pytest.raises(ValueError):
        bernoulli_sample(data, p=1.5)


def test_sampling_analytic_process_batch(spark, tmp_path):
    """Tests SamplingAnalytic Lane B plugin against SQLite database."""
    db_path = tmp_path / "test_sampling.db"
    conn = connect(db_path)
    init_schema(conn)
    conn.close()

    # Generate test batch of 100 packets with packet lengths 100 to 1090
    records = []
    for i in range(100):
        records.append(
            (
                "2026-09-24 12:00:01.000",
                "192.168.1.10",
                "8.8.8.8",
                1000 + i,
                443,
                "TCP",
                100 + i * 10,
                None,
                None,
                None,
                1.0,
            )
        )
    raw_df = spark.createDataFrame(records, schema=to_spark_schema())
    cleaned_df = clean(raw_df)

    cfg = {
        "spark": {
            "max_rows_per_batch": 5000,
            "sampling": {"k": 50},
        },
        "serving": {"db_path": str(db_path)},
    }
    ctx = BatchContext(cfg=cfg, db_path=db_path, batch_time="2026-09-24T12:00:00.000Z")

    plugin = SamplingAnalytic(k=50, p=0.2, seed=42)
    plugin.process_batch(cleaned_df, batch_id=1, ctx=ctx)

    # Verify rows in sampling_compare
    conn = connect(db_path)
    cur = conn.execute(
        "SELECT method, k, sample_n, sample_mean_len, full_mean_len, err_pct FROM sampling_compare ORDER BY method;"
    )
    rows = cur.fetchall()
    conn.close()

    assert len(rows) == 2
    # Row 0: bernoulli
    assert rows[0][0] == "bernoulli"
    assert rows[0][2] > 0  # sample_n
    assert abs(rows[0][4] - 595.0) < 1e-3  # full mean length for 100 + i*10 (i in 0..99) is 595.0

    # Row 1: reservoir
    assert rows[1][0] == "reservoir"
    assert rows[1][1] == 50  # k
    assert rows[1][2] == 50  # sample_n (capped at k=50 since population=100)
    assert abs(rows[1][4] - 595.0) < 1e-3  # full mean length is 595.0
    assert rows[1][5] >= 0.0  # err_pct


def test_reservoir_sampler_invalid_k_and_reset():
    """Tests that non-positive k raises ValueError and reset clears state."""
    with pytest.raises(ValueError, match="Reservoir size k must be positive"):
        ReservoirSampler(k=0)
    with pytest.raises(ValueError, match="Reservoir size k must be positive"):
        ReservoirSampler(k=-5)

    sampler = ReservoirSampler(k=10, seed=123)
    for i in range(20):
        sampler.add(i)
    assert len(sampler.sample()) == 10
    assert sampler.n_seen == 20

    sampler.reset()
    assert len(sampler.sample()) == 0
    assert sampler.n_seen == 0


def test_sampling_analytic_edge_cases_and_capping(spark, tmp_path):
    """Tests SamplingAnalytic empty batches, row capping, dynamic k, and ctx.conn."""
    db_path = str(tmp_path / "test_sampling_edges.db")
    conn = connect(db_path)
    init_schema(conn)

    # 1. Empty batch
    empty_df = spark.createDataFrame([], schema=to_spark_schema())
    ctx = BatchContext(
        cfg={"spark": {"max_rows_per_batch": 5}},
        db_path=db_path,
        conn=conn,
        batch_time="2026-09-24T12:00:00.000Z",
    )
    plugin = SamplingAnalytic(k=10)
    plugin.process_batch(empty_df, batch_id=0, ctx=ctx)

    # 2. Batch with dynamic k and row capping (10 rows > max_rows=5)
    records = []
    for i in range(10):
        records.append(
            (
                "2026-09-24 12:00:00.000",
                "192.168.1.1",
                "10.0.0.1",
                1000 + i,
                80,
                "TCP",
                100 + i * 10,
                None,
                None,
                None,
                1.0,
            )
        )
    df = clean(spark.createDataFrame(records, schema=to_spark_schema()))

    cfg_dynamic = {
        "spark": {
            "max_rows_per_batch": 5,
            "sampling": {"k": 3},  # change k from 10 to 3
        },
        "serving": {"db_path": db_path},
    }
    ctx_dynamic = BatchContext(
        cfg=cfg_dynamic,
        db_path=db_path,
        conn=conn,
        batch_time="2026-09-24T12:00:01.000Z",
    )
    plugin.process_batch(df, batch_id=1, ctx=ctx_dynamic)
    assert plugin.reservoir.k == 3

    # Check sampling_compare rows
    rows = conn.execute(
        "SELECT method, k, sample_n FROM sampling_compare WHERE ts = '2026-09-24T12:00:01.000Z'"
    ).fetchall()
    conn.close()
    assert len(rows) == 2
