"""
dashboard/data.py — Read-only SQLite access with caching for LNTA dashboard.

Provides typed query functions for all serving tables.
"""
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Any

import pandas as pd

# Configuration
SERVING_DB_PATH = os.environ.get("LNTA_SERVING_DB", "serving/analytics.db")
CACHE_TTL_SECONDS = 2


class ServingDB:
    """Read-only SQLite connection with WAL mode and short TTL caching."""

    def __init__(self, db_path: str = SERVING_DB_PATH, ttl: int = CACHE_TTL_SECONDS):
        self.db_path = db_path
        self.ttl = ttl
        self._cache: dict[str, tuple[float, Any]] = {}

    @contextmanager
    def connect(self):
        """Context manager for read-only connection with busy_timeout."""
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, query: str, params: tuple = ()) -> list[sqlite3.Row]:
        """Execute a query and return rows."""
        with self.connect() as conn:
            cursor = conn.execute(query, params)
            return cursor.fetchall()

    def execute_df(self, query: str, params: tuple = ()) -> pd.DataFrame:
        """Execute a query and return pandas DataFrame."""
        with self.connect() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def _cache_key(self, query: str, params: tuple) -> str:
        return f"{query}|{params}"

    def _get_cached(self, key: str) -> Any | None:
        if key in self._cache:
            timestamp, value = self._cache[key]
            if time.time() - timestamp < self.ttl:
                return value
            del self._cache[key]
        return None

    def _set_cache(self, key: str, value: Any) -> None:
        self._cache[key] = (time.time(), value)

    def query_cached(self, query: str, params: tuple = ()) -> list[sqlite3.Row]:
        """Execute query with TTL caching."""
        key = self._cache_key(query, params)
        cached = self._get_cached(key)
        if cached is not None:
            return cached
        result = self.execute(query, params)
        self._set_cache(key, result)
        return result

    def query_cached_df(self, query: str, params: tuple = ()) -> pd.DataFrame:
        """Execute query with TTL caching, return DataFrame."""
        key = self._cache_key(query, params)
        cached = self._get_cached(key)
        if cached is not None:
            return cached
        result = self.execute_df(query, params)
        self._set_cache(key, result)
        return result


class MockServingDB:
    """Mock database for testing and demo mode - provides canned data."""

    def __init__(self):
        pass

    def query_cached_df(self, query: str, params: tuple = ()) -> pd.DataFrame:
        """Return mock DataFrames based on query."""
        import pandas as pd
        
        # Generate mock data for different query types
        if "window_metrics" in query:
            return pd.DataFrame({
                "window_start": pd.date_range("2026-09-24", periods=10, freq="10s"),
                "window_len_s": [10] * 10,
                "packets": [100, 120, 110, 130, 125, 140, 135, 150, 145, 160],
                "bytes": [10000, 12000, 11000, 13000, 12500, 14000, 13500, 15000, 14500, 16000],
                "pps": [100, 120, 110, 130, 125, 140, 135, 150, 145, 160],
                "bps": [10000, 12000, 11000, 13000, 12500, 14000, 13500, 15000, 14500, 16000],
            })
        elif "protocol_counts" in query:
            return pd.DataFrame({
                "window_start": pd.date_range("2026-09-24", periods=10, freq="10s"),
                "protocol": ["TCP", "UDP", "ICMP"] * 3 + ["TCP"],
                "packets": [100, 30, 10] * 3 + [100],
                "bytes": [10000, 3000, 1000] * 3 + [10000],
            })
        elif "port_counts" in query:
            return pd.DataFrame({
                "port": [443, 53, 80, 22, 25],
                "total_packets": [80, 25, 15, 10, 5],
                "total_bytes": [8000, 2500, 1500, 1000, 500],
            })
        elif "distinct_counts" in query:
            return pd.DataFrame({
                "window_start": pd.date_range("2026-09-24", periods=10, freq="10s"),
                "window_len_s": [10] * 10,
                "src_ips_exact": [50, 52, 51, 53, 52, 54, 53, 55, 54, 56],
                "dst_ips_exact": [45, 47, 46, 48, 47, 49, 48, 50, 49, 51],
                "ports_exact": [20, 21, 20, 22, 21, 23, 22, 24, 23, 25],
                "src_ips_hll": [52, 54, 53, 55, 54, 56, 55, 57, 56, 58],
                "dst_ips_hll": [47, 49, 48, 50, 49, 51, 50, 52, 51, 53],
                "ports_hll": [21, 22, 21, 23, 22, 24, 23, 25, 24, 26],
                "src_ips_fm": [51, 53, 52, 54, 53, 55, 54, 56, 55, 57],
                "dst_ips_fm": [46, 48, 47, 49, 48, 50, 49, 51, 50, 52],
                "ports_fm": [20, 21, 20, 22, 21, 23, 22, 24, 23, 25],
            })
        elif "sampling_compare" in query:
            return pd.DataFrame({
                "ts": pd.date_range("2026-09-24", periods=10, freq="10s"),
                "method": ["reservoir"] * 10,
                "k": [1000] * 10,
                "sample_n": [1000] * 10,
                "sample_mean_len": [1000, 1020, 1010, 1030, 1025, 1040, 1035, 1050, 1045, 1060],
                "full_mean_len": [1000, 1020, 1010, 1030, 1025, 1040, 1035, 1050, 1045, 1060],
                "err_pct": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            })
        elif "counting_ones" in query:
            return pd.DataFrame({
                "ts": pd.date_range("2026-09-24", periods=10, freq="10s"),
                "predicate_name": ["tcp_only"] * 10,
                "window_n": [1000] * 10,
                "exact_ones": [500, 510, 505, 515, 510, 520, 515, 525, 520, 530],
                "dgim_estimate": [502, 512, 507, 517, 512, 522, 517, 527, 522, 532],
                "err_pct": [0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4],
            })
        elif "moments" in query:
            return pd.DataFrame({
                "window_start": pd.date_range("2026-09-24", periods=10, freq="10s"),
                "window_len_s": [10] * 10,
                "n": [1000] * 10,
                "mean_len": [1000, 1020, 1010, 1030, 1025, 1040, 1035, 1050, 1045, 1060],
                "var_len": [100, 105, 102, 108, 104, 110, 106, 112, 108, 114],
                "std_len": [10, 10.2, 10.1, 10.4, 10.2, 10.5, 10.3, 10.6, 10.4, 10.7],
                "iat_mean_ms": [10, 10.2, 10.1, 10.3, 10.2, 10.4, 10.3, 10.5, 10.4, 10.6],
                "iat_var_ms": [5, 5.2, 5.1, 5.3, 5.2, 5.4, 5.3, 5.5, 5.4, 5.6],
                "iat_std_ms": [2.2, 2.3, 2.2, 2.3, 2.3, 2.3, 2.3, 2.3, 2.3, 2.4],
                "f2_exact": [1000000] * 10,
                "f2_ams": [1002000, 1003000, 1002500, 1003500, 1003000, 1004000, 1003500, 1004500, 1004000, 1005000],
            })
        elif "decay_traffic" in query:
            return pd.DataFrame({
                "ts": pd.date_range("2026-09-24", periods=10, freq="10s"),
                "half_life_s": [60] * 10,
                "score": [0.5, 0.6, 0.55, 0.65, 0.6, 0.7, 0.65, 0.75, 0.7, 0.8],
                "raw_pps": [100, 120, 110, 130, 125, 140, 135, 150, 145, 160],
            })
        elif "decay_top_keys" in query:
            return pd.DataFrame({
                "ts": pd.date_range("2026-09-24", periods=10, freq="10s"),
                "key_type": ["src_ip"] * 10,
                "key": ["192.168.1.1"] * 10,
                "score": [0.8, 0.85, 0.82, 0.88, 0.84, 0.9, 0.86, 0.92, 0.88, 0.94],
                "rank": [1] * 10,
            })
        elif "frequent_itemsets" in query:
            return pd.DataFrame({
                "window_start": pd.date_range("2026-09-24", periods=10, freq="10s"),
                "window_len_s": [10] * 10,
                "algorithm": ["A-Priori"] * 10,
                "itemset": ["{TCP:443}", "{UDP:53}", "{TCP:443, UDP:53}", "{TCP:443}", "{UDP:53}", "{TCP:443, UDP:53}", "{TCP:443}", "{UDP:53}", "{TCP:443, UDP:53}", "{TCP:443}"],
                "size": [1, 1, 2, 1, 1, 2, 1, 1, 2, 1],
                "support_count": [80, 25, 15, 80, 25, 15, 80, 25, 15, 80],
                "support_ratio": [0.8, 0.25, 0.15, 0.8, 0.25, 0.15, 0.8, 0.25, 0.15, 0.8],
                "passes": [1, 1, 2, 1, 1, 2, 1, 1, 2, 1],
            })
        elif "ip_edges" in query:
            return pd.DataFrame({
                "window_start": pd.date_range("2026-09-24", periods=10, freq="10s"),
                "window_len_s": [10] * 10,
                "src_ip": ["192.168.1.1", "192.168.1.2", "10.0.0.1", "192.168.1.1", "192.168.1.2", "10.0.0.1", "192.168.1.1", "192.168.1.2", "10.0.0.1", "192.168.1.1"],
                "dst_ip": ["8.8.8.8", "1.1.1.1", "192.168.1.1", "8.8.8.8", "1.1.1.1", "192.168.1.1", "8.8.8.8", "1.1.1.1", "192.168.1.1", "8.8.8.8"],
                "packets": [100, 50, 30, 100, 50, 30, 100, 50, 30, 100],
                "bytes": [10000, 5000, 3000, 10000, 5000, 3000, 10000, 5000, 3000, 10000],
            })
        elif "source_stats" in query:
            return pd.DataFrame({
                "window_start": pd.date_range("2026-09-24", periods=10, freq="10s"),
                "window_len_s": [10] * 10,
                "src_ip": ["192.168.1.1"] * 10,
                "packets": [100, 120, 110, 130, 125, 140, 135, 150, 145, 160],
                "bytes": [10000, 12000, 11000, 13000, 12500, 14000, 13500, 15000, 14500, 16000],
                "unique_dst_ips": [5, 6, 5, 7, 6, 8, 7, 9, 8, 10],
                "unique_dst_ports": [10, 12, 11, 13, 12, 14, 13, 15, 14, 16],
            })
        elif "alerts" in query:
            return pd.DataFrame({
                "alert_id": ["a1", "a2", "a3"],
                "ts": pd.date_range("2026-09-24", periods=3, freq="10s"),
                "type": ["TRAFFIC_SPIKE", "UNUSUAL_PORT_ACTIVITY", "HIGH_FANOUT"],
                "severity": ["WARN", "WARN", "CRITICAL"],
                "src_ip": ["192.168.1.1", "192.168.1.2", "10.0.0.1"],
                "metric": ["pps", "distinct_dst_ports", "distinct_dst_ips"],
                "current_value": [400, 25, 50],
                "baseline_value": [100, 5, 10],
                "change_pct": [300, 400, 400],
                "threshold": [300, 20, 30],
                "reason": ["Traffic spike detected", "Port scan detected", "High fan-out detected"],
                "details_json": ["{}", "{}", "{}"],
            })
        elif "pipeline_health" in query:
            return pd.DataFrame({
                "ts": pd.date_range("2026-09-24", periods=20, freq="10s"),
                "component": ["capture"]*5 + ["flume"]*5 + ["spark"]*5 + ["serving"]*5,
                "metric": ["batch_duration_ms"]*20,
                "value": [50, 55, 48, 52, 51, 100, 95, 105, 98, 102, 200, 210, 195, 205, 200, 10, 12, 11, 13, 11],
            })
        elif "hist_results" in query:
            return pd.DataFrame({
                "query_name": ["protocol_totals", "top_src_ips"],
                "run_at": ["2026-09-24T10:00:00Z", "2026-09-24T10:00:00Z"],
                "columns_json": ['["protocol", "total_packets"]', '["src_ip", "packets"]'],
                "rows_json": ['[["TCP", 1000], ["UDP", 200]]', '[["192.168.1.1", 500]]'],
            })
        else:
            return pd.DataFrame()

    def query_cached(self, query: str, params: tuple = ()) -> list:
        return []


_db_instance: ServingDB | None = None


def get_db(db_path: str = SERVING_DB_PATH) -> ServingDB:
    """Get or create singleton ServingDB instance. Returns MockServingDB in mock mode."""
    global _db_instance
    
    # Check for mock mode
    mock_mode = os.environ.get("LNTA_MOCK", "false").lower() == "true"
    
    if mock_mode:
        return MockServingDB()
    
    if _db_instance is None or _db_instance.db_path != db_path:
        _db_instance = ServingDB(db_path)
    return _db_instance


# --- Query builders (reduce duplication) ---

def _build_query(
    table: str,
    columns: str,
    where: str = "",
    order_by: str = "",
    limit: int | None = None,
) -> str:
    """Build a parameterized SELECT query."""
    parts = [f"SELECT {columns} FROM {table}"]
    if where:
        parts.append(f"WHERE {where}")
    if order_by:
        parts.append(f"ORDER BY {order_by}")
    if limit:
        parts.append(f"LIMIT {limit}")
    return " ".join(parts)


# --- Convenience query functions for dashboard ---

def get_latest_window_metrics(
    db: ServingDB, window_len_s: int = 10, limit: int = 120
) -> pd.DataFrame:
    """Get recent window metrics for KPI strip and traffic chart."""
    query = _build_query(
        "window_metrics",
        "window_start, window_len_s, packets, bytes, pps, bps",
        where="window_len_s = ?",
        order_by="window_start DESC",
        limit=limit,
    )
    return db.query_cached_df(query, (window_len_s,))


def get_protocol_counts(
    db: ServingDB, window_len_s: int = 10, limit: int = 120
) -> pd.DataFrame:
    """Get protocol counts for donut chart."""
    query = _build_query(
        "protocol_counts",
        "window_start, protocol, packets, bytes",
        where="window_len_s = ?",
        order_by="window_start DESC",
        limit=limit,
    )
    return db.query_cached_df(query, (window_len_s,))


def get_port_counts(
    db: ServingDB, window_len_s: int = 10, limit: int = 20
) -> pd.DataFrame:
    """Get top destination ports for bar chart."""
    query = _build_query(
        "port_counts",
        "port, SUM(packets) as total_packets, SUM(bytes) as total_bytes",
        where="window_len_s = ?",
        order_by="total_packets DESC",
        limit=limit,
    )
    return db.query_cached_df(query, (window_len_s,))


def get_distinct_counts(
    db: ServingDB, window_len_s: int = 10, limit: int = 120
) -> pd.DataFrame:
    """Get distinct counts (exact + HLL + FM) for Stream Analytics tab."""
    query = _build_query(
        "distinct_counts",
        (
            "window_start, window_len_s, "
            "src_ips_exact, dst_ips_exact, ports_exact, "
            "src_ips_hll, dst_ips_hll, ports_hll, "
            "src_ips_fm, dst_ips_fm, ports_fm"
        ),
        where="window_len_s = ?",
        order_by="window_start DESC",
        limit=limit,
    )
    return db.query_cached_df(query, (window_len_s,))


def get_sampling_compare(db: ServingDB, limit: int = 120) -> pd.DataFrame:
    """Get sampling comparison for Stream Analytics tab."""
    query = _build_query(
        "sampling_compare",
        "ts, method, k, sample_n, sample_mean_len, full_mean_len, err_pct",
        order_by="ts DESC",
        limit=limit,
    )
    return db.query_cached_df(query, ())


def get_counting_ones(db: ServingDB, limit: int = 120) -> pd.DataFrame:
    """Get DGIM counting ones for Stream Analytics tab."""
    query = _build_query(
        "counting_ones",
        "ts, predicate_name, window_n, exact_ones, dgim_estimate, err_pct",
        order_by="ts DESC",
        limit=limit,
    )
    return db.query_cached_df(query, ())


def get_moments(
    db: ServingDB, window_len_s: int = 10, limit: int = 120
) -> pd.DataFrame:
    """Get moments (mean, var, std, AMS F2) for Stream Analytics tab."""
    query = _build_query(
        "moments",
        (
            "window_start, window_len_s, n, mean_len, var_len, std_len, "
            "iat_mean_ms, iat_var_ms, iat_std_ms, f2_exact, f2_ams"
        ),
        where="window_len_s = ?",
        order_by="window_start DESC",
        limit=limit,
    )
    return db.query_cached_df(query, (window_len_s,))


def get_decay_traffic(
    db: ServingDB, half_life_s: int = 60, limit: int = 120
) -> pd.DataFrame:
    """Get decaying window score for Stream Analytics tab."""
    query = _build_query(
        "decay_traffic",
        "ts, half_life_s, score, raw_pps",
        where="half_life_s = ?",
        order_by="ts DESC",
        limit=limit,
    )
    return db.query_cached_df(query, (half_life_s,))


def get_decay_top_keys(
    db: ServingDB, half_life_s: int = 60, key_type: str = "src_ip", limit: int = 20
) -> pd.DataFrame:
    """Get top decayed keys for Stream Analytics tab."""
    query = _build_query(
        "decay_top_keys",
        "ts, key_type, key, score, rank",
        where="half_life_s = ? AND key_type = ?",
        order_by="ts DESC, rank ASC",
        limit=limit,
    )
    return db.query_cached_df(query, (half_life_s, key_type))


def get_frequent_itemsets(
    db: ServingDB, window_len_s: int = 10, algorithm: str = "A-Priori", limit: int = 50
) -> pd.DataFrame:
    """Get frequent itemsets for Stream Analytics tab."""
    query = _build_query(
        "frequent_itemsets",
        "window_start, window_len_s, algorithm, itemset, size, support_count, support_ratio, passes",
        where="window_len_s = ? AND algorithm = ?",
        order_by="window_start DESC, support_ratio DESC",
        limit=limit,
    )
    return db.query_cached_df(query, (window_len_s, algorithm))


def get_ip_edges(
    db: ServingDB, window_len_s: int = 10, limit: int = 1000
) -> pd.DataFrame:
    """Get IP edges for Link Analysis graph."""
    query = _build_query(
        "ip_edges",
        "window_start, window_len_s, src_ip, dst_ip, packets, bytes",
        where="window_len_s = ?",
        order_by="window_start DESC, packets DESC",
        limit=limit,
    )
    return db.query_cached_df(query, (window_len_s,))


def get_source_stats(
    db: ServingDB, window_len_s: int = 10, limit: int = 100
) -> pd.DataFrame:
    """Get source stats for Link Analysis."""
    query = _build_query(
        "source_stats",
        "window_start, window_len_s, src_ip, packets, bytes, unique_dst_ips, unique_dst_ports",
        where="window_len_s = ?",
        order_by="window_start DESC, packets DESC",
        limit=limit,
    )
    return db.query_cached_df(query, (window_len_s,))


def get_active_alerts(db: ServingDB, limit: int = 50) -> pd.DataFrame:
    """Get recent alerts for Alerts tab."""
    query = _build_query(
        "alerts",
        (
            "alert_id, ts, type, severity, src_ip, metric, "
            "current_value, baseline_value, change_pct, threshold, reason, details_json"
        ),
        order_by="ts DESC",
        limit=limit,
    )
    return db.query_cached_df(query, ())


def get_pipeline_health(db: ServingDB, limit: int = 100) -> pd.DataFrame:
    """Get pipeline health for Pipeline tab."""
    query = _build_query(
        "pipeline_health",
        "ts, component, metric, value",
        order_by="ts DESC",
        limit=limit,
    )
    return db.query_cached_df(query, ())


def get_hist_results(
    db: ServingDB, query_name: str | None = None, limit: int = 100
) -> pd.DataFrame:
    """Get historical query results for History tab."""
    if query_name:
        query = _build_query(
            "hist_results",
            "query_name, run_at, columns_json, rows_json",
            where="query_name = ?",
            order_by="run_at DESC",
            limit=limit,
        )
        return db.query_cached_df(query, (query_name,))
    else:
        query = _build_query(
            "hist_results",
            "query_name, run_at, columns_json, rows_json",
            order_by="run_at DESC",
            limit=limit,
        )
        return db.query_cached_df(query, ())


def check_db_health(db: ServingDB) -> dict[str, Any]:
    """Check database connectivity and table status."""
    if isinstance(db, MockServingDB):
        return {
            "connected": True,
            "tables": ["window_metrics", "protocol_counts", "ip_edges", "alerts", "pipeline_health"],
            "table_status": {
                "window_metrics": "2026-09-24T12:00:00Z",
                "protocol_counts": "2026-09-24T12:00:00Z",
                "ip_edges": "2026-09-24T12:00:00Z",
                "alerts": "2026-09-24T12:00:00Z",
                "pipeline_health": "2026-09-24T12:00:00Z",
            },
            "db_path": "mock",
        }
    
    try:
        with db.connect() as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]

            table_status = {}
            for table in [
                "window_metrics",
                "protocol_counts",
                "ip_edges",
                "alerts",
                "pipeline_health",
            ]:
                if table in tables:
                    cursor = conn.execute(f"SELECT MAX(window_start) FROM {table}")
                    latest = cursor.fetchone()[0]
                    table_status[table] = latest
                else:
                    table_status[table] = None

            return {
                "connected": True,
                "tables": tables,
                "table_status": table_status,
                "db_path": db.db_path,
            }
    except Exception as e:
        return {
            "connected": False,
            "error": str(e),
            "db_path": db.db_path,
        }
