"""
dashboard/data.py — Read-only SQLite access with caching for LNTA dashboard.

Provides typed query functions for all serving tables.
"""
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# Configuration
SERVING_DB_PATH = os.environ.get("LNTA_SERVING_DB", "serving/analytics.db")
CACHE_TTL_SECONDS = 2


class ServingDB:
    """Read-only SQLite connection with WAL mode and short TTL caching."""

    def __init__(self, db_path: str = SERVING_DB_PATH, ttl: int = CACHE_TTL_SECONDS):
        self.db_path = db_path
        self.ttl = ttl
        self._cache: Dict[str, Tuple[float, Any]] = {}

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

    def execute(self, query: str, params: Tuple = ()) -> List[sqlite3.Row]:
        """Execute a query and return rows."""
        with self.connect() as conn:
            cursor = conn.execute(query, params)
            return cursor.fetchall()

    def execute_df(self, query: str, params: Tuple = ()) -> pd.DataFrame:
        """Execute a query and return pandas DataFrame."""
        with self.connect() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def _cache_key(self, query: str, params: Tuple) -> str:
        return f"{query}|{params}"

    def _get_cached(self, key: str) -> Optional[Any]:
        if key in self._cache:
            timestamp, value = self._cache[key]
            if time.time() - timestamp < self.ttl:
                return value
            del self._cache[key]
        return None

    def _set_cache(self, key: str, value: Any) -> None:
        self._cache[key] = (time.time(), value)

    def query_cached(self, query: str, params: Tuple = ()) -> List[sqlite3.Row]:
        """Execute query with TTL caching."""
        key = self._cache_key(query, params)
        cached = self._get_cached(key)
        if cached is not None:
            return cached
        result = self.execute(query, params)
        self._set_cache(key, result)
        return result

    def query_cached_df(self, query: str, params: Tuple = ()) -> pd.DataFrame:
        """Execute query with TTL caching, return DataFrame."""
        key = self._cache_key(query, params)
        cached = self._get_cached(key)
        if cached is not None:
            return cached
        result = self.execute_df(query, params)
        self._set_cache(key, result)
        return result


_db_instance: Optional[ServingDB] = None


def get_db(db_path: str = SERVING_DB_PATH) -> ServingDB:
    """Get or create singleton ServingDB instance."""
    global _db_instance
    if _db_instance is None or _db_instance.db_path != db_path:
        _db_instance = ServingDB(db_path)
    return _db_instance


# --- Query builders (reduce duplication) ---

def _build_query(
    table: str,
    columns: str,
    where: str = "",
    order_by: str = "",
    limit: Optional[int] = None,
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
    db: ServingDB, query_name: Optional[str] = None, limit: int = 100
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


def check_db_health(db: ServingDB) -> Dict[str, Any]:
    """Check database connectivity and table status."""
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
