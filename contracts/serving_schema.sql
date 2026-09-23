-- contracts/serving_schema.sql — SQLite DDL for LNTA Serving Store
-- Reference: TECH_RULES.md §5.2
-- All timestamps are ISO-8601 UTC text (e.g. '2026-09-21T12:00:00.000Z').
-- Every table includes updated_at TEXT DEFAULT CURRENT_TIMESTAMP.

-- 1. window_metrics
CREATE TABLE IF NOT EXISTS window_metrics (
    window_start TEXT NOT NULL,
    window_len_s INTEGER NOT NULL,
    packets INTEGER,
    bytes INTEGER,
    pps REAL,
    bps REAL,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_len_s)
);
CREATE INDEX IF NOT EXISTS idx_window_metrics_ws ON window_metrics(window_start);

-- 2. protocol_counts
CREATE TABLE IF NOT EXISTS protocol_counts (
    window_start TEXT NOT NULL,
    window_len_s INTEGER NOT NULL,
    protocol TEXT NOT NULL,
    packets INTEGER,
    bytes INTEGER,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_len_s, protocol)
);
CREATE INDEX IF NOT EXISTS idx_protocol_counts_ws ON protocol_counts(window_start);

-- 3. port_counts
CREATE TABLE IF NOT EXISTS port_counts (
    window_start TEXT NOT NULL,
    window_len_s INTEGER NOT NULL,
    port INTEGER NOT NULL,
    packets INTEGER,
    bytes INTEGER,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_len_s, port)
);
CREATE INDEX IF NOT EXISTS idx_port_counts_ws ON port_counts(window_start);

-- 4. filter_counts
CREATE TABLE IF NOT EXISTS filter_counts (
    window_start TEXT NOT NULL,
    window_len_s INTEGER NOT NULL,
    filter_name TEXT NOT NULL,
    packets INTEGER,
    bytes INTEGER,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_len_s, filter_name)
);
CREATE INDEX IF NOT EXISTS idx_filter_counts_ws ON filter_counts(window_start);

-- 5. distinct_counts
CREATE TABLE IF NOT EXISTS distinct_counts (
    window_start TEXT NOT NULL,
    window_len_s INTEGER NOT NULL,
    src_ips_exact INTEGER,
    dst_ips_exact INTEGER,
    ports_exact INTEGER,
    src_ips_hll INTEGER,
    dst_ips_hll INTEGER,
    ports_hll INTEGER,
    src_ips_fm INTEGER,
    dst_ips_fm INTEGER,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_len_s)
);
CREATE INDEX IF NOT EXISTS idx_distinct_counts_ws ON distinct_counts(window_start);

-- 6. sampling_compare
CREATE TABLE IF NOT EXISTS sampling_compare (
    ts TEXT NOT NULL,
    method TEXT NOT NULL,
    k INTEGER,
    sample_n INTEGER,
    sample_mean_len REAL,
    full_mean_len REAL,
    err_pct REAL,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts, method)
);
CREATE INDEX IF NOT EXISTS idx_sampling_compare_ts ON sampling_compare(ts);

-- 7. counting_ones
CREATE TABLE IF NOT EXISTS counting_ones (
    ts TEXT NOT NULL,
    predicate_name TEXT NOT NULL,
    window_n INTEGER,
    exact_ones INTEGER,
    dgim_estimate INTEGER,
    err_pct REAL,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts, predicate_name)
);
CREATE INDEX IF NOT EXISTS idx_counting_ones_ts ON counting_ones(ts);

-- 8. moments
CREATE TABLE IF NOT EXISTS moments (
    window_start TEXT NOT NULL,
    window_len_s INTEGER NOT NULL,
    n INTEGER,
    mean_len REAL,
    var_len REAL,
    std_len REAL,
    iat_mean_ms REAL,
    iat_var_ms REAL,
    iat_std_ms REAL,
    f2_exact REAL,
    f2_ams REAL,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_len_s)
);
CREATE INDEX IF NOT EXISTS idx_moments_ws ON moments(window_start);

-- 9. decay_traffic
CREATE TABLE IF NOT EXISTS decay_traffic (
    ts TEXT NOT NULL,
    half_life_s INTEGER NOT NULL,
    score REAL,
    raw_pps REAL,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts, half_life_s)
);
CREATE INDEX IF NOT EXISTS idx_decay_traffic_ts ON decay_traffic(ts);

-- 10. decay_top_keys
CREATE TABLE IF NOT EXISTS decay_top_keys (
    ts TEXT NOT NULL,
    key_type TEXT NOT NULL,
    key TEXT NOT NULL,
    score REAL,
    rank INTEGER,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts, key_type, key)
);
CREATE INDEX IF NOT EXISTS idx_decay_top_keys_ts ON decay_top_keys(ts);

-- 11. frequent_itemsets
CREATE TABLE IF NOT EXISTS frequent_itemsets (
    window_start TEXT NOT NULL,
    window_len_s INTEGER NOT NULL,
    algorithm TEXT NOT NULL,
    itemset TEXT NOT NULL,
    size INTEGER,
    support_count INTEGER,
    support_ratio REAL,
    passes INTEGER,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_len_s, algorithm, itemset)
);
CREATE INDEX IF NOT EXISTS idx_frequent_itemsets_ws ON frequent_itemsets(window_start);

-- 12. ip_edges
CREATE TABLE IF NOT EXISTS ip_edges (
    window_start TEXT NOT NULL,
    window_len_s INTEGER NOT NULL,
    src_ip TEXT NOT NULL,
    dst_ip TEXT NOT NULL,
    packets INTEGER,
    bytes INTEGER,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_len_s, src_ip, dst_ip)
);
CREATE INDEX IF NOT EXISTS idx_ip_edges_ws ON ip_edges(window_start);

-- 13. source_stats
CREATE TABLE IF NOT EXISTS source_stats (
    window_start TEXT NOT NULL,
    window_len_s INTEGER NOT NULL,
    src_ip TEXT NOT NULL,
    packets INTEGER,
    bytes INTEGER,
    unique_dst_ips INTEGER,
    unique_dst_ports INTEGER,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_len_s, src_ip)
);
CREATE INDEX IF NOT EXISTS idx_source_stats_ws ON source_stats(window_start);

-- 14. alerts
CREATE TABLE IF NOT EXISTS alerts (
    alert_id TEXT PRIMARY KEY,
    ts TEXT NOT NULL,
    type TEXT NOT NULL,
    severity TEXT NOT NULL,
    src_ip TEXT,
    metric TEXT,
    current_value REAL,
    baseline_value REAL,
    change_pct REAL,
    threshold REAL,
    reason TEXT,
    details_json TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts);

-- 15. pipeline_health
CREATE TABLE IF NOT EXISTS pipeline_health (
    ts TEXT NOT NULL,
    component TEXT NOT NULL,
    metric TEXT NOT NULL,
    value REAL,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts, component, metric)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_health_ts ON pipeline_health(ts);

-- 16. hist_results
CREATE TABLE IF NOT EXISTS hist_results (
    query_name TEXT NOT NULL,
    run_at TEXT NOT NULL,
    columns_json TEXT,
    rows_json TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (query_name, run_at)
);
CREATE INDEX IF NOT EXISTS idx_hist_results_run ON hist_results(run_at);

