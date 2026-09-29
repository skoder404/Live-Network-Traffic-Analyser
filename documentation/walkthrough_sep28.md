# Walkthrough — September 28, 2026 (Day 8)

## Team Progress

### Yashwant Vadhan M
- **T5-006: Multi-window operations (10s / 30s / 60s)**
  - Extended `streaming/stream_app.py` to concurrently instantiate windowed SQL queries for multiple sliding window granularities (10s, 30s, 60s)
  - Implemented query capping mechanism to prevent Spark execution thread starvation
  - Verified with `tests/e2e/test_multi_window.py`

- **T5-007: Load and high-volume test**
  - Created `scripts/load_test.py` benchmark harness simulating up to 5,000 packets/sec
  - Generated benchmark throughput and latency performance report in `docs/load_test_report.md`

### Naveena MS
- **T8-003: Wireshark / ingestion validation report**
  - Completed comparison between TShark packet capture logs and PySpark ingested metrics
  - Documented findings in `docs/validation_report.md` confirming zero packet loss under standard loads

## Key Decisions
- Set maximum concurrent Spark streaming queries limit to 24
- Standardized multi-window slide durations (10s window / 5s slide; 30s window / 10s slide; 60s window / 15s slide)

## Files Changed
- `streaming/stream_app.py` — Multi-window query execution
- `streaming/queries/window_metrics.py` — Dynamic slide duration support
- `scripts/load_test.py` — High volume load testing tool
- `docs/load_test_report.md` — Load test performance report
- `documentation/walkthrough_sep28.md` — Daily walkthrough
