# Walkthrough — September 27, 2026 (Day 7)

## Team Progress

### M A Sushil Kumar
- **T4-010: Group A unit + streaming validation tests**
  - Created end-to-end streaming validation suite for Lane A queries
  - Covered `window_metrics`, `counts`, `filter_counts`, and `distinct` under active Spark session
  - Achieved >85% test coverage for `streaming/queries/`

### Yashwant Vadhan M
- **T5-009: Group B unit and integration tests**
  - Added comprehensive test suite for Lane B micro-batch analytics plugins (`decay`, `itemsets`, `moments`, `sampling`, `dgim`)
  - Covered registry dispatch and SQL upsert execution

- **T5-010: Cross-validation with M A Sushil Kumar's results**
  - Created `tests/e2e/test_cross_validation.py` comparing exact SQL window counts against streaming sketch outputs
  - Verified error bounds between exact unique IP counts and HyperLogLog / FM estimates

- **T8-001: One-command demo script and environment verifier**
  - Created `scripts/run_demo.sh` to launch live/replay capture, streaming application, alerts worker, and Streamlit UI in one command
  - Created `scripts/stop_demo.sh` for graceful teardown
  - Updated `scripts/verify_env.py` to produce comprehensive environment audit report

## Key Decisions
- Set cross-validation tolerance threshold to 5% for HLL and 25% for Flajolet-Martin

## Files Changed
- `tests/e2e/test_cross_validation.py` — Cross validation suite
- `scripts/run_demo.sh` — Master launcher
- `scripts/stop_demo.sh` — Shutdown script
- `scripts/verify_env.py` — Environment validator
- `documentation/walkthrough_sep27.md` — Daily walkthrough
