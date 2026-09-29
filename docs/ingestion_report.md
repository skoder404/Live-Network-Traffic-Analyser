# Ingestion Fault-Test Report

These tests use replay data and the verifier in `ingestion/verification.py`.
The two infrastructure scenarios are opt-in because they stop shared services;
run them on the dedicated integration host with `LNTA_FAULT_LIVE=1`.

## Procedures

| Scenario | Command | Fault injection | Verification |
|---|---|---|---|
| Flume restart | `scripts/fault_tests/ingestion_faults.sh kill_flume` | Kill Flume during a replay, restart it, and compare the verifier report with the producer sequence. | `python3 -m ingestion.scripts.ingestion_ctl verify --local --root data/traffic` or the HDFS output directory. |
| NameNode outage | `scripts/fault_tests/ingestion_faults.sh nn_down` | Stop the NameNode for 30 seconds, restore it, then verify the replay output. | Compare valid records, malformed records, and duplicate records before and after recovery. |
| Capture pause | `scripts/fault_tests/ingestion_faults.sh capture_pause` | Pause the producer for one minute, resume it, and verify stable CSV files. | Confirm no malformed records and that the event-time range resumes after the gap. |

## Interpretation

Flume TAILDIR stores offsets and the file channel commits transactions. This is
at-least-once delivery: a crash can replay the last uncommitted transaction,
so identical rows after restart are duplicates rather than silent loss. The
mitigation is to retain the position file and durable channel, and to use the
verifier's duplicate count when reconciling with a producer sequence number.

## Results

| Date (UTC) | Scenario | Result | Loss | Duplicate rows | Notes |
|---|---|---|---|---|---|
| 2026-09-29 | kill_flume | PASS | unknown | 2 | Local replay simulated a crash replay; duplicate rows were detected as expected for at-least-once delivery. |
| 2026-09-29 | nn_down | PASS | unknown | 0 | Local HDFS outage simulation preserved both stable records; live NameNode outage remains opt-in. |
| 2026-09-29 | capture_pause | PASS | unknown | 0 | Local fixture pause/resume; verifier found no malformed rows. |
