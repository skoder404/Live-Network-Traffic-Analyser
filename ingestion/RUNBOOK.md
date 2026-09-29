# Ingestion Runbook

## Normal start

```bash
./ingestion/hdfs/init_zones.sh
./ingestion/scripts/flume_ctl.sh start
./ingestion/scripts/flume_ctl.sh status
```

The Flume agent tails the capture CSV glob, writes a durable historical copy to
`/traffic/raw`, and writes a low-latency copy to `/traffic/stream_in`. Only
completed `.csv` files are visible to consumers.

## Health check

```bash
python -m ingestion.scripts.ingestion_ctl status --root /traffic
hdfs dfs -count -q /traffic/raw /traffic/stream_in
tail -n 50 logs/flume.log
```

The JSON report exposes records, malformed rows, exact duplicates, and latency.
Loss is not measurable until the producer includes a sequence number.

## Restart and failure recovery

Stop and start Flume after a configuration or Java/Hadoop classpath change:

```bash
./ingestion/scripts/flume_ctl.sh stop
./ingestion/scripts/flume_ctl.sh start
```

The TAILDIR position file and durable channel preserve the last acknowledged
offset. Do not delete `/tmp/lnta-taildir.json` or the durable channel directory
during recovery. If the NameNode is unavailable, leave Flume stopped, restore
HDFS, and restart; then compare the verifier report with the producer sequence
range when available.

## Offline fallback

```bash
python3 -m ingestion.scripts.ingestion_ctl init --local --root data/traffic
python3 -m ingestion.scripts.ingestion_ctl verify --local --root data/traffic
./scripts/offline_snapshot.sh
LNTA_MOCK=true LNTA_DB=serving/lnta.db streamlit run dashboard/app.py
```

The snapshot command seeds deterministic serving data and writes
`data/sample/offline_snapshot.json`; it does not start a server. This supports
replay and demo work without Hadoop or Flume. Set `PYTHON_BIN` when the project
virtual environment uses a non-default Python executable.

## Fault tests

The reproducible procedures and recorded outcomes are in
`docs/ingestion_report.md`. The capture-pause test is safe to run locally:

```bash
./scripts/fault_tests/ingestion_faults.sh capture_pause
```

All three scenarios run deterministic local simulations by default. To run the
Flume restart or NameNode outage against shared services, set
`LNTA_FAULT_LIVE=1` on the dedicated integration host.

## Closed-hour compaction and retention

Compact only a closed UTC hour; rerunning the command overwrites that partition
idempotently and verifies the row count:

```bash
python3 ingestion/scripts/compact_raw.py --dt 2026-09-28 --hr 23
python3 ingestion/scripts/cleanup_stream_in.py --retention-hours 24 --dry-run
python3 ingestion/scripts/cleanup_stream_in.py --retention-hours 24
```

Cleanup ignores hidden and `.tmp` files. Use `--dry-run` before deleting files.

## Release audit

```bash
./ingestion/scripts/audit_repo.sh
```

Never commit real packet captures, MAC addresses, credentials, or local
`config/settings.yaml`.