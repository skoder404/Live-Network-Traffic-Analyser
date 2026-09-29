# LNTA Runbook

This runbook covers the end-to-end operation of the Live Network Traffic Analyser pipeline.

## Pre-flight Checklist

Before starting the pipeline, verify:
- **Environment**: Ensure the `.venv` is active (`source .venv/bin/activate`).
- **Disk Space**: Verify at least 10GB free space for HDFS data directories and local logs.
- **Dependencies**: Ensure Hadoop (`hdfs`), Spark (`spark-submit`), and Flume are installed and in the PATH.
- **Configuration**: Check that `config/settings.yaml` is configured based on `settings.example.yaml`.

## Start/Stop Procedures

### HDFS
- **Start**: `start-dfs.sh`
- **Stop**: `stop-dfs.sh`
- **Init Zones**: `./ingestion/hdfs/init_zones.sh`

### Capture
- **Start**: `sudo python -m capture.main --interface eth0`
- **Stop**: Send SIGINT (Ctrl+C)

### Flume
- **Start**: `./ingestion/scripts/flume_ctl.sh start`
- **Status**: `./ingestion/scripts/flume_ctl.sh status`
- **Stop**: `./ingestion/scripts/flume_ctl.sh stop`

The Flume agent tails the capture CSV glob, writes a durable historical copy to `/traffic/raw`, and a low-latency copy to `/traffic/stream_in`.

### Spark (Processing)
- **Start**: `spark-submit --master local[*] streaming/app.py`
- **Stop**: Send SIGINT (Ctrl+C) or kill the Spark driver process.

### Dashboard
- **Start**: `streamlit run dashboard/app.py`
- **Stop**: Send SIGINT (Ctrl+C)

## Troubleshooting Guide

- **Flume unable to connect to HDFS**: Ensure HDFS is running. If NameNode is unavailable, leave Flume stopped, restore HDFS, and restart. Do not delete `/tmp/lnta-taildir.json`.
- **Spark streaming lags**: Increase executor memory or reduce batch interval.
- **No data in dashboard**: Check that the capture script is generating CSVs, Flume is tailing them (check `tail -n 50 logs/flume.log`), and Spark is processing `/traffic/stream_in`.
- **Malformed records detected**: View the JSON report (`python -m ingestion.scripts.ingestion_ctl status --root /traffic`) to see duplicate/malformed counts.

## Offline Fallback

To run with replay data when live capture isn't available:
```bash
python -m ingestion.scripts.ingestion_ctl init --local --root data/traffic
# Copy replay data to data/traffic/stream_in/
python -m ingestion.scripts.ingestion_ctl verify --local --root data/traffic
```
This supports replay and demo work without Hadoop or Flume.

For offline demos, you can also use a pre-captured snapshot. Use the `scripts/offline_snapshot.sh` script to capture a snapshot of serving tables.
