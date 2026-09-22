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
python -m ingestion.scripts.ingestion_ctl init --local --root data/traffic
python -m ingestion.scripts.ingestion_ctl verify --local --root data/traffic
```

This supports replay and demo work without Hadoop or Flume.

## Release audit

```bash
./ingestion/scripts/audit_repo.sh
```

Never commit real packet captures, MAC addresses, credentials, or local
`config/settings.yaml`.