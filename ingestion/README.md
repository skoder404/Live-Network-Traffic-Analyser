# ingestion

Owner: **Rithika GV**

This folder owns the Capture -> Flume -> HDFS handoff and the historical storage
contract. The pipeline has two HDFS destinations:

- `/traffic/raw`: durable historical copy
- `/traffic/stream_in`: low-latency input for Spark Structured Streaming

Both destinations are fed by the replicating Flume selector. HDFS sink files are
written with a `.inprogress-` prefix while open and renamed to `traffic-*.csv`
when closed, so Spark and Hive never consume a partial file.

## Offline check

The local backend needs no Hadoop installation and is suitable for replay/demo:

```bash
../rithu/bin/python -m ingestion.scripts.ingestion_ctl init --local --root data/traffic
../rithu/bin/python -m ingestion.scripts.ingestion_ctl verify --local --root data/traffic
```

The verifier reports malformed rows, exact duplicate rows, and event-to-observation
latency. Packet loss is marked `not_available_without_sequence_id`; the capture
producer must add a monotonic sequence field before loss can be proven.

## HDFS and Flume

```bash
./ingestion/hdfs/init_zones.sh
flume-ng agent --conf-file ingestion/flume/lnta-agent.conf --name lnta
```

Run status from another shell:

```bash
../rithu/bin/python -m ingestion.scripts.ingestion_ctl status --root /traffic
```

Set the capture glob and Flume directories in `ingestion/flume/lnta-agent.conf`
for the host. Confirm Hadoop/Flume compatibility with `java -version`,
`hadoop version`, and `flume-ng version` before starting the agent.

## Hive and Spark handoff

Run `ingestion/hive/external_tables.sql` with Beeline, then run
`MSCK REPAIR TABLE` after a new hourly partition appears. Spark should read
`hdfs://localhost:9000/traffic/stream_in` and checkpoint under
`hdfs://localhost:9000/traffic/checkpoints`; Python callers can build these
paths with `ingestion.spark_hdfs.streaming_paths`.
