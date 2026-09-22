# ingestion/scripts
Owner: **Rithika GV**
Operational scripts for starting HDFS, controlling Flume, and verifying zero packet loss during ingestion.

`ingestion_ctl.py` provides the reproducible control/status surface:

```bash
python -m ingestion.scripts.ingestion_ctl init --local --root data/traffic
python -m ingestion.scripts.ingestion_ctl status --local --root data/traffic
python -m ingestion.scripts.ingestion_ctl verify --local --root data/traffic
```

The command writes `logs/ingestion_status.json` atomically. Verification can
measure duplicates and malformed records immediately; packet loss requires a
producer sequence number and is intentionally reported as unavailable without it.

Use `flume_ctl.sh start|stop|status` to manage the agent and `audit_repo.sh`
before release. The full recovery procedure is in `ingestion/RUNBOOK.md`.
