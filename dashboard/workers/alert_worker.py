"""Headless alert evaluator: python -m dashboard.workers.alert_worker"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from dashboard.data import get_db
from linkanalysis import AlertEngine, load_alert_config


def run(interval=5, window=30):
    db, eng = (
        get_db(),
        AlertEngine(
            load_alert_config(
                str(Path(__file__).resolve().parents[2] / "config" / "alert_rules.yaml")
            )
        ),
    )
    last = None
    while True:
        s = db.snapshot(window)
        if s["bucket"] != last:
            last = s["bucket"]
            for a in eng.evaluate_window(
                datetime.now(timezone.utc).isoformat(),
                window,
                s["pps"],
                s["port_counts"],
                s["dst_counts"],
                s["pkt_counts"],
            ):
                print(json.dumps(a.to_dict()), flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    run()
