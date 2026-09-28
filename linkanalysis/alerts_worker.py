"""
linkanalysis/alerts_worker.py — Background worker evaluating alert rules and persisting alerts.
"""

import logging
import os
import sqlite3
import time

from common.config import load_config
from common.serving_db import get_connection
from linkanalysis.alerts import Alert, AlertEngine, AlertRuleConfig

logger = logging.getLogger("lnta.alerts_worker")


def evaluate_and_persist(conn: sqlite3.Connection, engine: AlertEngine) -> list[Alert]:
    """Run alert checks against recent database records and insert generated alerts."""
    alerts: list[Alert] = []

    # 1. Traffic spike check
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT window_start, packets, bytes
            FROM window_metrics
            WHERE window_len_s = 10
            ORDER BY window_start DESC
            LIMIT 10
            """
        )
        rows = cur.fetchall()
        for row in reversed(rows):
            ws = row["window_start"]
            pkts = row["packets"] or 0
            alert = engine.check_traffic_spike(window_start=ws, packet_count=pkts)
            if alert:
                alerts.append(alert)
    except Exception as e:
        logger.debug(f"Error querying window_metrics: {e}")

    # 2. Per-source checks (unusual ports & fanout)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT window_start, src_ip, unique_dst_ips, unique_dst_ports
            FROM source_stats
            WHERE window_len_s = 10
            ORDER BY window_start DESC
            LIMIT 50
            """
        )
        rows = cur.fetchall()
        for row in rows:
            ws = row["window_start"]
            src = row["src_ip"]
            dst_ips = row["unique_dst_ips"] or 0
            dst_ports = row["unique_dst_ports"] or 0

            port_alert = engine.check_unusual_ports(
                window_start=ws, src_ip=src, distinct_port_count=dst_ports
            )
            if port_alert:
                alerts.append(port_alert)

            fanout_alert = engine.check_high_fanout(
                window_start=ws, src_ip=src, distinct_ip_count=dst_ips
            )
            if fanout_alert:
                alerts.append(fanout_alert)
    except Exception as e:
        logger.debug(f"Error querying source_stats: {e}")

    # Persist alerts to DB
    if alerts:
        cur = conn.cursor()
        for a in alerts:
            cur.execute(
                """
                INSERT OR IGNORE INTO alerts (
                    alert_id, ts, type, severity, src_ip, metric,
                    current_value, baseline_value, change_pct, threshold,
                    reason, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    a.alert_id,
                    a.ts,
                    a.type,
                    a.severity,
                    a.src_ip,
                    a.metric,
                    a.current_value,
                    a.baseline_value,
                    a.change_pct,
                    a.threshold,
                    a.reason,
                    a.details_json,
                ),
            )
        conn.commit()
        logger.info(f"Persisted {len(alerts)} alerts.")

    return alerts


def run_worker(db_path: str, poll_interval_s: float = 5.0, once: bool = False) -> None:
    """Run alert worker loop."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    logger.info(f"Starting alert worker on DB: {db_path}")

    config = AlertRuleConfig()
    engine = AlertEngine(config=config)

    while True:
        try:
            if os.path.exists(db_path):
                conn = get_connection(db_path, read_only=False)
                evaluate_and_persist(conn, engine)
                conn.close()
        except Exception as e:
            logger.warning(f"Error in alert worker tick: {e}")

        if once:
            break
        time.sleep(poll_interval_s)


if __name__ == "__main__":
    cfg = load_config()
    db_file = cfg.get("serving", {}).get("db_path", "serving/analytics.db")
    run_worker(db_file, poll_interval_s=5.0)
