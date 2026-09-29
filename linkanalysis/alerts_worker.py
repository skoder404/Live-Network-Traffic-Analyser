import argparse
import logging
import time

from common.serving_db import connect
from linkanalysis.alerts import AlertEngine, load_alert_config

logger = logging.getLogger(__name__)


def evaluate_window(engine, window, db_conn):
    cur = db_conn.cursor()
    window_start = window["window_start"]
    pps = window["pps"]

    cur.execute(
        """
        SELECT src_ip, packets, unique_dst_ports, unique_dst_ips
        FROM source_stats
        WHERE window_start = ?
    """,
        (window_start,),
    )

    rows = cur.fetchall()
    src_ip_packet_counts = {r["src_ip"]: r["packets"] for r in rows}
    src_ip_port_counts = {r["src_ip"]: r["unique_dst_ports"] for r in rows}
    src_ip_dst_ip_counts = {r["src_ip"]: r["unique_dst_ips"] for r in rows}

    alerts = engine.evaluate_window(
        window_start=window_start,
        pps=pps,
        src_ip_packet_counts=src_ip_packet_counts,
        src_ip_port_counts=src_ip_port_counts,
        src_ip_dst_ip_counts=src_ip_dst_ip_counts,
    )

    if alerts:
        logger.info(f"Generated {len(alerts)} alerts for window {window_start}")
        for alert in alerts:
            cur.execute(
                """
                INSERT INTO alerts (alert_id, ts, type, severity, src_ip, metric, current_value, baseline_value, change_pct, threshold, reason, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    alert.alert_id,
                    alert.ts,
                    alert.type,
                    alert.severity,
                    alert.src_ip,
                    alert.metric,
                    alert.current_value,
                    alert.baseline_value,
                    alert.change_pct,
                    alert.threshold,
                    alert.reason,
                    alert.details_json,
                ),
            )
        db_conn.commit()


def run_worker(db_path: str, poll_interval: float = 1.0):
    conn = connect(db_path)
    engine = AlertEngine(config=load_alert_config())
    last_window_start = None

    while True:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT window_start, pps, bps
                FROM window_metrics
                ORDER BY window_start DESC
                LIMIT 1
            """)
            row = cur.fetchone()

            if row:
                current_window_start = row["window_start"]
                if current_window_start != last_window_start:
                    logger.info(f"Processing new window: {current_window_start}")

                    window = {
                        "window_start": row["window_start"],
                        "pps": row["pps"] or 0.0,
                    }
                    evaluate_window(engine, window, conn)

                    last_window_start = current_window_start

            time.sleep(poll_interval)
        except Exception as e:
            logger.error(f"Error in alerts worker: {e}")
            time.sleep(poll_interval)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="Path to serving SQLite DB")
    parser.add_argument("--interval", type=float, default=2.0, help="Poll interval")
    args = parser.parse_args()

    logger.info(f"Starting alerts worker polling {args.db} every {args.interval}s")
    run_worker(args.db, args.interval)
