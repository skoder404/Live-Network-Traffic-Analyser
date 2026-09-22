"""
tests/unit/test_config.py — Unit tests for config loader and logging setup.
"""

import logging
import os
import tempfile
import unittest
from pathlib import Path

import yaml

from common.config import ConfigError, get_config, load_config
from common.logging_setup import UTCFormatter, setup_logging


class TestConfig(unittest.TestCase):
    def setUp(self):
        # Clear any LNTA_ environment variables before tests
        self._orig_env = os.environ.copy()
        for key in list(os.environ.keys()):
            if key.startswith("LNTA_"):
                del os.environ[key]

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._orig_env)

    def test_load_default_config(self):
        config = load_config()
        self.assertIsNotNone(config)
        self.assertEqual(config.capture.iface, "wlan0")
        self.assertEqual(config.spark.trigger_s, 5)
        self.assertEqual(config.spark.watermark_s, 30)
        self.assertEqual(config.spark.windows_s, [10, 30, 60])
        self.assertEqual(config.dashboard.timezone, "Asia/Kolkata")
        self.assertEqual(config.serving.retention_hours, 24)
        self.assertEqual(config.graph.lookback_s, 60)
        self.assertEqual(config.decay.half_lives_s, [10, 30, 60])

    def test_env_override(self):
        os.environ["LNTA_SPARK__TRIGGER_S"] = "12"
        os.environ["LNTA_CAPTURE__IFACE"] = "eth1"
        os.environ["LNTA_DASHBOARD__REFRESH_S"] = "5"
        os.environ["LNTA_SPARK__WINDOWS_S"] = "[15, 45]"

        config = load_config()
        self.assertEqual(config.spark.trigger_s, 12)
        self.assertEqual(config.capture.iface, "eth1")
        self.assertEqual(config.dashboard.refresh_s, 5)
        self.assertEqual(config.spark.windows_s, [15, 45])

    def test_missing_required_key_raises_config_error(self):
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            temp_path = f.name

            yaml.dump(
                {
                    "capture": {
                        "iface": "w0",
                        "rotate_seconds": 60,
                        "rotate_mb": 10,
                        "out_dir": "data",
                        "sink": "file",
                    },
                    "flume": {
                        "tcp_host": "localhost",
                        "tcp_port": 44444,
                    },
                    "hdfs": {
                        "namenode_uri": "hdfs://localhost",
                        "root": "/traffic",
                    },
                    "spark": {
                        "watermark_s": 30,
                        "windows_s": [10],
                        "slide_s": 5,
                        "max_files_per_trigger": 10,
                        "max_rows_per_batch": 1000,
                        "checkpoint_root": "/cp",
                        "plugins_enabled": [],
                        "top_n_ports": 10,
                        "max_edges_per_window": 100,
                    },
                    "serving": {
                        "db_path": "test.db",
                        "cleanup_every_s": 60,
                        "retention_hours": 24,
                    },
                    "graph": {
                        "lookback_s": 60,
                        "top_n_nodes": 50,
                    },
                    "alerts": {
                        "rules_path": "alert.yaml",
                    },
                    "dashboard": {
                        "refresh_s": 2,
                        "timezone": "UTC",
                        "stale_after_s": 15,
                    },
                    "itemsets": {
                        "window_s": 60,
                        "every_s": 10,
                        "min_support": 0.1,
                        "num_buckets": 100,
                    },
                    "decay": {
                        "half_lives_s": [10],
                    },
                },
                f,
            )

        try:
            with self.assertRaises(ConfigError) as ctx:
                load_config(temp_path)

            self.assertIn("spark.trigger_s", str(ctx.exception))
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_singleton_get_config(self):
        c1 = get_config(reload=True)
        c2 = get_config()
        self.assertIs(c1, c2)

    def test_utc_logging_formatter(self):
        formatter = UTCFormatter(
            fmt="%(asctime)s %(levelname)s %(name)s | %(message)s"
        )

        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname=__file__,
            lineno=10,
            msg="Hello UTC",
            args=(),
            exc_info=None,
        )

        formatted = formatter.format(record)
        self.assertIn("INFO test_logger | Hello UTC", formatted)

        with tempfile.TemporaryDirectory() as tmp_dir:
            logger = setup_logging(
                "test_component",
                log_dir=tmp_dir,
                to_console=False,
            )

            logger.info("Log test message")

            log_file = Path(tmp_dir) / "test_component.log"
            self.assertTrue(log_file.exists())

            content = log_file.read_text(encoding="utf-8")
            self.assertIn("Log test message", content)

            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)


if __name__ == "__main__":
    unittest.main()