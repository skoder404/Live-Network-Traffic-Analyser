"""
tests/unit/test_alerts.py — Unit tests for alert engine.
"""
import pytest
import time

from linkanalysis.alerts import (
    AlertEngine,
    AlertRuleConfig,
    AlertState,
    load_alert_config,
)


@pytest.fixture
def default_config():
    return AlertRuleConfig(
        ewma_alpha=0.1,
        warmup_windows=5,  # Small for testing
        warn_multiplier=3.0,
        critical_multiplier=6.0,
        min_pps=10,
        max_distinct_dst_ports=5,
        max_distinct_dst_ips=5,
        cooldown_seconds=1,
        min_window_packets=1,
    )


@pytest.fixture
def engine(default_config):
    return AlertEngine(config=default_config)


class TestAlertEngine:
    def test_no_alert_below_min_pps(self, engine):
        """No traffic spike alert if pps < min_pps."""
        alerts = engine.evaluate_window(
            window_start="2026-09-23T12:00:00Z",
            window_len_s=10,
            pps=5,  # Below min_pps=10
            src_ip_port_counts={},
            src_ip_dst_ip_counts={},
            src_ip_packet_counts={},
        )
        assert len(alerts) == 0

    def test_no_alert_during_warmup(self, engine):
        """No alert during EWMA warmup period."""
        for i in range(4):  # warmup_windows=5, so 4 windows = still warming up
            alerts = engine.evaluate_window(
                window_start=f"2026-09-23T12:00:{i*10:02d}Z",
                window_len_s=10,
                pps=100,
                src_ip_port_counts={},
                src_ip_dst_ip_counts={},
                src_ip_packet_counts={},
            )
            assert len(alerts) == 0

    def test_warn_alert_after_warmup(self, engine):
        """WARN alert when pps > 3x baseline after warmup."""
        # Warmup: 5 windows at 100 pps -> baseline = 100
        for i in range(5):
            engine.evaluate_window(
                window_start=f"2026-09-23T12:00:{i*10:02d}Z",
                window_len_s=10,
                pps=100,
                src_ip_port_counts={},
                src_ip_dst_ip_counts={},
                src_ip_packet_counts={},
            )

        # Window 6: spike to 400 pps (4x baseline, > 3x warn threshold)
        alerts = engine.evaluate_window(
            window_start="2026-09-23T12:00:50Z",
            window_len_s=10,
            pps=400,
            src_ip_port_counts={},
            src_ip_dst_ip_counts={},
            src_ip_packet_counts={},
        )

        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.type == "TRAFFIC_SPIKE"
        assert alert.severity == "WARN"
        assert alert.src_ip == "global"
        assert alert.current_value == 400
        assert alert.baseline_value > 0
        assert alert.change_pct > 0

    def test_critical_alert_high_spike(self, engine):
        """CRITICAL alert when pps > 6x baseline."""
        for i in range(5):
            engine.evaluate_window(
                window_start=f"2026-09-23T12:00:{i*10:02d}Z",
                window_len_s=10,
                pps=100,
                src_ip_port_counts={},
                src_ip_dst_ip_counts={},
                src_ip_packet_counts={},
            )

        # Spike to 800 pps (8x baseline, > 6x critical threshold)
        alerts = engine.evaluate_window(
            window_start="2026-09-23T12:00:50Z",
            window_len_s=10,
            pps=800,
            src_ip_port_counts={},
            src_ip_dst_ip_counts={},
            src_ip_packet_counts={},
        )

        assert len(alerts) == 1
        assert alerts[0].severity == "CRITICAL"

    def test_cooldown_prevents_duplicate_alerts(self, engine):
        """Cooldown prevents duplicate alerts within cooldown period."""
        for i in range(5):
            engine.evaluate_window(
                window_start=f"2026-09-23T12:00:{i*10:02d}Z",
                window_len_s=10,
                pps=100,
                src_ip_port_counts={},
                src_ip_dst_ip_counts={},
                src_ip_packet_counts={},
            )

        # First spike
        alerts1 = engine.evaluate_window(
            window_start="2026-09-23T12:00:50Z",
            window_len_s=10,
            pps=400,
            src_ip_port_counts={},
            src_ip_dst_ip_counts={},
            src_ip_packet_counts={},
        )
        assert len(alerts1) == 1

        # Immediate second spike (within cooldown)
        alerts2 = engine.evaluate_window(
            window_start="2026-09-23T12:01:00Z",
            window_len_s=10,
            pps=400,
            src_ip_port_counts={},
            src_ip_dst_ip_counts={},
            src_ip_packet_counts={},
        )
        assert len(alerts2) == 0  # Suppressed by cooldown

    def test_cooldown_expires(self, engine):
        """Alert fires again after cooldown expires."""
        engine.config.cooldown_seconds = 0  # No cooldown for this test

        for i in range(5):
            engine.evaluate_window(
                window_start=f"2026-09-23T12:00:{i*10:02d}Z",
                window_len_s=10,
                pps=100,
                src_ip_port_counts={},
                src_ip_dst_ip_counts={},
                src_ip_packet_counts={},
            )

        alerts1 = engine.evaluate_window(
            window_start="2026-09-23T12:00:50Z",
            window_len_s=10,
            pps=400,
            src_ip_port_counts={},
            src_ip_dst_ip_counts={},
            src_ip_packet_counts={},
        )
        assert len(alerts1) == 1

        alerts2 = engine.evaluate_window(
            window_start="2026-09-23T12:01:00Z",
            window_len_s=10,
            pps=400,
            src_ip_port_counts={},
            src_ip_dst_ip_counts={},
            src_ip_packet_counts={},
        )
        assert len(alerts2) == 1  # Fires again because cooldown=0


class TestUnusualPortActivity:
    def test_alert_when_exceeds_max_ports(self, engine):
        """Alert when src_ip accesses > max_distinct_dst_ports."""
        alerts = engine.evaluate_window(
            window_start="2026-09-23T12:00:00Z",
            window_len_s=10,
            pps=100,
            src_ip_port_counts={"192.168.1.1": 10},  # > max=5
            src_ip_dst_ip_counts={"192.168.1.1": 2},
            src_ip_packet_counts={"192.168.1.1": 50},
        )

        port_alerts = [a for a in alerts if a.type == "UNUSUAL_PORT_ACTIVITY"]
        assert len(port_alerts) == 1
        alert = port_alerts[0]
        assert alert.src_ip == "192.168.1.1"
        assert alert.current_value == 10
        assert alert.threshold == 5

    def test_no_alert_when_within_limit(self, engine):
        """No alert when distinct ports <= max."""
        alerts = engine.evaluate_window(
            window_start="2026-09-23T12:00:00Z",
            window_len_s=10,
            pps=100,
            src_ip_port_counts={"192.168.1.1": 3},  # <= max=5
            src_ip_dst_ip_counts={"192.168.1.1": 2},
            src_ip_packet_counts={"192.168.1.1": 50},
        )

        port_alerts = [a for a in alerts if a.type == "UNUSUAL_PORT_ACTIVITY"]
        assert len(port_alerts) == 0

    def test_port_alert_cooldown(self, engine):
        """Port activity alert respects cooldown."""
        engine.config.cooldown_seconds = 0

        alerts1 = engine.evaluate_window(
            window_start="2026-09-23T12:00:00Z",
            window_len_s=10,
            pps=100,
            src_ip_port_counts={"192.168.1.1": 10},
            src_ip_dst_ip_counts={},
            src_ip_packet_counts={"192.168.1.1": 50},
        )
        assert len([a for a in alerts1 if a.type == "UNUSUAL_PORT_ACTIVITY"]) == 1

        alerts2 = engine.evaluate_window(
            window_start="2026-09-23T12:00:10Z",
            window_len_s=10,
            pps=100,
            src_ip_port_counts={"192.168.1.1": 10},
            src_ip_dst_ip_counts={},
            src_ip_packet_counts={"192.168.1.1": 50},
        )
        assert len([a for a in alerts2 if a.type == "UNUSUAL_PORT_ACTIVITY"]) == 1


class TestHighFanout:
    def test_alert_when_exceeds_max_ips(self, engine):
        """Alert when src_ip talks to > max_distinct_dst_ips."""
        alerts = engine.evaluate_window(
            window_start="2026-09-23T12:00:00Z",
            window_len_s=10,
            pps=100,
            src_ip_port_counts={"192.168.1.1": 2},
            src_ip_dst_ip_counts={"192.168.1.1": 10},  # > max=5
            src_ip_packet_counts={"192.168.1.1": 50},
        )

        fanout_alerts = [a for a in alerts if a.type == "HIGH_FANOUT"]
        assert len(fanout_alerts) == 1
        alert = fanout_alerts[0]
        assert alert.src_ip == "192.168.1.1"
        assert alert.current_value == 10
        assert alert.threshold == 5

    def test_no_alert_when_within_limit(self, engine):
        """No alert when distinct IPs <= max."""
        alerts = engine.evaluate_window(
            window_start="2026-09-23T12:00:00Z",
            window_len_s=10,
            pps=100,
            src_ip_port_counts={"192.168.1.1": 2},
            src_ip_dst_ip_counts={"192.168.1.1": 3},  # <= max=5
            src_ip_packet_counts={"192.168.1.1": 50},
        )

        fanout_alerts = [a for a in alerts if a.type == "HIGH_FANOUT"]
        assert len(fanout_alerts) == 0


class TestMinVolumeFloor:
    def test_no_alert_below_min_packets(self, engine):
        """No per-source alerts if packet count < min_window_packets."""
        alerts = engine.evaluate_window(
            window_start="2026-09-23T12:00:00Z",
            window_len_s=10,
            pps=100,
            src_ip_port_counts={"192.168.1.1": 100},  # Way over limit
            src_ip_dst_ip_counts={"192.168.1.1": 100},
            src_ip_packet_counts={"192.168.1.1": 0},  # Below min=1
        )

        # Should not alert because packet count is 0
        port_alerts = [a for a in alerts if a.type == "UNUSUAL_PORT_ACTIVITY"]
        fanout_alerts = [a for a in alerts if a.type == "HIGH_FANOUT"]
        assert len(port_alerts) == 0
        assert len(fanout_alerts) == 0


class TestAlertStatePersistence:
    def test_state_snapshot_and_restore(self, engine):
        """Engine state can be snapshotted and restored."""
        # Generate some state
        for i in range(5):
            engine.evaluate_window(
                window_start=f"2026-09-23T12:00:{i*10:02d}Z",
                window_len_s=10,
                pps=100,
                src_ip_port_counts={},
                src_ip_dst_ip_counts={},
                src_ip_packet_counts={},
            )

        snapshot = engine.get_state_snapshot()
        assert "ewma_baselines" in snapshot
        assert "ewma_counts" in snapshot
        assert "cooldowns" in snapshot

        # Restore
        engine2 = AlertEngine.from_state_snapshot(snapshot, config=engine.config)
        assert engine2.state.ewma_baselines == engine.state.ewma_baselines
        assert dict(engine2.state.ewma_counts) == dict(engine.state.ewma_counts)


class TestLoadConfig:
    def test_load_default_config(self):
        """Load config from YAML file."""
        config = load_alert_config("config/alert_rules.yaml")
        assert isinstance(config, AlertRuleConfig)
        assert config.ewma_alpha == 0.1
        assert config.warn_multiplier == 3.0
        assert config.max_distinct_dst_ports == 20

    def test_load_missing_config_returns_defaults(self):
        """Missing config file returns defaults."""
        config = load_alert_config("config/nonexistent.yaml")
        assert isinstance(config, AlertRuleConfig)
        assert config.ewma_alpha == 0.1


class TestAlertFields:
    def test_alert_to_dict(self, engine):
        """Alert serializes to dict correctly."""
        for i in range(5):
            engine.evaluate_window(
                window_start=f"2026-09-23T12:00:{i*10:02d}Z",
                window_len_s=10,
                pps=100,
                src_ip_port_counts={},
                src_ip_dst_ip_counts={},
                src_ip_packet_counts={},
            )

        alerts = engine.evaluate_window(
            window_start="2026-09-23T12:00:50Z",
            window_len_s=10,
            pps=400,
            src_ip_port_counts={},
            src_ip_dst_ip_counts={},
            src_ip_packet_counts={},
        )

        alert_dict = alerts[0].to_dict()
        assert "alert_id" in alert_dict
        assert "ts" in alert_dict
        assert "type" in alert_dict
        assert "severity" in alert_dict
        assert "src_ip" in alert_dict
        assert "current_value" in alert_dict
        assert "baseline_value" in alert_dict
        assert "change_pct" in alert_dict
        assert "threshold" in alert_dict
        assert "reason" in alert_dict
        assert "details_json" in alert_dict
