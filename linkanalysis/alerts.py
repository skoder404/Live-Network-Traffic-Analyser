"""
linkanalysis/alerts.py — Explainable alert engine for LNTA.

Implements behavioral alerts per PRD §FR-ALR and TECH_RULES §3.7:
- Traffic spike vs EWMA baseline
- Unusual destination port activity from single source
- High fan-out (many destination IPs from single source)

All alerts include: current value, baseline, change %, threshold, human-readable reason.
"""

import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import yaml


@dataclass
class AlertRuleConfig:
    """Configuration for a single alert rule."""

    # Traffic spike
    ewma_alpha: float = 0.1
    warmup_windows: int = 30
    warn_multiplier: float = 3.0
    critical_multiplier: float = 6.0
    min_pps: int = 50

    # Unusual port activity
    max_distinct_dst_ports: int = 20

    # High fan-out
    max_distinct_dst_ips: int = 30

    # General
    cooldown_seconds: int = 60
    min_window_packets: int = 10


@dataclass
class AlertState:
    """Persistent state for alert evaluation."""

    # EWMA baseline per metric (key: metric_name)
    ewma_baselines: dict[str, float] = field(default_factory=dict)
    ewma_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    # Cooldown tracking: (alert_type, src_ip) -> last_alert_timestamp
    cooldowns: dict[tuple[str, str], float] = field(default_factory=dict)

    # Alert history for deduplication
    recent_alerts: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class Alert:
    """Represents a generated alert."""

    alert_id: str
    ts: str  # ISO 8601 UTC
    type: str  # TRAFFIC_SPIKE, UNUSUAL_PORT_ACTIVITY, HIGH_FANOUT
    severity: str  # WARN, CRITICAL
    src_ip: str
    metric: str
    current_value: float
    baseline_value: float
    change_pct: float
    threshold: float
    reason: str
    details_json: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "ts": self.ts,
            "type": self.type,
            "severity": self.severity,
            "src_ip": self.src_ip,
            "metric": self.metric,
            "current_value": self.current_value,
            "baseline_value": self.baseline_value,
            "change_pct": self.change_pct,
            "threshold": self.threshold,
            "reason": self.reason,
            "details_json": self.details_json,
        }


def load_alert_config(path: str = "config/alert_rules.yaml") -> AlertRuleConfig:
    """Load alert configuration from YAML file."""
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        return AlertRuleConfig(
            ewma_alpha=data.get("traffic_spike", {}).get("ewma_alpha", 0.1),
            warmup_windows=data.get("traffic_spike", {}).get("warmup_windows", 30),
            warn_multiplier=data.get("traffic_spike", {}).get("warn_multiplier", 3.0),
            critical_multiplier=data.get("traffic_spike", {}).get("critical_multiplier", 6.0),
            min_pps=data.get("traffic_spike", {}).get("min_pps", 50),
            max_distinct_dst_ports=data.get("unusual_port_activity", {}).get(
                "max_distinct_dst_ports", 20
            ),
            max_distinct_dst_ips=data.get("high_fanout", {}).get("max_distinct_dst_ips", 30),
            cooldown_seconds=data.get("cooldown_seconds", 60),
            min_window_packets=data.get("min_window_packets", 10),
        )
    except FileNotFoundError:
        return AlertRuleConfig()


class AlertEngine:
    """
    Alert evaluation engine.

    Designed to be called per window with aggregated metrics.
    Maintains state (EWMA baselines, cooldowns) across calls.
    """

    def __init__(self, config: AlertRuleConfig | None = None, state: AlertState | None = None):
        self.config = config or AlertRuleConfig()
        self.state = state or AlertState()

    def evaluate_window(
        self,
        window_start: str,
        window_len_s: int,
        pps: float,
        src_ip_port_counts: dict[str, int],
        src_ip_dst_ip_counts: dict[str, int],
        src_ip_packet_counts: dict[str, int],
    ) -> list[Alert]:
        """
        Evaluate all alert rules for a single window.

        Args:
            window_start: ISO timestamp of window start
            window_len_s: Window length in seconds
            pps: Packets per second for the window
            src_ip_port_counts: {src_ip: distinct_dst_port_count}
            src_ip_dst_ip_counts: {src_ip: distinct_dst_ip_count}
            src_ip_packet_counts: {src_ip: packet_count}

        Returns:
            List of generated Alert objects
        """
        alerts = []

        # 1. Traffic spike alert (global, no src_ip)
        spike_alert = self._check_traffic_spike(window_start, pps)
        if spike_alert:
            alerts.append(spike_alert)

        # 2. Per-source alerts
        for src_ip in src_ip_packet_counts:
            pkt_count = src_ip_packet_counts[src_ip]
            if pkt_count < self.config.min_window_packets:
                continue

            # Unusual port activity
            port_alert = self._check_unusual_port_activity(
                window_start, src_ip, src_ip_port_counts.get(src_ip, 0)
            )
            if port_alert:
                alerts.append(port_alert)

            # High fan-out
            fanout_alert = self._check_high_fanout(
                window_start, src_ip, src_ip_dst_ip_counts.get(src_ip, 0)
            )
            if fanout_alert:
                alerts.append(fanout_alert)

        return alerts

    def _check_traffic_spike(self, window_start: str, pps: float) -> Alert | None:
        """Check for traffic spike vs EWMA baseline."""
        metric = "global_pps"
        alpha = self.config.ewma_alpha
        warmup = self.config.warn_multiplier
        critical_mult = self.config.critical_multiplier
        min_pps = self.config.min_pps

        if pps < min_pps:
            return None

        # Get current baseline (before update)
        baseline = self.state.ewma_baselines.get(metric)
        count = self.state.ewma_counts[metric]

        if baseline is None:
            # First window - initialize
            self.state.ewma_baselines[metric] = pps
            self.state.ewma_counts[metric] = 1
            return None

        # Need warmup windows before alerting
        if count + 1 < self.config.warmup_windows:
            # Still update EWMA but don't alert
            new_baseline = alpha * pps + (1 - alpha) * baseline
            self.state.ewma_baselines[metric] = new_baseline
            self.state.ewma_counts[metric] = count + 1
            return None

        # Check thresholds against OLD baseline (before update)
        warn_threshold = warmup * baseline
        critical_threshold = critical_mult * baseline

        if pps >= critical_threshold:
            severity = "CRITICAL"
            threshold = critical_threshold
        elif pps >= warn_threshold:
            severity = "WARN"
            threshold = warn_threshold
        else:
            # Still update EWMA even if no alert
            new_baseline = alpha * pps + (1 - alpha) * baseline
            self.state.ewma_baselines[metric] = new_baseline
            self.state.ewma_counts[metric] = count + 1
            return None

        # Now update EWMA with current value
        new_baseline = alpha * pps + (1 - alpha) * baseline
        self.state.ewma_baselines[metric] = new_baseline
        self.state.ewma_counts[metric] = count + 1

        change_pct = ((pps - baseline) / baseline) * 100 if baseline > 0 else 0

        # Cooldown check
        cooldown_key = ("TRAFFIC_SPIKE", "global")
        if self._in_cooldown(cooldown_key):
            return None

        alert = Alert(
            alert_id=str(uuid.uuid4()),
            ts=window_start,
            type="TRAFFIC_SPIKE",
            severity=severity,
            src_ip="global",
            metric="pps",
            current_value=round(pps, 2),
            baseline_value=round(new_baseline, 2),
            change_pct=round(change_pct, 1),
            threshold=round(threshold, 2),
            reason=(
                f"Traffic spike detected: {pps:.1f} pps is {change_pct:.1f}% "
                f"above EWMA baseline of {new_baseline:.1f} pps "
                f"(threshold: {threshold:.1f} pps, {severity.lower()})"
            ),
            details_json=json.dumps(
                {
                    "ewma_alpha": alpha,
                    "warmup_windows": self.config.warmup_windows,
                    "window_count": count + 1,
                }
            ),
        )
        self._set_cooldown(cooldown_key)
        return alert

    def _check_unusual_port_activity(
        self, window_start: str, src_ip: str, distinct_port_count: int
    ) -> Alert | None:
        """Check if single source accesses too many distinct destination ports."""
        max_ports = self.config.max_distinct_dst_ports
        if distinct_port_count <= max_ports:
            return None

        cooldown_key = ("UNUSUAL_PORT_ACTIVITY", src_ip)
        if self._in_cooldown(cooldown_key):
            return None

        alert = Alert(
            alert_id=str(uuid.uuid4()),
            ts=window_start,
            type="UNUSUAL_PORT_ACTIVITY",
            severity="WARN",
            src_ip=src_ip,
            metric="distinct_dst_ports",
            current_value=float(distinct_port_count),
            baseline_value=float(max_ports),
            change_pct=round(((distinct_port_count - max_ports) / max_ports) * 100, 1),
            threshold=float(max_ports),
            reason=(
                f"Source {src_ip} accessed {distinct_port_count} distinct destination ports "
                f"(threshold: {max_ports}). Possible port scanning or service enumeration."
            ),
            details_json=json.dumps({"max_allowed": max_ports}),
        )
        self._set_cooldown(cooldown_key)
        return alert

    def _check_high_fanout(
        self, window_start: str, src_ip: str, distinct_ip_count: int
    ) -> Alert | None:
        """Check if single source communicates with too many distinct destination IPs."""
        max_ips = self.config.max_distinct_dst_ips
        if distinct_ip_count <= max_ips:
            return None

        cooldown_key = ("HIGH_FANOUT", src_ip)
        if self._in_cooldown(cooldown_key):
            return None

        alert = Alert(
            alert_id=str(uuid.uuid4()),
            ts=window_start,
            type="HIGH_FANOUT",
            severity="WARN",
            src_ip=src_ip,
            metric="distinct_dst_ips",
            current_value=float(distinct_ip_count),
            baseline_value=float(max_ips),
            change_pct=round(((distinct_ip_count - max_ips) / max_ips) * 100, 1),
            threshold=float(max_ips),
            reason=(
                f"Source {src_ip} communicated with {distinct_ip_count} distinct destination IPs "
                f"(threshold: {max_ips}). Possible lateral movement or distributed scanning."
            ),
            details_json=json.dumps({"max_allowed": max_ips}),
        )
        self._set_cooldown(cooldown_key)
        return alert

    def _in_cooldown(self, key: tuple[str, str]) -> bool:
        """Check if alert type + src_ip is in cooldown period."""
        last_ts = self.state.cooldowns.get(key)
        if last_ts is None:
            return False
        return (time.time() - last_ts) < self.config.cooldown_seconds

    def _set_cooldown(self, key: tuple[str, str]) -> None:
        """Record alert timestamp for cooldown."""
        self.state.cooldowns[key] = time.time()

    def get_state_snapshot(self) -> dict[str, Any]:
        """Get serializable state for persistence."""
        return {
            "ewma_baselines": self.state.ewma_baselines,
            "ewma_counts": dict(self.state.ewma_counts),
            "cooldowns": {f"{k[0]}|{k[1]}": v for k, v in self.state.cooldowns.items()},
        }

    @classmethod
    def from_state_snapshot(
        cls, snapshot: dict[str, Any], config: AlertRuleConfig | None = None
    ) -> "AlertEngine":
        """Restore engine from persisted state."""
        state = AlertState(
            ewma_baselines=snapshot.get("ewma_baselines", {}),
            ewma_counts=defaultdict(int, snapshot.get("ewma_counts", {})),
            cooldowns={tuple(k.split("|", 1)): v for k, v in snapshot.get("cooldowns", {}).items()},
        )
        return cls(config=config, state=state)
