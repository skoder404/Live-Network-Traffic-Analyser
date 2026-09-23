"""
common/config.py — Strongly-typed configuration loader with env-var overrides.

Loads config/settings.yaml with fallback to config/settings.example.yaml.
Supports environment overrides in the format: LNTA_<SECTION>__<KEY>
(e.g., LNTA_SPARK__TRIGGER_S=10).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


class ConfigError(Exception):
    """Raised when configuration loading or validation fails."""


@dataclass(frozen=True)
class CaptureConfig:
    iface: str
    rotate_seconds: int
    rotate_mb: int
    out_dir: str
    sink: str


@dataclass(frozen=True)
class FlumeConfig:
    tcp_host: str
    tcp_port: int


@dataclass(frozen=True)
class HdfsConfig:
    namenode_uri: str
    root: str


@dataclass(frozen=True)
class SparkConfig:
    trigger_s: int
    watermark_s: int
    windows_s: list[int]
    slide_s: int
    max_files_per_trigger: int
    max_rows_per_batch: int
    checkpoint_root: str
    plugins_enabled: list[str]
    top_n_ports: int
    max_edges_per_window: int
    filters: list[dict[str, Any]] = field(default_factory=list)
    predicates: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class ServingConfig:
    db_path: str
    cleanup_every_s: int
    retention_hours: int


@dataclass(frozen=True)
class GraphConfig:
    lookback_s: int
    top_n_nodes: int


@dataclass(frozen=True)
class AlertsConfig:
    rules_path: str


@dataclass(frozen=True)
class DashboardConfig:
    refresh_s: int
    timezone: str
    stale_after_s: int


@dataclass(frozen=True)
class ItemsetsConfig:
    window_s: int
    every_s: int
    min_support: float
    num_buckets: int


@dataclass(frozen=True)
class DecayConfig:
    half_lives_s: list[int]


@dataclass(frozen=True)
class AppConfig:
    capture: CaptureConfig
    flume: FlumeConfig
    hdfs: HdfsConfig
    spark: SparkConfig
    serving: ServingConfig
    graph: GraphConfig
    alerts: AlertsConfig
    dashboard: DashboardConfig
    itemsets: ItemsetsConfig
    decay: DecayConfig


REQUIRED_SECTIONS: dict[str, list[str]] = {
    "capture": ["iface", "rotate_seconds", "rotate_mb", "out_dir", "sink"],
    "flume": ["tcp_host", "tcp_port"],
    "hdfs": ["namenode_uri", "root"],
    "spark": [
        "trigger_s",
        "watermark_s",
        "windows_s",
        "slide_s",
        "max_files_per_trigger",
        "max_rows_per_batch",
        "checkpoint_root",
        "plugins_enabled",
        "top_n_ports",
        "max_edges_per_window",
    ],
    "serving": ["db_path", "cleanup_every_s", "retention_hours"],
    "graph": ["lookback_s", "top_n_nodes"],
    "alerts": ["rules_path"],
    "dashboard": ["refresh_s", "timezone", "stale_after_s"],
    "itemsets": ["window_s", "every_s", "min_support", "num_buckets"],
    "decay": ["half_lives_s"],
}


def _parse_env_value(raw: str) -> Any:
    """Attempt type casting of string environment variables to int, float, bool, or json."""
    if raw.lower() in ("true", "yes", "1"):
        return True
    if raw.lower() in ("false", "no", "0"):
        return False
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    if (raw.startswith("[") and raw.endswith("]")) or (raw.startswith("{") and raw.endswith("}")):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
    return raw


def _apply_env_overrides(data: dict[str, Any], prefix: str = "LNTA_") -> dict[str, Any]:
    """
    Applies overrides from environment variables formatted as LNTA_<SECTION>__<KEY>.
    Example: LNTA_SPARK__TRIGGER_S=10 -> data['spark']['trigger_s'] = 10
    """
    for env_key, env_val in os.environ.items():
        if not env_key.startswith(prefix):
            continue
        key_body = env_key[len(prefix) :].lower()
        if "__" not in key_body:
            continue
        section, key = key_body.split("__", 1)
        if section not in data or not isinstance(data[section], dict):
            data[section] = {}
        data[section][key] = _parse_env_value(env_val)
    return data


def _validate_section(data: dict[str, Any], section: str, required_keys: list[str]) -> None:
    if section not in data or not isinstance(data[section], dict):
        raise ConfigError(f"Missing required config section: '{section}'")
    sec_data = data[section]
    for key in required_keys:
        if key not in sec_data or sec_data[key] is None:
            raise ConfigError(f"Missing required config key: '{section}.{key}'")


def load_config(config_path: str | Path | None = None) -> AppConfig:
    """
    Loads configuration from config_path or searches config/settings.yaml -> config/settings.example.yaml.
    Applies LNTA_<SECTION>__<KEY> environment overrides and validates all required fields.
    """
    resolved_path: Path | None = None

    if config_path is not None:
        p = Path(config_path)
        if not p.exists():
            raise ConfigError(f"Specified configuration file not found: {p}")
        resolved_path = p
    else:
        candidates = [
            Path("config/settings.yaml"),
            Path("config/settings.example.yaml"),
            Path(__file__).resolve().parent.parent / "config" / "settings.yaml",
            Path(__file__).resolve().parent.parent / "config" / "settings.example.yaml",
        ]
        for candidate in candidates:
            if candidate.exists():
                resolved_path = candidate
                break

    if resolved_path is None or not resolved_path.exists():
        raise ConfigError("Could not locate config/settings.yaml or config/settings.example.yaml")

    try:
        with open(resolved_path, encoding="utf-8") as f:
            raw_data = yaml.safe_load(f) or {}
    except Exception as e:
        raise ConfigError(f"Failed to parse YAML file at {resolved_path}: {e}") from e

    # Apply environment overrides
    data = _apply_env_overrides(raw_data)

    # Validate all required sections and keys
    for section, keys in REQUIRED_SECTIONS.items():
        _validate_section(data, section, keys)

    try:
        spark_dict = data["spark"]
        return AppConfig(
            capture=CaptureConfig(**data["capture"]),
            flume=FlumeConfig(**data["flume"]),
            hdfs=HdfsConfig(**data["hdfs"]),
            spark=SparkConfig(
                trigger_s=spark_dict["trigger_s"],
                watermark_s=spark_dict["watermark_s"],
                windows_s=list(spark_dict["windows_s"]),
                slide_s=spark_dict["slide_s"],
                max_files_per_trigger=spark_dict["max_files_per_trigger"],
                max_rows_per_batch=spark_dict["max_rows_per_batch"],
                checkpoint_root=spark_dict["checkpoint_root"],
                plugins_enabled=list(spark_dict["plugins_enabled"]),
                top_n_ports=spark_dict["top_n_ports"],
                max_edges_per_window=spark_dict["max_edges_per_window"],
                filters=spark_dict.get("filters", []),
                predicates=spark_dict.get("predicates", []),
            ),
            serving=ServingConfig(**data["serving"]),
            graph=GraphConfig(**data["graph"]),
            alerts=AlertsConfig(**data["alerts"]),
            dashboard=DashboardConfig(**data["dashboard"]),
            itemsets=ItemsetsConfig(**data["itemsets"]),
            decay=DecayConfig(half_lives_s=list(data["decay"]["half_lives_s"])),
        )
    except (TypeError, KeyError) as e:
        raise ConfigError(f"Error mapping configuration to dataclasses: {e}") from e


_CONFIG_CACHE: AppConfig | None = None


def get_config(reload: bool = False) -> AppConfig:
    """Returns the application singleton config instance, loading it on first call."""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None or reload:
        _CONFIG_CACHE = load_config()
    return _CONFIG_CACHE
