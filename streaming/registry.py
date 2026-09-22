"""
streaming/registry.py — Plugin registry for Lane B streaming analytics.

Allows dynamic registration and activation of streaming algorithm plugins based on configuration.
"""

from __future__ import annotations

import logging
from typing import Any

from streaming.analytics.base import Analytic

logger = logging.getLogger("StreamingRegistry")

_REGISTRY: dict[str, Analytic] = {}


def register(analytic: Analytic) -> None:
    """Registers an analytic plugin instance by name."""
    if not isinstance(analytic, Analytic):
        raise TypeError(f"Expected Analytic instance, got {type(analytic)}")
    _REGISTRY[analytic.name] = analytic
    logger.debug("Registered analytic plugin: '%s'", analytic.name)


def get_registered(name: str) -> Analytic | None:
    """Retrieves a registered analytic by name."""
    return _REGISTRY.get(name)


def get_enabled(cfg: dict[str, Any]) -> list[Analytic]:
    """
    Returns list of active analytic plugins enabled in configuration.

    Reads `spark.plugins_enabled` list from config. If empty or not set, returns all registered plugins.
    """
    spark_cfg = cfg.get("spark", {})
    enabled_names = spark_cfg.get("plugins_enabled")

    if enabled_names is None:
        return list(_REGISTRY.values())

    active: list[Analytic] = []
    for name in enabled_names:
        if name in _REGISTRY:
            active.append(_REGISTRY[name])
        else:
            logger.warning("Enabled plugin '%s' not found in registry", name)
    return active


def clear_registry() -> None:
    """Clears all registered plugins (primarily for test isolation)."""
    _REGISTRY.clear()
