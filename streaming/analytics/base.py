"""
streaming/analytics/base.py — Abstract interface and context for Lane B streaming analytics.

Defines the Analytic protocol/base class and BatchContext passed into each micro-batch plugin.
Reference: TECH_RULES §3.5
"""

from __future__ import annotations

import logging
import sqlite3
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from common.serving_db import current_utc_iso


@dataclass
class BatchContext:
    """Context and dependencies provided to Lane B analytic plugins per micro-batch."""

    cfg: dict[str, Any]
    db_path: str | Path
    conn: sqlite3.Connection | None = None
    clock: float = field(default_factory=time.time)
    logger: logging.Logger = field(
        default_factory=lambda: logging.getLogger("AnalyticPlugin")
    )
    batch_time: str = field(default_factory=current_utc_iso)


class Analytic(ABC):
    """Abstract Base Class for Lane B micro-batch stream analytic plugins."""

    name: str = "base_analytic"

    @abstractmethod
    def process_batch(
        self, batch_df: DataFrame, batch_id: int, ctx: BatchContext
    ) -> None:
        """
        Processes a single micro-batch DataFrame.

        Args:
            batch_df: Cached micro-batch DataFrame with valid cleaned records.
            batch_id: Monotonically increasing Spark micro-batch ID.
            ctx: BatchContext with configuration, database connection, and logger.
        """
        raise NotImplementedError


class NoopAnalytic(Analytic):
    """A no-op analytic plugin used for testing and dispatcher verification."""

    name = "noop"

    def __init__(self) -> None:
        self.processed_batches: list[int] = []

    def process_batch(
        self, batch_df: DataFrame, batch_id: int, ctx: BatchContext
    ) -> None:
        self.processed_batches.append(batch_id)
        ctx.logger.debug(
            "NoopAnalytic processed batch %d with %d rows", batch_id, batch_df.count()
        )
