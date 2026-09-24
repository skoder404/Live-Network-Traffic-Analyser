"""Storage zones and filesystem/HDFS operations for the ingestion pipeline."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StorageLayout:
    """The three ingestion zones shared by Flume, Spark, and Hive."""

    root: str = "/traffic"

    @property
    def raw(self) -> str:
        return f"{self.root}/raw"

    @property
    def stream_in(self) -> str:
        return f"{self.root}/stream_in"

    @property
    def processed(self) -> str:
        return f"{self.root}/processed"

    @property
    def checkpoints(self) -> str:
        return f"{self.root}/checkpoints"

    @property
    def hive_warehouse(self) -> str:
        return f"{self.root}/hive"

    def all_zones(self) -> tuple[str, ...]:
        return (self.raw, self.stream_in, self.processed, self.checkpoints, self.hive_warehouse)


def local_layout(root: str | Path = "data/traffic") -> StorageLayout:
    """Return a layout rooted at a local directory for offline demos."""
    return StorageLayout(str(Path(root)))


def create_local_zones(layout: StorageLayout) -> list[Path]:
    """Create local storage zones and return the created paths."""
    paths = [Path(zone) for zone in layout.all_zones()]
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
    return paths


def create_hdfs_zones(layout: StorageLayout, hdfs_bin: str = "hdfs") -> None:
    """Create HDFS zones with one atomic command and fail with useful output."""
    command = [hdfs_bin, "dfs", "-mkdir", "-p", *layout.all_zones()]
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True)
    except OSError as exc:
        raise RuntimeError(
            f"Unable to execute {hdfs_bin!r}; install Hadoop or use --local"
        ) from exc
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "hdfs dfs -mkdir failed")


def hdfs_available(hdfs_bin: str = "hdfs") -> bool:
    """Return whether the Hadoop CLI can be executed."""
    try:
        return (
            subprocess.run(
                [hdfs_bin, "version"], capture_output=True, check=False, timeout=10
            ).returncode
            == 0
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
