"""Small Spark/HDFS connection helpers used by the streaming team."""

from __future__ import annotations

from typing import Any


def hdfs_path(namenode_uri: str, path: str) -> str:
    """Join a NameNode URI and absolute HDFS path without duplicate slashes."""
    return f"{namenode_uri.rstrip('/')}/{path.lstrip('/')}"


def streaming_paths(config: dict[str, Any]) -> dict[str, str]:
    """Build stable Spark input/output/checkpoint paths from application config."""
    hdfs = config.get("hdfs", {})
    spark = config.get("spark", {})
    namenode = hdfs.get("namenode_uri", "hdfs://localhost:9000")
    root = hdfs.get("root", "/traffic")
    return {
        "raw": hdfs_path(namenode, f"{root}/raw"),
        "stream_in": hdfs_path(namenode, f"{root}/stream_in"),
        "processed": hdfs_path(namenode, f"{root}/processed"),
        "checkpoint": spark.get("checkpoint_root", hdfs_path(namenode, f"{root}/checkpoints")),
    }
