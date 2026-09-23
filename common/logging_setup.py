"""
common/logging_setup.py — UTC logging configuration with console and rotating file handlers.

Format: %(asctime)s %(levelname)s %(name)s | %(message)s (strictly in UTC).
"""

from __future__ import annotations

import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path


class UTCFormatter(logging.Formatter):
    """Formatter that outputs timestamps strictly in UTC."""

    converter = time.gmtime


DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s | %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    component_name: str,
    log_dir: str | Path = "logs",
    level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    to_console: bool = True,
) -> logging.Logger:
    """
    Configures and returns a logger for the given component.
    Writes rotating logs to <log_dir>/<component_name>.log and optionally to console.
    """
    logger = logging.getLogger(component_name)
    logger.setLevel(level)

    # Avoid duplicate handlers if setup is called multiple times
    if logger.handlers:
        return logger

    formatter = UTCFormatter(fmt=DEFAULT_LOG_FORMAT, datefmt=DEFAULT_DATE_FORMAT)

    # File handler (with rotation)
    p_log_dir = Path(log_dir)
    p_log_dir.mkdir(parents=True, exist_ok=True)
    log_file = p_log_dir / f"{component_name}.log"

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    if to_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    logger.propagate = False
    return logger
