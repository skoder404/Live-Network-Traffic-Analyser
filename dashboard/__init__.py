"""
dashboard package — Streamlit interactive real-time network traffic dashboard.
"""

from .data import ServingDB, get_db
from .theme import (
    LNTA_COLORS,
    get_protocol_color,
    get_severity_color,
    register_lnta_theme,
)

__all__ = [
    "LNTA_COLORS",
    "ServingDB",
    "get_db",
    "get_protocol_color",
    "get_severity_color",
    "register_lnta_theme",
]
