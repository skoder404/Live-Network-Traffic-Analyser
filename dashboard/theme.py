"""
dashboard/theme.py — Cyberpunk/Neon Plotly theme and color tokens for LNTA.

No side effects on import. Call register_lnta_theme() explicitly.
"""

from .theme_cyberpunk import (
    CATEGORICAL_PALETTE_CYBERPUNK,
    CYBERPUNK_TOKENS,
    PROTOCOL_COLOR_MAP_CYBERPUNK,
    SEVERITY_COLORS_CYBERPUNK,
    get_cyberpunk_template,
    get_protocol_color_cyberpunk,
    get_severity_color_cyberpunk,
    register_cyberpunk_theme,
)

# Re-export for backwards compatibility
LNTA_COLORS = CYBERPUNK_TOKENS
PROTOCOL_COLOR_MAP = PROTOCOL_COLOR_MAP_CYBERPUNK
SEVERITY_COLORS = SEVERITY_COLORS_CYBERPUNK
CATEGORICAL_PALETTE = CATEGORICAL_PALETTE_CYBERPUNK


def get_lnta_dark_template():
    """Alias for cyberpunk template (backwards compatibility)."""
    return get_cyberpunk_template()


def register_lnta_theme() -> None:
    """Register and set the LNTA cyberpunk theme as default. Call explicitly at app startup."""
    register_cyberpunk_theme()


def get_protocol_color(protocol: str) -> str:
    """Get color for a protocol (backwards compatibility)."""
    return get_protocol_color_cyberpunk(protocol)


def get_severity_color(severity: str) -> str:
    """Get color for alert severity (backwards compatibility)."""
    return get_severity_color_cyberpunk(severity)