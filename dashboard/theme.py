"""
dashboard/theme.py — Plotly dark theme and color tokens for LNTA.

No side effects on import. Call register_lnta_theme() explicitly.
"""
import plotly.graph_objects as go
import plotly.io as pio

LNTA_COLORS = {
    "bg_base": "#0A0F1C",
    "bg_panel": "#101828",
    "bg_elevated": "#162033",
    "border": "#1F2A44",
    "text_primary": "#E6EDF7",
    "text_secondary": "#A9B7CC",
    "text_muted": "#8496B0",
    "accent": "#22D3EE",
    "accent_2": "#6366F1",
    "proto_tcp": "#38BDF8",
    "proto_udp": "#A78BFA",
    "proto_icmp": "#FBBF24",
    "proto_other": "#94A3B8",
    "ok": "#22C55E",
    "warn": "#F59E0B",
    "critical": "#EF4444",
    "info": "#38BDF8",
}

PROTOCOL_COLOR_MAP = {
    "TCP": LNTA_COLORS["proto_tcp"],
    "UDP": LNTA_COLORS["proto_udp"],
    "ICMP": LNTA_COLORS["proto_icmp"],
    "OTHER": LNTA_COLORS["proto_other"],
}

# 12 distinct colors (no duplicates)
CATEGORICAL_PALETTE = [
    "#22D3EE", "#A78BFA", "#FBBF24", "#34D399", "#F472B6", "#94A3B8",
    "#60A5FA", "#F87171", "#4ADE80", "#FB923C", "#C084FC", "#14B8A6",
]

SEVERITY_COLORS = {
    "INFO": LNTA_COLORS["info"],
    "WARN": LNTA_COLORS["warn"],
    "CRITICAL": LNTA_COLORS["critical"],
}


def get_lnta_dark_template() -> go.layout.Template:
    """Create the LNTA dark Plotly template."""
    return go.layout.Template(
        layout=go.Layout(
            font={
                "family": "JetBrains Mono, Consolas, Courier New, monospace",
                "color": LNTA_COLORS["text_primary"],
                "size": 12,
            },
            title={
                "font": {"size": 16, "color": LNTA_COLORS["text_primary"]}, "x": 0.02
            },
            paper_bgcolor=LNTA_COLORS["bg_base"],
            plot_bgcolor=LNTA_COLORS["bg_panel"],
            xaxis={
                "gridcolor": LNTA_COLORS["border"],
                "zerolinecolor": LNTA_COLORS["border"],
                "linecolor": LNTA_COLORS["border"],
                "tickfont": {"color": LNTA_COLORS["text_secondary"]},
                "title": {"font": {"color": LNTA_COLORS["text_primary"]}},
            },
            yaxis={
                "gridcolor": LNTA_COLORS["border"],
                "zerolinecolor": LNTA_COLORS["border"],
                "linecolor": LNTA_COLORS["border"],
                "tickfont": {"color": LNTA_COLORS["text_secondary"]},
                "title": {"font": {"color": LNTA_COLORS["text_primary"]}},
            },
            legend={
                "bgcolor": LNTA_COLORS["bg_panel"],
                "bordercolor": LNTA_COLORS["border"],
                "font": {"color": LNTA_COLORS["text_primary"]},
            },
            colorway=CATEGORICAL_PALETTE,
            hoverlabel={
                "bgcolor": LNTA_COLORS["bg_elevated"],
                "bordercolor": LNTA_COLORS["border"],
                "font": {"color": LNTA_COLORS["text_primary"]},
            },
            margin={"l": 60, "r": 30, "t": 50, "b": 50},
        )
    )


def register_lnta_theme() -> None:
    """Register and set the LNTA dark theme as default. Call explicitly at app startup."""
    template = get_lnta_dark_template()
    pio.templates["lnta_dark"] = template
    pio.templates.default = "lnta_dark"


def get_protocol_color(protocol: str) -> str:
    """Get color for a protocol."""
    return PROTOCOL_COLOR_MAP.get(protocol.upper(), LNTA_COLORS["proto_other"])


def get_severity_color(severity: str) -> str:
    """Get color for alert severity."""
    return SEVERITY_COLORS.get(severity.upper(), LNTA_COLORS["info"])
