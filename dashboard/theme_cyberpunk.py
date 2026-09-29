"""
dashboard/theme_cyberpunk.py — Cyberpunk/Neon design tokens and Plotly template.
"""

import plotly.graph_objects as go
import plotly.io as pio

CYBERPUNK_TOKENS = {
    "void-base": "#050816",
    "void-elevated": "#0d1428",
    "void-panel": "#121b35",
    "void-glass": "rgba(13, 20, 40, 0.75)",
    "neon-cyan": "#00ffff",
    "neon-magenta": "#ff00ff",
    "neon-green": "#39ff14",
    "neon-amber": "#ffcc00",
    "neon-red": "#ff1744",
    "neon-blue": "#00b4d8",
    "neon-cyan-glow": "rgba(0, 255, 255, 0.4)",
    "neon-magenta-glow": "rgba(255, 0, 255, 0.4)",
    "neon-green-glow": "rgba(57, 255, 20, 0.4)",
    "neon-amber-glow": "rgba(255, 204, 0, 0.4)",
    "neon-red-glow": "rgba(255, 23, 68, 0.4)",
    "glass-bg": "rgba(13, 20, 40, 0.65)",
    "glass-border": "rgba(0, 255, 255, 0.25)",
    "glass-highlight": "rgba(255, 255, 255, 0.08)",
    "glass-shadow": "0 8px 32px 0 rgba(0, 0, 0, 0.5)",
    "ink-primary": "#f0f4fc",
    "ink-secondary": "#a0aec0",
    "ink-muted": "#64748b",
    "ink-inverse": "#050816",
    "scanline-color": "rgba(0, 255, 255, 0.03)",
    "scanline-gap": "4px",
    "font-display": "'Orbitron', 'Rajdhani', sans-serif",
    "font-mono": "'JetBrains Mono', monospace",
    "text-display": "2.25rem",
    "text-xl": "1.5rem",
    "text-lg": "1.25rem",
    "text-base": "1rem",
    "text-sm": "0.875rem",
    "text-xs": "0.75rem",
    "space-1": "0.25rem",
    "space-2": "0.5rem",
    "space-3": "0.75rem",
    "space-4": "1rem",
    "space-6": "1.5rem",
    "space-8": "3rem",
    "radius-sm": "4px",
    "radius-md": "8px",
    "radius-lg": "12px",
    "radius-xl": "16px",
    "radius-full": "9999px",
    "radius-sharp": "0px",
    "glow-sm": "0 0 8px rgba(0, 255, 255, 0.3)",
    "glow-md": "0 0 16px rgba(0, 255, 255, 0.4)",
    "glow-lg": "0 0 24px rgba(0, 255, 255, 0.5)",
    "glow-xl": "0 0 32px rgba(0, 255, 255, 0.6)",
    "shadow-sm": "0 1px 2px rgba(0, 0, 0, 0.4)",
    "shadow-md": "0 4px 6px rgba(0, 0, 0, 0.4)",
    "shadow-lg": "0 10px 15px rgba(0, 0, 0, 0.4)",
    "shadow-glow": "0 0 20px rgba(0, 255, 255, 0.3)",
    "duration-instant": "50ms",
    "duration-fast": "150ms",
    "duration-base": "250ms",
    "duration-slow": "400ms",
    "duration-scan": "8s",
    "duration-pulse": "2s",
    "ease-out": "cubic-bezier(0.16, 1, 0.3, 1)",
    "ease-spring": "cubic-bezier(0.34, 1.56, 0.64, 1)",
    "ease-smooth": "cubic-bezier(0.4, 0, 0.2, 1)",
    "scanline-speed": "8s",
    "scanline-opacity": "0.04",
    "cursor-trail-length": "12",
    "cursor-glow-size": "24px",
    "cursor-glow-color": "rgba(0, 255, 255, 0.3)",
}

TOKENS = CYBERPUNK_TOKENS
FONTS = {"display": CYBERPUNK_TOKENS["font-display"], "mono": CYBERPUNK_TOKENS["font-mono"]}
SEVERITY = {"WARN": CYBERPUNK_TOKENS["neon-amber"], "CRITICAL": CYBERPUNK_TOKENS["neon-red"]}


def get_cyberpunk_template() -> go.layout.Template:
    """Create a Plotly layout template styled with cyberpunk neon tokens."""
    template = go.layout.Template()
    template.layout.paper_bgcolor = CYBERPUNK_TOKENS["void-base"]
    template.layout.plot_bgcolor = CYBERPUNK_TOKENS["void-elevated"]
    template.layout.font = dict(
        color=CYBERPUNK_TOKENS["ink-primary"],
        family=CYBERPUNK_TOKENS["font-mono"],
    )
    template.layout.colorway = [
        CYBERPUNK_TOKENS["neon-cyan"],
        CYBERPUNK_TOKENS["neon-magenta"],
        CYBERPUNK_TOKENS["neon-green"],
        CYBERPUNK_TOKENS["neon-amber"],
        CYBERPUNK_TOKENS["neon-blue"],
        CYBERPUNK_TOKENS["neon-red"],
    ]
    return template


def register_cyberpunk_theme() -> None:
    """Register 'cyberpunk' Plotly template in pio.templates."""
    pio.templates["cyberpunk"] = get_cyberpunk_template()
    pio.templates.default = "cyberpunk"


def get_protocol_color_cyberpunk(proto: str) -> str:
    """Return neon color for protocol name."""
    mapping = {
        "TCP": CYBERPUNK_TOKENS["neon-cyan"],
        "UDP": CYBERPUNK_TOKENS["neon-magenta"],
        "ICMP": CYBERPUNK_TOKENS["neon-green"],
        "OTHER": CYBERPUNK_TOKENS["neon-blue"],
    }
    return mapping.get(proto.upper(), CYBERPUNK_TOKENS["neon-blue"])


def get_severity_color_cyberpunk(sev: str) -> str:
    """Return neon color for severity level."""
    mapping = {
        "INFO": CYBERPUNK_TOKENS["neon-cyan"],
        "WARN": CYBERPUNK_TOKENS["neon-amber"],
        "CRITICAL": CYBERPUNK_TOKENS["neon-red"],
    }
    return mapping.get(sev.upper(), CYBERPUNK_TOKENS["neon-cyan"])
