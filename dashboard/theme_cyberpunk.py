"""Cyberpunk/Neon design tokens (single source of truth, mirrored in cyberpunk.css)."""

TOKENS = {
    "bg": "#07060f",
    "panel": "#120f24",
    "text": "#e8e6ff",
    "muted": "#9d98c9",
    "pink": "#ff2e88",
    "cyan": "#22e6ff",
    "lime": "#b6ff3c",
    "amber": "#ffb020",
    "red": "#ff4d5e",
}

FONTS = {"display": "Rajdhani", "mono": "JetBrains Mono"}
SEVERITY = {"WARN": TOKENS["amber"], "CRITICAL": TOKENS["red"]}

# Alias and extended dictionary for test compatibility
CYBERPUNK_TOKENS = {
    "void-base": "#050816",
    "void-elevated": "#0a0f24",
    "void-panel": "#120f24",
    "void-glass": "rgba(18, 15, 36, 0.7)",
    "neon-cyan": "#00ffff",
    "neon-magenta": "#ff00ff",
    "neon-green": "#39ff14",
    "neon-amber": "#ffcc00",
    "neon-red": "#ff1744",
    "neon-blue": "#00b4d8",
    "neon-cyan-glow": "0 0 10px rgba(0,255,255,0.5)",
    "neon-magenta-glow": "0 0 10px rgba(255,0,255,0.5)",
    "neon-green-glow": "0 0 10px rgba(57,255,20,0.5)",
    "neon-amber-glow": "0 0 10px rgba(255,204,0,0.5)",
    "neon-red-glow": "0 0 10px rgba(255,23,68,0.5)",
    "glass-bg": "rgba(18, 15, 36, 0.7)",
    "glass-border": "rgba(34, 230, 255, 0.2)",
    "glass-highlight": "rgba(255, 255, 255, 0.1)",
    "glass-shadow": "0 8px 32px 0 rgba(0, 0, 0, 0.37)",
    "ink-primary": "#e8e6ff",
    "ink-secondary": "#9d98c9",
    "ink-muted": "#6b6699",
    "ink-inverse": "#07060f",
    "scanline-color": "rgba(34, 230, 255, 0.04)",
    "scanline-gap": "4px",
    "scanline-speed": "8s",
    "scanline-opacity": "0.04",
    "font-display": "Orbitron, Rajdhani, sans-serif",
    "font-mono": "JetBrains Mono, monospace",
    "text-display": "2.5rem",
    "text-xl": "1.5rem",
    "text-lg": "1.25rem",
    "text-base": "1rem",
    "text-sm": "0.875rem",
    "text-xs": "0.75rem",
    "radius-sm": "4px",
    "radius-md": "8px",
    "radius-lg": "12px",
    "radius-xl": "16px",
    "radius-full": "9999px",
    "radius-sharp": "0px",
    "glow-sm": "0 0 5px",
    "glow-md": "0 0 10px",
    "glow-lg": "0 0 20px",
    "glow-xl": "0 0 30px",
    "shadow-sm": "0 1px 2px rgba(0,0,0,0.5)",
    "shadow-md": "0 4px 6px rgba(0,0,0,0.5)",
    "shadow-lg": "0 10px 15px rgba(0,0,0,0.5)",
    "shadow-glow": "0 0 15px rgba(34, 230, 255, 0.3)",
    "duration-instant": "50ms",
    "duration-fast": "150ms",
    "duration-base": "300ms",
    "duration-slow": "500ms",
    "duration-scan": "8s",
    "duration-pulse": "2s",
    "ease-out": "cubic-bezier(0.0, 0.0, 0.2, 1)",
    "ease-spring": "cubic-bezier(0.175, 0.885, 0.32, 1.275)",
    "ease-smooth": "cubic-bezier(0.4, 0.0, 0.2, 1)",
    "cursor-trail-length": "20",
    "cursor-glow-size": "360px",
    "cursor-glow-color": "rgba(34, 230, 255, 0.13)",
    "space-1": "0.25rem",
    "space-4": "1rem",
    "space-8": "3rem",
}


def get_cyberpunk_template():
    """Return a Plotly layout template configured with cyberpunk colors."""
    import plotly.graph_objects as go

    layout = go.Layout(
        paper_bgcolor="#050816",
        plot_bgcolor="#0d1428",
        font=dict(color="#e8e6ff", family="JetBrains Mono"),
        colorway=["#00ffff", "#ff00ff", "#39ff14", "#ffcc00", "#ff1744", "#00b4d8"],
    )
    return go.layout.Template(layout=layout)


def register_cyberpunk_theme():
    """Register 'cyberpunk' theme with Plotly io."""
    import plotly.io as pio

    template = get_cyberpunk_template()
    pio.templates["cyberpunk"] = template
    pio.templates.default = "cyberpunk"


def get_protocol_color_cyberpunk(protocol: str) -> str:
    """Return neon color for a network protocol."""
    proto_map = {
        "TCP": "#00ffff",
        "UDP": "#ff00ff",
        "ICMP": "#39ff14",
        "OTHER": "#00b4d8",
    }
    return proto_map.get(protocol.upper(), "#00b4d8")


def get_severity_color_cyberpunk(severity: str) -> str:
    """Return neon color for alert severity."""
    sev_map = {
        "INFO": "#00ffff",
        "WARN": "#ffcc00",
        "CRITICAL": "#ff1744",
    }
    return sev_map.get(severity.upper(), "#00ffff")
