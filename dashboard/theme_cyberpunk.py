"""
dashboard/theme_cyberpunk.py — Cyberpunk/Neon design system token block and Plotly template.

Full Cyberpunk/Neon token block with:
- Void base layer colors
- Neon color system (cyan, magenta, green, amber, red, blue)
- Glass system (bg, border, highlight, shadow)
- Scanline system (color, gap, speed, opacity)
- Typography (Orbitron display + JetBrains Mono)
- Space scale (4px base)
- Radius scale
- Neon glow system (sm/md/lg/xl)
- Motion tokens (durations, easings, scanline speed)
- Cursor glow trail tokens
"""

import plotly.graph_objects as go
import plotly.io as pio

# =============================================================================
# CYBERPUNK/NEON TOKEN BLOCK
# =============================================================================

CYBERPUNK_TOKENS = {
    # ===== VOID BASE LAYER =====
    "void-base": "#050816",           # Deep void background
    "void-elevated": "#0a0f2a",       # Elevated panels
    "void-panel": "#0d1428",          # Standard panels
    "void-glass": "rgba(13, 20, 40, 0.7)",  # Glass panels

    # ===== NEON COLOR SYSTEM =====
    "neon-cyan": "#00ffff",           # Primary: Data flow, primary actions
    "neon-cyan-dim": "rgba(0, 255, 255, 0.3)",
    "neon-cyan-glow": "rgba(0, 255, 255, 0.5)",

    "neon-magenta": "#ff00ff",        # Secondary: Alerts, highlights
    "neon-magenta-dim": "rgba(255, 0, 255, 0.3)",
    "neon-magenta-glow": "rgba(255, 0, 255, 0.5)",

    "neon-green": "#39ff14",          # Success: Healthy, good
    "neon-green-dim": "rgba(57, 255, 20, 0.3)",
    "neon-green-glow": "rgba(57, 255, 20, 0.5)",

    "neon-amber": "#ffcc00",          # Warning: Attention, caution
    "neon-amber-dim": "rgba(255, 204, 0, 0.3)",
    "neon-amber-glow": "rgba(255, 204, 0, 0.5)",

    "neon-red": "#ff1744",            # Critical: Danger, errors
    "neon-red-dim": "rgba(255, 23, 68, 0.3)",
    "neon-red-glow": "rgba(255, 23, 68, 0.5)",

    "neon-blue": "#00b4d8",           # Info: Auxiliary, links
    "neon-blue-dim": "rgba(0, 180, 216, 0.3)",
    "neon-blue-glow": "rgba(0, 180, 216, 0.5)",

    # ===== TEXT HIERARCHY =====
    "ink-primary": "#ffffff",         # Primary text
    "ink-secondary": "#b8c5e0",       # Secondary text
    "ink-muted": "#6b7280",           # Muted/meta text
    "ink-inverse": "#050816",         # Inverse text on neon

    # ===== SURFACE & GLASS =====
    "glass-bg": "rgba(13, 20, 40, 0.6)",
    "glass-border": "rgba(0, 255, 255, 0.2)",
    "glass-highlight": "rgba(0, 255, 255, 0.05)",
    "glass-shadow": (
        "0 4px 32px rgba(0, 0, 0, 0.5), "
        "0 0 0 1px rgba(0, 255, 255, 0.1), "
        "inset 0 1px 0 rgba(255, 255, 255, 0.05)"
    ),

    # ===== SCANLINE OVERLAY =====
    "scanline-color": "rgba(0, 255, 255, 0.03)",
    "scanline-gap": "4px",

    # ===== TYPOGRAPHY =====
    "font-display": '"Orbitron", "JetBrains Mono", "Fira Code", monospace',
    "font-mono": '"JetBrains Mono", "Fira Code", "Consolas", monospace',
    "text-display": "clamp(1.5rem, 3vw, 2.5rem)",
    "text-xl": "clamp(1.25rem, 2.5vw, 2rem)",
    "text-lg": "clamp(1.125rem, 2vw, 1.25rem)",
    "text-base": "1rem",
    "text-sm": "0.875rem",
    "text-xs": "0.75rem",
    "text-mono": '"JetBrains Mono", "Fira Code", monospace',

    # ===== SPACE SCALE (4px base) =====
    "space-1": "0.25rem",
    "space-2": "0.5rem",
    "space-3": "0.75rem",
    "space-4": "1rem",
    "space-5": "1.5rem",
    "space-6": "2rem",
    "space-8": "3rem",
    "space-10": "4rem",

    # ===== RADIUS =====
    "radius-sm": "4px",
    "radius-md": "8px",
    "radius-lg": "12px",
    "radius-xl": "16px",
    "radius-full": "9999px",
    "radius-sharp": "0",

    # ===== NEON GLOW SYSTEM =====
    "glow-sm": "0 0 4px currentColor, 0 0 8px currentColor",
    "glow-md": "0 0 8px currentColor, 0 0 16px currentColor, 0 0 32px currentColor",
    "glow-lg": "0 0 16px currentColor, 0 0 32px currentColor, 0 0 64px currentColor",
    "glow-xl": "0 0 32px currentColor, 0 0 64px currentColor, 0 0 128px currentColor",

    # ===== SHADOWS =====
    "shadow-sm": "0 2px 8px rgba(0,0,0,0.4)",
    "shadow-md": "0 4px 24px rgba(0,0,0,0.5)",
    "shadow-lg": "0 8px 48px rgba(0,0,0,0.6)",
    "shadow-glow": "0 0 0 1px currentColor, var(--glow-md)",

    # ===== MOTION =====
    "duration-instant": "50ms",
    "duration-fast": "100ms",
    "duration-base": "200ms",
    "duration-slow": "300ms",
    "duration-scan": "8s",
    "duration-pulse": "2s",
    "ease-out": "cubic-bezier(0.16, 1, 0.3, 1)",
    "ease-spring": "cubic-bezier(0.34, 1.56, 0.64, 1)",
    "ease-smooth": "cubic-bezier(0.16, 1, 0.3, 1)",

    # ===== SCANLINE ANIMATION =====
    "scanline-speed": "8s",
    "scanline-opacity": "0.04",

    # ===== CURSOR GLOW TRAIL =====
    "cursor-trail-length": "20",
    "cursor-glow-size": "300px",
    "cursor-glow-color": "#00ffff",
}

# =============================================================================
# PROTOCOL COLOR MAP (Cyberpunk Neon)
# =============================================================================

PROTOCOL_COLOR_MAP_CYBERPUNK = {
    "TCP": CYBERPUNK_TOKENS["neon-cyan"],
    "UDP": CYBERPUNK_TOKENS["neon-magenta"],
    "ICMP": CYBERPUNK_TOKENS["neon-green"],
    "OTHER": CYBERPUNK_TOKENS["neon-blue"],
}

# =============================================================================
# SEVERITY COLOR MAP (Cyberpunk Neon)
# =============================================================================

SEVERITY_COLORS_CYBERPUNK = {
    "INFO": CYBERPUNK_TOKENS["neon-cyan"],
    "WARN": CYBERPUNK_TOKENS["neon-amber"],
    "CRITICAL": CYBERPUNK_TOKENS["neon-red"],
}

# =============================================================================
# CATEGORICAL PALETTE (12 distinct neon colors)
# =============================================================================

CATEGORICAL_PALETTE_CYBERPUNK = [
    "#00ffff",   # neon-cyan
    "#ff00ff",   # neon-magenta
    "#39ff14",   # neon-green
    "#ffcc00",   # neon-amber
    "#ff1744",   # neon-red
    "#00b4d8",   # neon-blue
    "#ff6b6b",   # coral
    "#a855f7",   # purple
    "#22d3ee",   # sky
    "#f472b6",   # pink
    "#4ade80",   # emerald
    "#fb923c",   # orange
]


# =============================================================================
# PLOTLY CYBERPUNK TEMPLATE
# =============================================================================

def get_cyberpunk_template() -> go.layout.Template:
    """Create the Cyberpunk/Neon Plotly template."""
    return go.layout.Template(
        layout=go.Layout(
            font={
                "family": CYBERPUNK_TOKENS["font-mono"],
                "color": CYBERPUNK_TOKENS["ink-primary"],
                "size": 12,
            },
            title={
                "font": {
                    "size": 16,
                    "color": CYBERPUNK_TOKENS["ink-primary"],
                    "family": CYBERPUNK_TOKENS["font-display"],
                },
                "x": 0.02,
            },
            paper_bgcolor=CYBERPUNK_TOKENS["void-base"],
            plot_bgcolor=CYBERPUNK_TOKENS["void-panel"],
            xaxis={
                "gridcolor": CYBERPUNK_TOKENS["glass-border"],
                "zerolinecolor": CYBERPUNK_TOKENS["glass-border"],
                "linecolor": CYBERPUNK_TOKENS["glass-border"],
                "tickfont": {"color": CYBERPUNK_TOKENS["ink-secondary"]},
                "title": {"font": {"color": CYBERPUNK_TOKENS["ink-primary"]}},
            },
            yaxis={
                "gridcolor": CYBERPUNK_TOKENS["glass-border"],
                "zerolinecolor": CYBERPUNK_TOKENS["glass-border"],
                "linecolor": CYBERPUNK_TOKENS["glass-border"],
                "tickfont": {"color": CYBERPUNK_TOKENS["ink-secondary"]},
                "title": {"font": {"color": CYBERPUNK_TOKENS["ink-primary"]}},
            },
            legend={
                "bgcolor": CYBERPUNK_TOKENS["void-panel"],
                "bordercolor": CYBERPUNK_TOKENS["glass-border"],
                "font": {"color": CYBERPUNK_TOKENS["ink-primary"]},
            },
            colorway=CATEGORICAL_PALETTE_CYBERPUNK,
            hoverlabel={
                "bgcolor": CYBERPUNK_TOKENS["void-elevated"],
                "bordercolor": CYBERPUNK_TOKENS["glass-border"],
                "font": {"color": CYBERPUNK_TOKENS["ink-primary"]},
            },
            margin={"l": 60, "r": 30, "t": 50, "b": 50},
        )
    )


def register_cyberpunk_theme() -> None:
    """Register and set the Cyberpunk/Neon theme as default. Call explicitly at app startup."""
    template = get_cyberpunk_template()
    pio.templates["cyberpunk"] = template
    pio.templates.default = "cyberpunk"


def get_protocol_color_cyberpunk(protocol: str) -> str:
    """Get cyberpunk neon color for a protocol."""
    return PROTOCOL_COLOR_MAP_CYBERPUNK.get(protocol.upper(), CYBERPUNK_TOKENS["neon-blue"])


def get_severity_color_cyberpunk(severity: str) -> str:
    """Get cyberpunk neon color for alert severity."""
    return SEVERITY_COLORS_CYBERPUNK.get(severity.upper(), CYBERPUNK_TOKENS["neon-cyan"])