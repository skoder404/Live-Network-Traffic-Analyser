"""
tests/unit/test_cyberpunk_theme.py — Unit tests for Cyberpunk/Neon theme tokens and Plotly template.
"""



def test_cyberpunk_tokens_exist():
    """Verify all cyberpunk tokens are defined."""
    from dashboard.theme_cyberpunk import CYBERPUNK_TOKENS

    required_tokens = [
        "void-base",
        "void-elevated",
        "void-panel",
        "void-glass",
        "neon-cyan",
        "neon-magenta",
        "neon-green",
        "neon-amber",
        "neon-red",
        "neon-blue",
        "neon-cyan-glow",
        "neon-magenta-glow",
        "neon-green-glow",
        "neon-amber-glow",
        "neon-red-glow",
        "glass-bg",
        "glass-border",
        "glass-highlight",
        "glass-shadow",
        "ink-primary",
        "ink-secondary",
        "ink-muted",
        "ink-inverse",
        "scanline-color",
        "scanline-gap",
        "font-display",
        "font-mono",
        "text-display",
        "text-xl",
        "text-lg",
        "text-base",
        "text-sm",
        "text-xs",
        "radius-sm",
        "radius-md",
        "radius-lg",
        "radius-xl",
        "radius-full",
        "radius-sharp",
        "glow-sm",
        "glow-md",
        "glow-lg",
        "glow-xl",
        "shadow-sm",
        "shadow-md",
        "shadow-lg",
        "shadow-glow",
        "duration-instant",
        "duration-fast",
        "duration-base",
        "duration-slow",
        "duration-scan",
        "duration-pulse",
        "ease-out",
        "ease-spring",
        "ease-smooth",
        "scanline-speed",
        "scanline-opacity",
        "cursor-trail-length",
        "cursor-glow-size",
        "cursor-glow-color",
    ]

    for token in required_tokens:
        assert token in CYBERPUNK_TOKENS, f"Missing token: {token}"


def test_cyberpunk_template_creation():
    """Verify Plotly template can be created with correct cyberpunk colors."""
    from dashboard.theme_cyberpunk import get_cyberpunk_template

    template = get_cyberpunk_template()
    assert template is not None
    assert template.layout.paper_bgcolor == "#050816"
    assert template.layout.plot_bgcolor == "#0d1428"
    assert template.layout.colorway[0] == "#00ffff"


def test_scanline_css_variables():
    """Verify scanline CSS variables are defined with correct values."""
    from dashboard.theme_cyberpunk import CYBERPUNK_TOKENS

    assert CYBERPUNK_TOKENS["scanline-speed"] == "8s"
    assert CYBERPUNK_TOKENS["scanline-opacity"] == "0.04"
    assert CYBERPUNK_TOKENS["scanline-gap"] == "4px"


def test_cyberpunk_color_values():
    """Verify key cyberpunk color values match design spec."""
    from dashboard.theme_cyberpunk import CYBERPUNK_TOKENS

    assert CYBERPUNK_TOKENS["void-base"] == "#050816"
    assert CYBERPUNK_TOKENS["neon-cyan"] == "#00ffff"
    assert CYBERPUNK_TOKENS["neon-magenta"] == "#ff00ff"
    assert CYBERPUNK_TOKENS["neon-green"] == "#39ff14"
    assert CYBERPUNK_TOKENS["neon-amber"] == "#ffcc00"
    assert CYBERPUNK_TOKENS["neon-red"] == "#ff1744"
    assert CYBERPUNK_TOKENS["neon-blue"] == "#00b4d8"


def test_cyberpunk_typography():
    """Verify typography tokens are correctly defined."""
    from dashboard.theme_cyberpunk import CYBERPUNK_TOKENS

    assert "Orbitron" in CYBERPUNK_TOKENS["font-display"]
    assert "JetBrains Mono" in CYBERPUNK_TOKENS["font-mono"]


def test_cyberpunk_space_scale():
    """Verify space scale tokens follow 4px base."""
    from dashboard.theme_cyberpunk import CYBERPUNK_TOKENS

    assert CYBERPUNK_TOKENS["space-1"] == "0.25rem"
    assert CYBERPUNK_TOKENS["space-4"] == "1rem"
    assert CYBERPUNK_TOKENS["space-8"] == "3rem"


def test_register_cyberpunk_theme():
    """Verify theme registration function exists and works."""
    import plotly.io as pio

    from dashboard.theme_cyberpunk import register_cyberpunk_theme

    register_cyberpunk_theme()
    assert "cyberpunk" in pio.templates
    assert pio.templates.default == "cyberpunk"


def test_protocol_color_mapping():
    """Verify protocol color mapping uses cyberpunk neon colors."""
    from dashboard.theme_cyberpunk import get_protocol_color_cyberpunk

    assert get_protocol_color_cyberpunk("TCP") == "#00ffff"
    assert get_protocol_color_cyberpunk("UDP") == "#ff00ff"
    assert get_protocol_color_cyberpunk("ICMP") == "#39ff14"
    assert get_protocol_color_cyberpunk("OTHER") == "#00b4d8"


def test_severity_color_mapping():
    """Verify severity color mapping uses cyberpunk neon colors."""
    from dashboard.theme_cyberpunk import get_severity_color_cyberpunk

    assert get_severity_color_cyberpunk("INFO") == "#00ffff"
    assert get_severity_color_cyberpunk("WARN") == "#ffcc00"
    assert get_severity_color_cyberpunk("CRITICAL") == "#ff1744"