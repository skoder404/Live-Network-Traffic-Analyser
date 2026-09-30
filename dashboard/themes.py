"""Multi-theme engine for LNTA dashboard.

Themes:
  1. Cyberpunk Neon  – dark background, neon accents
  2. Warm Amber      – cream/gold background, amber accents
  3. Mint Green      – light green background, mint/emerald accents
"""

from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

ASSETS = Path(__file__).parent / "assets"


def html_embed(html, height):
    """st.iframe on new Streamlit, components.html on older."""
    if hasattr(st, "iframe"):
        return st.iframe(html, height=max(height, 1))
    return components.html(html, height=height)


THEME_TOKENS = {
    "Cyberpunk Neon": {
        "bg": "#07060f",
        "panel": "rgba(18,15,36,.72)",
        "text": "#e8e6ff",
        "muted": "#9d98c9",
        "primary": "#ff2e88",
        "secondary": "#22e6ff",
        "accent1": "#b6ff3c",
        "accent2": "#ffb020",
        "danger": "#ff4d5e",
        "chart_colors": ["#22e6ff", "#ff2e88", "#b6ff3c", "#ffb020", "#ff4d5e"],
        "chart_area": "#22e6ff",
        "chart_bar": "#b6ff3c",
        "mode": "dark",
    },
    "Warm Amber": {
        "bg": "#FFFBF5",
        "panel": "#FFFFFF",
        "text": "#3D2B1F",
        "muted": "#8B7355",
        "primary": "#C17817",
        "secondary": "#D4930D",
        "accent1": "#A0522D",
        "accent2": "#CD853F",
        "danger": "#CC3333",
        "chart_colors": ["#C17817", "#D4930D", "#A0522D", "#CD853F", "#8B6914"],
        "chart_area": "#D4930D",
        "chart_bar": "#C17817",
        "mode": "light",
    },
    "Mint Green": {
        "bg": "#F0FFF4",
        "panel": "#FFFFFF",
        "text": "#1A3A2A",
        "muted": "#5A7D6A",
        "primary": "#2E8B57",
        "secondary": "#38A169",
        "accent1": "#276749",
        "accent2": "#48BB78",
        "danger": "#E53E3E",
        "chart_colors": ["#2E8B57", "#38A169", "#276749", "#48BB78", "#2D9C6F"],
        "chart_area": "#38A169",
        "chart_bar": "#2E8B57",
        "mode": "light",
    },
}

_GLOW = """<script>try{const d=window.parent.document;if(!d.getElementById('cg')&&!matchMedia('(prefers-reduced-motion: reduce)').matches){
const g=d.createElement('div');g.id='cg';g.setAttribute('aria-hidden','true');g.style.cssText='position:fixed;width:360px;height:360px;border-radius:50%;pointer-events:none;z-index:1;background:radial-gradient(circle,rgba(34,230,255,.13),transparent 65%);transform:translate(-50%,-50%);left:-999px;top:0';
d.body.appendChild(g);d.addEventListener('mousemove',e=>{g.style.left=e.clientX+'px';g.style.top=e.clientY+'px'})}}catch(e){}</script>"""


def get_tokens(name: str | None = None) -> dict:
    """Return theme design tokens."""
    if name is None:
        try:
            name = st.session_state.get("_lnta_theme", "Cyberpunk Neon")
        except Exception:
            name = "Cyberpunk Neon"
    return THEME_TOKENS.get(name, THEME_TOKENS["Cyberpunk Neon"])


def get_plotly_layout(name: str | None = None) -> dict:
    """Return a Plotly layout dict that adapts to the active theme.

    This sets transparent backgrounds and font/axis colours so that
    chart text (titles, axis labels, tick labels) is readable on every
    theme — dark *and* light.
    """
    t = get_tokens(name)
    return dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=t["text"]),
        title_font=dict(color=t["text"]),
        xaxis=dict(
            color=t["text"],
            tickfont=dict(color=t["text"]),
            title_font=dict(color=t["text"]),
            gridcolor=t["muted"] + "33",
        ),
        yaxis=dict(
            color=t["text"],
            tickfont=dict(color=t["text"]),
            title_font=dict(color=t["text"]),
            gridcolor=t["muted"] + "33",
        ),
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(font=dict(color=t["text"])),
    )


def apply_theme(name: str = "Cyberpunk Neon"):
    """Inject selected theme CSS into Streamlit."""
    css_map = {
        "Cyberpunk Neon": "cyberpunk.css",
        "Warm Amber": "warm_amber.css",
        "Mint Green": "mint_green.css",
    }
    css_file = css_map.get(name, "cyberpunk.css")
    css_path = ASSETS / css_file
    if css_path.exists():
        css = css_path.read_text()
        if name == "Cyberpunk Neon":
            st.markdown(
                f"<style>{css}</style><div class='scanlines' aria-hidden='true'></div>",
                unsafe_allow_html=True,
            )
            html_embed(_GLOW, 1)
        else:
            st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
