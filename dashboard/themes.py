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
        "mode": "light",
    },
}

_GLOW = """<script>try{const d=window.parent.document;if(!d.getElementById('cg')&&!matchMedia('(prefers-reduced-motion: reduce)').matches){
const g=d.createElement('div');g.id='cg';g.setAttribute('aria-hidden','true');g.style.cssText='position:fixed;width:360px;height:360px;border-radius:50%;pointer-events:none;z-index:1;background:radial-gradient(circle,rgba(34,230,255,.13),transparent 65%);transform:translate(-50%,-50%);left:-999px;top:0';
d.body.appendChild(g);d.addEventListener('mousemove',e=>{g.style.left=e.clientX+'px';g.style.top=e.clientY+'px'})}}catch(e){}</script>"""


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
