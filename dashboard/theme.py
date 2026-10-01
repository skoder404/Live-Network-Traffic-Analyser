from pathlib import Path
import streamlit as st
import streamlit.components.v1 as components

def html_embed(html, height):
    """st.iframe on new Streamlit (components.html is deprecated), components.html on older."""
    if hasattr(st, "iframe"): return st.iframe(html, height=max(height, 1))
    return components.html(html, height=height)
from dashboard.theme_cyberpunk import TOKENS, FONTS, SEVERITY  # noqa: F401
CSS = Path(__file__).parent / "assets" / "cyberpunk.css"
GLOW = """<script>try{const d=window.parent.document;if(!d.getElementById('cg')&&!matchMedia('(prefers-reduced-motion: reduce)').matches){
const g=d.createElement('div');g.id='cg';g.setAttribute('aria-hidden','true');g.style.cssText='position:fixed;width:360px;height:360px;border-radius:50%;pointer-events:none;z-index:1;background:radial-gradient(circle,rgba(34,230,255,.13),transparent 65%);transform:translate(-50%,-50%);left:-999px;top:0';
d.body.appendChild(g);d.addEventListener('mousemove',e=>{g.style.left=e.clientX+'px';g.style.top=e.clientY+'px'})}}catch(e){}</script>"""
def apply_theme():
    st.markdown(f"<style>{CSS.read_text()}</style><div class='scanlines' aria-hidden='true'></div>", unsafe_allow_html=True)
    html_embed(GLOW, 1)
