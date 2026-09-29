# Cyberpunk/Neon UI Implementation Plan — LNTA Dashboard

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven-development (recommended) or executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a professional Cyberpunk/Neon UI for the LNTA Dashboard with premium interactive effects, cursor glow trails, neon glow animations, scanline effects, and full WCAG 2.1 AA accessibility compliance.

**Architecture:** Streamlit dashboard with 6 tabs enhanced with Cyberpunk/Neon design system featuring neon glow effects, scanline overlays, magnetic cursor interactions, ambient ambient animations, and full WCAG 2.1 AA accessibility. All components compatible with `LNTA_MOCK=true` for development without pipeline.

**Tech Stack:** Streamlit 1.30+, Plotly 5.18+, pandas 3.0+, custom CSS (Cyberpunk/Neon theme), pytest + Streamlit AppTest, full WCAG 2.1 AA compliance.

---

## Design Brief

```
Purpose:    Real-time network traffic monitoring dashboard for NOC operators
Audience:   Security analysts, network engineers monitoring live traffic 24/7
Tone:       Cyberpunk/Neon — futuristic NOC, premium terminal aesthetic with neon glow
Reference:  Cyberpunk 2077 UI + Bloomberg Terminal density + VS Code dark theme
Palette:    Deep void base (#050816) / Neon cyan (#00ffff) / Hot pink (#ff00ff) / Electric green (#39ff14) / Warning amber (#ffcc00)
Type:       JetBrains Mono (data) + Orbitron (display) — technical precision + sci-fi display
Memorable:  Living scanline overlay that reacts to data velocity + magnetic cursor glow trail
Restraint:  No flat colors. No flat shadows. Every surface glows. No flat buttons. Scanlines always visible.
```

---

## Token Block — Cyberpunk/Neon Design System

```css
:root {
  /* ===== VOID BASE LAYER ===== */
  --void-base:        #050816;        /* Deep void background */
  --void-elevated:    #0a0f2a;        /* Elevated panels */
  --void-panel:       #0d1428;        /* Standard panels */
  --void-glass:       rgba(13, 20, 40, 0.7);  /* Glass panels */

  /* ===== NEON COLOR SYSTEM ===== */
  --neon-cyan:        #00ffff;        /* Primary: Data flow, primary actions */
  --neon-cyan-dim:    rgba(0, 255, 255, 0.3);
  --neon-cyan-glow:   rgba(0, 255, 255, 0.5);
  
  --neon-magenta:     #ff00ff;        /* Secondary: Alerts, highlights */
  --neon-magenta-dim: rgba(255, 0, 255, 0.3);
  --neon-magenta-glow: rgba(255, 0, 255, 0.5);
  
  --neon-green:       #39ff14;        /* Success: Healthy, good */
  --neon-green-dim:   rgba(57, 255, 20, 0.3);
  --neon-green-glow:  rgba(57, 255, 20, 0.5);
  
  --neon-amber:       #ffcc00;        /* Warning: Attention, caution */
  --neon-amber-dim:   rgba(255, 204, 0, 0.3);
  --neon-amber-glow:  rgba(255, 204, 0, 0.5);
  
  --neon-red:         #ff1744;        /* Critical: Danger, errors */
  --neon-red-dim:     rgba(255, 23, 68, 0.3);
  --neon-red-glow:    rgba(255, 23, 68, 0.5);
  
  --neon-blue:        #00b4d8;        /* Info: Auxiliary, links */
  --neon-blue-dim:    rgba(0, 180, 216, 0.3);

  /* ===== TEXT HIERARCHY ===== */
  --ink-primary:   #ffffff;        /* Primary text */
  --ink-secondary: #b8c5e0;        /* Secondary text */
  --ink-muted:     #6b7280;        /* Muted/meta text */
  --ink-inverse:   #050816;        /* Inverse text on neon */

  /* ===== SURFACE & GLASS ===== */
  --glass-bg:      rgba(13, 20, 40, 0.6);
  --glass-border:  rgba(0, 255, 255, 0.2);
  --glass-highlight: rgba(0, 255, 255, 0.05);
  --glass-shadow:  0 4px 32px rgba(0, 0, 0, 0.5), 
                    0 0 0 1px rgba(0, 255, 255, 0.1),
                    inset 0 1px 0 rgba(255, 255, 255, 0.05);

  /* ===== SCANLINE OVERLAY ===== */
  --scanline-color: rgba(0, 255, 255, 0.03);
  --scanline-gap: 4px;

  /* ===== TYPOGRAPHY ===== */
  --font-display: "Orbitron", "JetBrains Mono", "Fira Code", monospace;
  --font-mono:    "JetBrains Mono", "Fira Code", "Consolas", monospace;
  --text-display: clamp(1.5rem, 3vw, 2.5rem);
  --text-xl:      clamp(1.25rem, 2.5vw, 2rem);
  --text-lg:      clamp(1.125rem, 2vw, 1.25rem);
  --text-base:    1rem;
  --text-sm:      0.875rem;
  --text-xs:      0.75rem;
  --text-mono:    "JetBrains Mono", "Fira Code", monospace;

  /* ===== SPACE SCALE (4px base) ===== */
  --space-1: 0.25rem; --space-2: 0.5rem; --space-3: 0.75rem;
  --space-4: 1rem;    --space-5: 1.5rem; --space-6: 2rem;
  --space-8: 3rem;    --space-10: 4rem;

  /* ===== RADIUS ===== */
  --radius-sm:  4px;
  --radius-md:  8px;
  --radius-lg:  12px;
  --radius-xl:  16px;
  --radius-full: 9999px;
  --radius-sharp: 0;  /* For brutalist elements */

  /* ===== NEON GLOW SYSTEM ===== */
  --glow-sm:   0 0 4px currentColor, 0 0 8px currentColor;
  --glow-md:   0 0 8px currentColor, 0 0 16px currentColor, 0 0 32px currentColor;
  --glow-lg:   0 0 16px currentColor, 0 0 32px currentColor, 0 0 64px currentColor;
  --glow-xl:   0 0 32px currentColor, 0 0 64px currentColor, 0 0 128px currentColor;

  /* ===== SHADOWS ===== */
  --shadow-sm:  0 2px 8px rgba(0,0,0,0.4);
  --shadow-md:  0 4px 24px rgba(0,0,0,0.5);
  --shadow-lg:  0 8px 48px rgba(0,0,0,0.6);
  --shadow-glow: 0 0 0 1px currentColor, var(--glow-md);

  /* ===== MOTION ===== */
  --duration-instant: 50ms;
  --duration-fast:  100ms;
  --duration-base:  200ms;
  --duration-slow:  300ms;
  --duration-scan:  8s;
  --duration-pulse: 2s;
  --ease-out:    cubic-bezier(0.16, 1, 0.3, 1);
  --ease-spring: cubic-bezier(0.34, 1.56, 0.64, 1);
  --ease-smooth: cubic-bezier(0.16, 1, 0.3, 1);

  /* ===== SCANLINE ANIMATION ===== */
  --scanline-speed: 8s;
  --scanline-opacity: 0.04;

  /* ===== CURSOR GLOW TRAIL ===== */
  --cursor-trail-length: 20;
  --cursor-glow-size: 300px;
  --cursor-glow-color: #00ffff;
}

/* ===== SCANLINE OVERLAY ===== */
.scanlines {
  position: fixed;
  inset: 0;
  pointer-events: none;
  z-index: 9999;
  opacity: var(--scanline-opacity);
  background: repeating-linear-gradient(
    0deg,
    transparent,
    transparent calc(var(--scanline-gap) / 2),
    var(--scanline-color) calc(var(--scanline-gap) / 2),
    var(--scanline-color) var(--scanline-gap)
  );
  background-size: 100% var(--scanline-gap);
  animation: scanline-move var(--scanline-speed) linear infinite;
  pointer-events: none;
}

@keyframes scanline-move {
  0% { background-position: 0 0; }
  100% { background-position: 0 calc(var(--scanline-gap) * 2); }
}

/* ===== CURSOR GLOW TRAIL ===== */
.cursor-glow-trail {
  position: fixed;
  pointer-events: none;
  z-index: 10000;
  width: var(--cursor-glow-size);
  height: var(--cursor-glow-size);
  border-radius: 50%;
  background: radial-gradient(circle at center, 
    var(--cursor-glow-color) 0%, 
    rgba(0, 255, 255, 0.2) 40%, 
    transparent 70%
  );
  filter: blur(20px);
  transform: translate(-50%, -50%);
  pointer-events: none;
  transition: transform 0.05s linear, opacity 0.1s ease-out;
  opacity: 0.6;
  pointer-events: none;
  z-index: 9999;
}

.cursor-glow-trail::before {
  content: '';
  position: absolute;
  inset: -2px;
  border-radius: 50%;
  border: 2px solid var(--neon-cyan);
  opacity: 0.5;
  animation: cursor-pulse 1.5s ease-in-out infinite;
}

@keyframes cursor-pulse {
  0%, 100% { transform: scale(1); opacity: 0.5; }
  50% { transform: scale(1.1); opacity: 0.8; }
}
```

---

## File Structure Map

```
dashboard/
├── app.py                          # Entry point (modify: cyberpunk theme, st.fragment)
├── components/
│   ├── __init__.py
│   ├── header.py                   # MODIFY: Neon KPI cards, live pulse
│   ├── live_overview.py            # MODIFY: Scanline charts, neon charts
│   ├── stream_analytics.py         # MODIFY: Neon concept cards
│   ├── link_analysis.py            # MODIFY: Neon graph, scanline graph
│   ├── alerts.py                   # MODIFY: Neon alert cards, pulse
│   ├── history.py                  # MODIFY: Cyberpunk table styling
│   ├── pipeline.py                 # MODIFY: Neon stage indicators
│   └── __init__.py
├── data.py                         # MODIFY: Neon query helpers
├── theme.py                        # REPLACE: Cyberpunk/Neon Plotly template
├── theme_cyberpunk.py              # CREATE: Cyberpunk token block + CSS
├── assets/
│   ├── theme.css                   # REPLACE: Full cyberpunk theme
│   └── cyberpunk.css               # CREATE: Cyberpunk-specific styles
├── .streamlit/config.toml          # VERIFY: Cyberpunk theme config
└── tests/
    ├── test_cyberpunk_theme.py     # CREATE: Theme tests
    ├── test_dashboard.py           # MODIFY: Cyberpunk AppTest
    └── test_components.py          # MODIFY: Component tests
```

---

## Task 1: Cyberpunk/Neon Token Block & Base Theme

### Files:
- Create: `dashboard/theme_cyberpunk.py`
- Create: `dashboard/assets/cyberpunk.css`
- Modify: `dashboard/theme.py` (replace)
- Modify: `dashboard/assets/theme.css` (replace)
- Modify: `dashboard/theme.py` (update imports)

### Step 1: Write Failing Test for Theme Tokens

```python
# tests/unit/test_cyberpunk_theme.py
import pytest
from dashboard.theme_cyberpunk import CYBERPUNK_TOKENS, get_cyberpunk_template

def test_cyberpunk_tokens_exist():
    """Verify all cyberpunk tokens are defined."""
    required_tokens = [
        'void-base', 'void-elevated', 'void-panel', 'void-glass',
        'neon-cyan', 'neon-magenta', 'neon-green', 'neon-amber', 'neon-red',
        'neon-blue', 'neon-cyan-glow', 'neon-magenta-glow',
        'glass-bg', 'glass-border', 'glass-shadow',
        'neon-cyan', 'neon-magenta', 'neon-green', 'neon-amber', 'neon-red',
        'scanline-color', 'scanline-gap',
        'font-display', 'font-mono', 'text-display', 'text-xl', 'text-base',
        'radius-sm', 'radius-md', 'radius-lg', 'radius-xl',
        'glow-sm', 'glow-md', 'glow-lg', 'glow-xl',
        'duration-fast', 'duration-base', 'duration-slow', 'duration-scan',
        'ease-spring', 'ease-smooth', 'scanline-speed', 'scanline-opacity',
    ]
    
    for token in required_tokens:
        assert token in CYBERPUNK_TOKENS, f"Missing token: {token}"

def test_cyberpunk_template_creation():
    """Verify Plotly template can be created."""
    template = get_cyberpunk_template()
    assert template is not None
    assert template.layout.paper_bgcolor == "#050816"
    assert template.layout.plot_bgcolor == "#0d1428"
    assert template.layout.colorway[0] == "#00ffff"

def test_scanline_css_variables():
    """Verify scanline CSS variables are defined."""
    from dashboard.theme_cyberpunk import CYBERPUNK_TOKENS
    assert CYBERPUNK_TOKENS['scanline-speed'] == '8s'
    assert CYBERPUNK_TOKENS['scanline-opacity'] == '0.04'
    assert CYBERPUNK_TOKENS['scanline-gap'] == '4px'
```

### Step 2: Run Test to Verify Failure

Run: `pytest tests/unit/test_cyberpunk_theme.py -v`
Expected: FAIL — module/theme_cyberpunk.py not found

### Step 3: Create Cyberpunk Token Block

Create `dashboard/theme_cyberpunk.py` with the full token block from the Design Brief above.

### Step 4: Create Cyberpunk CSS File

Create `dashboard/assets/cyberpunk.css` with:
- Scanline overlay system
- Cursor glow trail system
- Neon glow utilities
- Glass panel system
- Cyberpunk component states (5 states each)
- Scanline animation
- Cursor glow trail
- Reduced motion support

### Step 5: Update theme.py to Export Cyberpunk Template

```python
# dashboard/theme.py
from .theme_cyberpunk import (
    CYBERPUNK_TOKENS,
    get_cyberpunk_template,
    register_cyberpunk_theme,
    get_protocol_color_cyberpunk,
    get_severity_color_cyberpunk,
)

# Re-export for backwards compatibility
LNTA_COLORS = CYBERPUNK_TOKENS
PROTOCOL_COLOR_MAP = {...}
SEVERITY_COLORS = {...}
CATEGORICAL_PALETTE = [...]

def get_lnta_dark_template():
    """Alias for cyberpunk template."""
    return get_cyberpunk_template()

def register_lnta_theme():
    register_cyberpunk_theme()
```

### Step 5: Replace theme.css with Cyberpunk Version

Update `dashboard/assets/theme.css` with full cyberpunk theme including:
- CSS custom properties from token block
- Scanline overlay system
- Cursor glow trail
- Glass panel system
- Neon button states (5 states)
- Card states (5 states)
- Input states (5 states)
- Scanline animation
- Cursor glow trail
- Reduced motion support

### Step 6: Run Tests to Verify Pass

Run: `pytest tests/unit/test_cyberpunk_theme.py -v`
Expected: PASS

### Step 7: Commit

```bash
git add dashboard/theme_cyberpunk.py dashboard/assets/cyberpunk.css dashboard/theme.py dashboard/assets/theme.css tests/unit/test_cyberpunk_theme.py
git commit -m "feat(theme): Cyberpunk/Neon design system with neon glow, scanlines, cursor glow trail

- Full Cyberpunk/Neon token block (colors, typography, spacing, motion, glow)
- Scanline overlay with animated scanlines
- Cursor glow trail with magnetic trail effect
- Glass panel system with neon borders
- Component state system (5 states each)
- Scanline animation + cursor glow trail
- Reduced motion support
- Plotly cyberpunk template
- Ruff clean"
```

---

## Task 2: Dashboard App & Components — Cyberpunk Enhancement

### Files:
- Modify: `dashboard/app.py` (cyberpunk theme, scanlines, cursor glow)
- Modify: `dashboard/components/header.py` (neon KPI cards)
- Modify: `dashboard/components/live_overview.py` (scanline charts)
- Modify: `dashboard/components/stream_analytics.py` (neon concept cards)
- Modify: `dashboard/components/link_analysis.py` (neon graph)
- Modify: `dashboard/components/alerts.py` (neon alert cards)
- Modify: `dashboard/components/history.py` (cyberpunk tables)
- Modify: `dashboard/components/pipeline.py` (neon pipeline)
- Modify: `dashboard/components/stream_analytics.py` (neon cards)

### Step 1: Write Failing Tests for Cyberpunk Components

```python
# tests/unit/test_components_cyberpunk.py
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

def test_cyberpunk_header_renders_neon_kpi():
    """Header renders neon KPI cards with glow effects."""
    from dashboard.components.header import render_header
    
    db_health = {"connected": True, "table_status": {"window_metrics": "2026-09-27T12:00:00Z"}}
    
    mock_col = MagicMock()
    mock_col.__enter__ = Mock(return_value=mock_col)
    mock_col.__exit__ = Mock(return_value=False)
    
    with patch("streamlit.columns") as mock_columns, \
         patch("streamlit.markdown") as mock_markdown, \
         patch("streamlit.caption"):
        
        mock_columns.return_value = [Mock() for _ in range(10)]
        
        from dashboard.components.header import render_header
        render_header(db_health, "LIVE")
        
        # Verify neon KPI cards rendered
        md_calls = [str(c) for c in mock_markdown.call_args_list]
        assert any("neon-kpi" in str(c) or "kpi-card" in str(c) for c in md_calls)

def test_cyberpunk_live_overview_neon_charts():
    """Live Overview renders with neon scanline charts."""
    from dashboard.components.live_overview import render_live_overview_tab
    
    mock_db = Mock()
    mock_db.get_latest_window_metrics.return_value = pd.DataFrame({
        "window_start": pd.date_range("2026-09-27", periods=5, freq="10s"),
        "pps": [100, 120, 110, 130, 125],
        "bps": [10000, 12000, 11000, 13000, 12500],
    })
    # ... mock other methods
    
    with patch("streamlit.plotly_chart") as mock_plotly:
        render_live_overview_tab(mock_db, controls)
        # Verify neon chart styling
        assert mock_plotly.call_count >= 3
```

### Step 2: Run Tests → Fail

Run: `pytest tests/unit/test_components_cyberpunk.py -v`
Expected: FAIL — cyberpunk components not implemented

### Step 3: Implement Cyberpunk Components

**Update `dashboard/components/header.py`:**
- Neon KPI cards with glow effects
- Delta indicators with neon colors
- Mini sparklines with neon glow
- Live pulse with cyberpunk animation

**Update `dashboard/components/live_overview.py`:**
- Dual-axis chart with neon cyan/magenta traces
- Scanline overlay on charts
- Protocol donut with neon colors
- Top ports bar with neon glow
- Decay score with neon green/amber

**Update `dashboard/components/stream_analytics.py`:**
- 7 concept cards with cyberpunk styling
- Neon glow on hover
- Exact vs approximate with neon colors

**Update `dashboard/components/link_analysis.py`:**
- Cyberpunk graph with neon edges
- PageRank table with neon glow
- Markov heatmap with cyberpunk colors
- Scanline overlay on graph

**Update `dashboard/components/alerts.py`:**
- Neon alert cards with pulse animation
- Severity-based neon colors (cyan/magenta/red)
- Timeline with scanline effect

**Update `dashboard/components/link_analysis.py`:**
- Neon graph with animated edges
- PageRank table with neon glow

**Update `dashboard/components/history.py`:**
- Cyberpunk table with scanlines
- Neon query selector

**Update `dashboard/components/pipeline.py`:**
- Neon stage indicators
- Scanline charts for batch duration

### Step 3: Run Tests → Pass

Run: `pytest tests/unit/test_components_cyberpunk.py -v`
Expected: PASS

### Step 3: Commit

```bash
git add dashboard/components/*.py dashboard/assets/cyberpunk.css tests/unit/test_components_cyberpunk.py
git commit -m "feat(ui): Cyberpunk/Neon dashboard components with neon glow, scanlines

- Neon KPI cards with glow, delta, sparklines
- Scanline charts with animated scanlines
- 7 cyberpunk concept cards for Stream Analytics
- Neon graph with animated edges, PageRank, Markov
- Cyberpunk alert cards with pulse animation
- Cyberpunk history table with scanlines
- Neon pipeline stage indicators
- Ruff clean"
```

---

## Task 3: Dashboard App — Cyberpunk Integration

### Files:
- Modify: `dashboard/app.py`
- Modify: `dashboard/theme.py` (cyberpunk template)

### Step 1: Write Failing Test

```python
# tests/unit/test_dashboard_cyberpunk.py
def test_cyberpunk_app_loads():
    """App loads with cyberpunk theme and scanlines."""
    at = AppTest.from_file(str(APP_PATH)).run(timeout=15)
    assert not at.exception
    
    # Check for scanline overlay
    html = at.get("html")
    assert any("scanlines" in str(el) for el in html)
    
    # Check for cursor glow trail
    assert any("cursor-glow" in str(el) for el in html)
```

### Step 2: Run Test → Fail

### Step 3: Update `app.py` with Cyberpunk Integration

```python
# dashboard/app.py - Key additions
import streamlit as st
from dashboard.theme import register_cyberpunk_theme

# Inject cyberpunk CSS and scanlines
st.markdown("""
<div class="scanlines" id="scanlines"></div>
<div class="cursor-glow-trail" id="cursor-glow"></div>
<script>
  // Cursor glow trail
  const trail = document.getElementById('cursor-glow');
  document.addEventListener('mousemove', (e) => {
    trail.style.transform = `translate(${e.clientX}px, ${e.clientY}px) translate(-50%, -50%)`;
  });
</script>
""", unsafe_allow_html=True)

# Register cyberpunk theme
register_cyberpunk_theme()

# Page config with cyberpunk theme
st.set_page_config(
    page_title="LNTA — Cyberpunk NOC",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sidebar with cyberpunk controls
with st.sidebar:
    st.markdown("### ⚡ CYBERPUNK NOC")
    window_len = st.select_slider("⏱ WINDOW", [10, 30, 60], value=10, format_func=lambda x: f"{x}s")
    refresh = st.slider("🔄 REFRESH (s)", 1, 30, 3)
    # ... protocol filter, top-N
```

### Step 3: Run Test → Pass

### Step 4: Commit

---

## Task 3: Accessibility & States (WCAG 2.1 AA)

### Files:
- Modify: `dashboard/assets/cyberpunk.css` (add accessibility styles)
- Modify: `dashboard/components/*.py` (add ARIA, focus-visible, live regions)

### Step 1: Write Failing Test

```python
def test_cyberpunk_accessibility():
    """Verify WCAG 2.1 AA compliance."""
    from dashboard.components.header import render_header
    
    with patch("streamlit.markdown") as mock_md:
        render_header(db_health, "LIVE")
        html = str(mock_md.call_args_list)
        
        # Check for focus-visible
        assert "focus-visible" in str(mock_markdown.call_args_list)
        
        # Check for ARIA labels
        assert "aria-label" in str(mock_markdown.call_args_list) or "aria-labelledby" in str(mock_markdown.call_args_list)
        
        # Check for focus-visible CSS
        assert "focus-visible" in open("dashboard/assets/cyberpunk.css").read()
        
        # Check reduced motion
        assert "prefers-reduced-motion" in open("dashboard/assets/cyberpunk.css").read()
```

### Step 2: Add Accessibility Styles to CSS

```css
/* Focus visible for all interactive elements */
*:focus-visible {
  outline: 2px solid var(--neon-cyan) !important;
  outline-offset: 2px !important;
  box-shadow: 0 0 0 2px var(--bg-base), 0 0 0 4px var(--neon-cyan) !important;
}

/* Reduced motion */
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    animation-iteration-count: 1 !important;
    transition-duration: 0.01ms !important;
  }
  .scanlines { animation: none !important; }
  .cursor-glow-trail { animation: none !important; }
}

/* Live regions for async updates */
.sr-only {
  position: absolute; width: 1px; height: 1px; padding: 0;
  margin: -1px; overflow: hidden; clip-path: inset(50%); white-space: nowrap;
}

/* Live region for alerts */
[aria-live="polite"], [role="status"] { /* styles */ }
[role="alert"] { /* styles */ }

/* High contrast mode support */
@media (prefers-contrast: high) {
  :root {
    --neon-cyan: #00ffff;
    --neon-magenta: #ff00ff;
    --neon-green: #39ff14;
    --neon-amber: #ffcc00;
    --neon-red: #ff1744;
  }
}

/* Focus visible on all interactive */
button:focus-visible, 
.stButton > button:focus-visible,
.stSelectbox:focus-visible,
.stSlider:focus-visible,
.stTextInput:focus-visible {
  outline: 2px solid var(--neon-cyan) !important;
  outline-offset: 2px !important;
}
```

### Step 2: Add ARIA to Components

- Add `role="status"` to loading spinners
- Add `aria-live="polite"` to live data updates
- Add `role="alert"` to critical alerts
- Add `aria-label` to icon-only buttons
- Add `aria-expanded` to collapsible sections
- Add `aria-controls` to tabs

### Step 3: Run Tests → Pass

### Step 4: Commit

```bash
git add dashboard/assets/cyberpunk.css dashboard/components/*.py tests/unit/test_accessibility_cyberpunk.py
git commit -m "feat(a11y): WCAG 2.1 AA compliance for Cyberpunk UI

- Focus-visible on all interactive elements
- Reduced motion support
- ARIA labels, live regions, roles
- High contrast mode support
- Keyboard navigation support
- Live regions for async updates
- Ruff clean"
```

---

## Task 4: Auto-Refresh & Multi-Window (P9, P10)

### Files:
- Modify: `dashboard/app.py` (st.fragment auto-refresh)
- Modify: `dashboard/components/*.py` (respect window_len_s)

### Step 1: Write Failing Tests

```python
def test_cyberpunk_auto_refresh():
    """Verify st.fragment auto-refresh works."""
    at = AppTest.from_file(str(APP_PATH)).run(timeout=15)
    assert not at.exception
    # Check for fragment elements
    assert len(at.fragment) > 0

def test_cyberpunk_multi_window():
    """Test multi-window toggle."""
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    window_select = at.sidebar.selectbox[0]
    window_select.select("30s").run()
    assert not at.exception
```

### Step 2: Implement Auto-Refresh

```python
# dashboard/app.py
@st.fragment(run_every=cfg.get("refresh_interval", 3))
def render_live_overview_tab(db, controls): ...

@st.fragment(run_every=cfg.get("refresh_interval", 3))
def render_stream_analytics_tab(db, controls): ...
```

### Step 3: Add Multi-Window to Sidebar

```python
# dashboard/app.py
window_len = st.select_slider("⏱ WINDOW", [10, 30, 60], value=10, format_func=lambda x: f"{x}s")
```

### Step 3: Run Tests → Pass

### Step 4: Commit

---

## Task 5: Dashboard Tests (AppTest + Component Tests)

### Files:
- Modify: `tests/unit/test_dashboard.py`
- Modify: `tests/unit/test_components.py`

### Tests to Add:

```python
# tests/unit/test_dashboard_cyberpunk.py
def test_cyberpunk_app_loads():
    at = AppTest.from_file(str(APP_PATH)).run(timeout=15)
    assert not at.exception
    
    # Check cyberpunk elements
    markdown = " ".join([m.value for m in at.markdown])
    assert "CYBERPUNK" in markdown.upper() or "NEON" in markdown.upper()

def test_cyberpunk_sidebar_controls():
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    assert not at.exception
    
    # Check sidebar widgets
    widget_labels = [w.label for w in at.sidebar.selectbox + at.sidebar.slider + at.sidebar.multiselect]
    assert any("WINDOW" in l.upper() for l in widget_labels)
    assert any("REFRESH" in l.upper() for l in widget_labels)

def test_cyberpunk_mock_mode():
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    assert not at.exception
    markdown = " ".join([m.value for m in at.markdown])
    assert "DEMO" in markdown or "MOCK" in markdown or "NEON" in markdown

def test_cyberpunk_all_tabs_render():
    at = AppTest.from_file(str(APP_PATH)).run(timeout=15)
    tab_labels = [tab.label for tab in at.tabs]
    expected = ["LIVE OVERVIEW", "STREAM ANALYTICS", "LINK ANALYSIS", "ALERTS", "HISTORY", "PIPELINE"]
    for label in expected:
        assert any(label in t.upper() for t in tab_labels)
```

### Step 2: Run Tests → Pass

### Step 3: Commit

---

## Task 6: Verification & Final Integration

### Step 1: Full Test Suite

```bash
cd /mnt/Project/bda/Live-Network-Traffic-Analyser
PYTHONPATH=. python -m pytest tests/unit/test_cyberpunk_theme.py tests/unit/test_components_cyberpunk.py tests/unit/test_dashboard_cyberpunk.py tests/unit/test_accessibility_cyberpunk.py -v
```

Expected: **All tests pass**

### Step 2: Lint Check

```bash
ruff check dashboard/ linkanalysis/ tests/
```

Expected: **Clean** (only acceptable BLE001)

### Step 3: Accessibility Audit

```bash
npx @axe-core/cli http://localhost:8501 --exit
```

Expected: **0 violations**

### Step 4: Manual Verification

```bash
export LNTA_MOCK=true
streamlit run dashboard/app.py --server.port=8501
```

Verify in browser:
- [ ] All 6 tabs render with cyberpunk theme
- [ ] Scanlines visible and animated
- [ ] Cursor glow trail follows mouse
- [ ] Neon glow on hover/focus
- [ ] Auto-refresh works (3s default)
- [ ] Multi-window toggle (10/30/60s) works
- [ ] Keyboard navigation works (Tab, Enter, Esc)
- [ ] Focus visible on all elements
- [ ] Reduced motion respected
- [ ] Scanlines animate
- [ ] Cursor glow trail follows mouse

### Step 5: Final Commit

```bash
git add -A
git commit -m "feat(ui): Cyberpunk/Neon UI complete with premium effects

- Cyberpunk/Neon token block with neon colors, scanlines, cursor glow
- Full component suite: 6 tabs with neon glow, scanlines, magnetic hover
- Scanline overlay animation (8s loop, 4px gap)
- Cursor glow trail with magnetic trail effect
- Neon glow on hover/focus with spring physics
- Auto-refresh via st.fragment (configurable interval)
- Multi-window support (10s/30s/60s)
- WCAG 2.1 AA: focus-visible, reduced-motion, ARIA, live regions
- Scanline overlay animation (8s loop)
- Cursor glow trail with magnetic trail effect
- 49 unit tests + 4 AppTest smoke tests passing
- Axe-core accessibility: 0 violations
- Ruff clean
- Ready for M3 demo"
```

---

## Verification Checklist

| Check | Command | Expected |
|-------|---------|----------|
| Unit tests | `pytest tests/unit/test_linkanalysis.py tests/unit/test_alerts.py tests/unit/test_components.py tests/unit/test_dashboard.py tests/unit/test_cyberpunk_theme.py -v` | 53+ passed |
| Lint | `ruff check dashboard/ linkanalysis/ tests/` | Clean |
| Accessibility | `npx @axe-core/cli http://localhost:8501 --exit` | 0 violations |
| Manual UI | `streamlit run dashboard/app.py` | All 6 tabs, scanlines, cursor glow, auto-refresh |

---

## Execution Handoff

**Plan complete and saved to `docs/plans/2026-09-27-cyberpunk-neon-ui.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh sub-agent per task using the `task` tool, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
