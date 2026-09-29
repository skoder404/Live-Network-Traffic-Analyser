# M3 Polish Implementation Plan — Option 1: Polish First

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven-development (recommended) or executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete M3 Polish (Sep 28) deliverables: Dashboard states & accessibility (WCAG 2.1 AA), auto-refresh, multi-window support, dashboard tests. All work compatible with `LNTA_MOCK=true` — no pipeline dependencies.

**Architecture:** Streamlit dashboard with 6 tabs, ServingDB read layer, Plotly charts, custom CSS theme. All components already implemented with mock data; polish adds states, accessibility, and UX refinements.

**Tech Stack:** Streamlit 1.30+, Plotly 5.18+, pandas 3.0+, custom CSS theme (`dashboard/assets/theme.css`), pytest + Streamlit AppTest.

---

## File Structure Map

```
dashboard/
├── app.py                          # Entry point (modify: st.fragment auto-refresh)
├── components/
│   ├── __init__.py
│   ├── header.py                   # MODIFY: real KPI data, delta, sparkline
│   ├── live_overview.py            # MODIFY: add empty/error/stale states
│   ├── stream_analytics.py         # MODIFY: add empty/error/stale states
│   ├── link_analysis.py            # MODIFY: add empty/error/stale states
│   ├── alerts.py                   # MODIFY: add empty/error/stale states
│   ├── history.py                  # MODIFY: add empty/error/stale states
│   ├── pipeline.py                 # MODIFY: add empty/error/stale states
│   └── __init__.py
├── data.py                         # MODIFY: add stale detection helpers
├── theme.py                        # MODIFY: verify contrast tokens
├── assets/theme.css                # MODIFY: add state styles, focus-visible, reduced-motion
├── .streamlit/config.toml          # VERIFY: theme config
└── tests/
    ├── test_dashboard.py           # ADD: AppTest smoke tests
    └── test_components.py          # ADD: component unit tests
```

---

## Task 1: Accessibility & States Foundation (P8)

### Files:
- Modify: `dashboard/assets/theme.css`
- Modify: `dashboard/components/*.py` (all 6 components)
- Modify: `dashboard/data.py`

### Step 1: Write Failing Tests for States

```python
# tests/unit/test_components.py - add these tests
def test_live_overview_shows_loading_state():
    """Live Overview shows spinner while loading."""
    from dashboard.components.live_overview import render_live_overview_tab
    from dashboard.data import ServingDB
    from dashboard.theme import register_lnta_theme
    import time

    register_lnta_theme()
    mock_db = Mock()

    def slow_query(*args, **kwargs):
        time.sleep(0.1)
        return pd.DataFrame()

    mock_db.get_latest_window_metrics.side_effect = slow_query
    # ... mock other methods returning empty DataFrames

    controls = {"window_len_s": 10}
    mock_col = MagicMock()
    mock_col.__enter__ = Mock(return_value=mock_col)
    mock_col.__exit__ = Mock(return_value=False)

    with (
        patch("streamlit.spinner") as mock_spinner,
        patch("streamlit.columns") as mock_columns,
        patch("streamlit.plotly_chart"),
    ):
        mock_columns.return_value = [mock_col, mock_col]
        mock_spinner.return_value.__enter__ = Mock(return_value=None)
        mock_spinner.return_value.__exit__ = Mock(return_value=False)

        render_live_overview_tab(mock_db, controls)
        mock_spinner.assert_called()  # Loading state triggered
```

### Step 2: Run Test to Verify Failure

Run: `pytest tests/unit/test_components.py::test_live_overview_shows_loading_state -v`
Expected: FAIL — loading state not implemented

### Step 3: Add CSS State Classes to `theme.css`

```css
/* Loading skeleton */
.skeleton {
  background: linear-gradient(90deg, var(--bg-panel) 25%, var(--bg-elevated) 50%, var(--bg-panel) 75%);
  background-size: 200% 100%;
  animation: shimmer 1.2s infinite;
  border-radius: 8px;
}
@keyframes shimmer {
  0% { background-position: 200% 0; }
  100% { background-position: -200% 0; }
}

/* Empty state */
.empty-state {
  display: flex; flex-direction: column; align-items: center; justify-content: center;
  padding: 48px 24px; color: var(--text-muted); text-align: center;
}
.empty-state-icon { font-size: 48px; margin-bottom: 16px; opacity: 0.5; }

/* Error state */
.error-state { border: 2px solid var(--critical); border-radius: 8px; padding: 16px; }

/* Stale data warning */
.stale-badge { 
  background: rgba(var(--warn-rgb), 0.15); color: var(--warn); 
  border: 1px solid var(--warn); padding: 4px 10px; border-radius: 9999px; 
  font-size: 12px; font-weight: 500; font-family: 'JetBrains Mono', monospace;
}

/* Focus visible for keyboard navigation */
*:focus-visible {
  outline: 2px solid var(--accent) !important;
  outline-offset: 2px !important;
}

/* Reduced motion */
@media (prefers-reduced-motion: reduce) {
  .live-pulse { animation: none; }
  .kpi-card, .panel-card { transition: none; }
  .skeleton { animation: none; }
}
```

### Step 4: Add State Rendering to All Components

Each component needs 4 state handlers:
- **Loading**: `with st.spinner("Loading..."):` wrapper
- **Empty**: Render `.empty-state` with icon + message
- **Error**: Try/except → `st.error()` + `.error-state` card
- **Stale**: Check data timestamp > threshold → show warning badge

### Step 5: Run Tests to Verify Pass

Run: `pytest tests/unit/test_components.py -v`
Expected: All 5 component tests PASS

### Step 6: Verify Accessibility (WCAG 2.1 AA)

Run: `npx @axe-core/cli http://localhost:8501 --exit`
Expected: Zero violations

### Step 7: Commit

```bash
git add dashboard/assets/theme.css dashboard/components/*.py dashboard/data.py tests/unit/test_components.py
git commit -m "feat(dashboard): add loading/empty/error/stale states + WCAG 2.1 AA accessibility

- Added CSS: .skeleton, .empty-state, .error-state, .stale-badge, focus-visible, reduced-motion
- All 6 components: loading spinners, empty states, error handling, stale warnings
- WCAG 2.1 AA: focus-visible, reduced-motion, contrast tokens, ARIA labels
- 5 new component tests verifying state rendering
- Ruff clean"
```

---

## Task 2: Header/KPI Real Data + Delta + Sparkline (P7)

### Files:
- Modify: `dashboard/components/header.py`
- Modify: `dashboard/data.py` (add KPI query functions)

### Step 1: Write Failing Test

```python
# tests/unit/test_components.py
def test_render_header_shows_real_kpi_with_delta():
    """Header shows real KPI values with delta and sparkline."""
    from dashboard.components.header import render_header
    from dashboard.data import get_latest_window_metrics

    mock_db = Mock()
    # Mock real data with previous window for delta calculation
    mock_db.get_latest_window_metrics.return_value = pd.DataFrame(
        {
            "window_start": pd.date_range("2026-09-24", periods=2, freq="10s"),
            "pps": [100, 120],
            "bps": [10000, 12000],
            "packets": [100, 120],
            "bytes": [10000, 12000],
        }
    )

    with patch("streamlit.columns"), patch("streamlit.markdown") as mock_md:
        render_header({"connected": True, "table_status": {}}, "LIVE")
        # Verify KPI cards render with values and deltas
        kpi_calls = [c for c in mock_md.call_args_list if "kpi-value" in str(c)]
        assert len(kpi_calls) == 6
        # Verify delta indicators present
```

### Step 2: Run Test → Fail

Run: `pytest tests/unit/test_components.py::test_render_header_shows_real_kpi_with_delta -v`

### Step 3: Add Real KPI Queries to `data.py`

```python
# Add to dashboard/data.py
def get_kpi_sparkline(db: ServingDB, metric: str, window_len_s: int = 10, limit: int = 60) -> list:
    """Get last N values for sparkline."""
    query = _build_query(
        "window_metrics",
        f"window_start, {metric}",
        where="window_len_s = ?",
        order_by="window_start DESC",
        limit=limit,
    )
    df = db.query_cached_df(query, (window_len_s,))
    return df[metric].tolist()[::-1]  # Reverse for chronological
```

### Step 4: Update `header.py` to Use Real Data

- Replace hardcoded KPI cards with dynamic rendering
- Add delta calculation: `(current - previous) / previous * 100`
- Add sparkline via Plotly (mini chart, height 32px)

### Step 5: Run Test → Pass

Run: `pytest tests/unit/test_components.py::test_render_header_shows_real_kpi_with_delta -v`

### Step 6: Commit

```bash
git add dashboard/components/header.py dashboard/data.py tests/unit/test_components.py
git commit -m "feat(dashboard): Header/KPI real data with delta + sparkline

- Added get_kpi_sparkline() query function
- Header renders real KPI values with delta% and mini sparklines
- Delta color: green (positive) / red (negative)
- Sparkline: mini Plotly line chart (height 32px)
- Test coverage for real KPI rendering
- Ruff clean"
```

---

## Task 3: Auto-Refresh with `st.fragment` (P9)

### Files:
- Modify: `dashboard/app.py`
- Modify: `dashboard/components/*.py` (add `@st.fragment` decorators)

### Step 1: Write Failing Test

```python
# tests/unit/test_dashboard.py
def test_app_auto_refresh_works():
    """Dashboard auto-refreshes via st.fragment."""
    from streamlit.testing.v1 import AppTest
    
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    assert not at.exception
    
    # Check for fragment elements (auto-refresh indicators)
    fragments = at.get("fragment")
    assert len(fragments) > 0
    
    # Verify refresh interval control in sidebar
    widget_labels = [w.label for w in at.sidebar.slider]
    assert any("Refresh Interval" in l for l in widget_labels)
```

### Step 2: Run Test → Fail

Run: `pytest tests/unit/test_dashboard.py::test_app_auto_refresh_works -v`

### Step 3: Add `st.fragment` to `app.py`

```python
# In dashboard/app.py main()
from streamlit import fragment


# Wrap each tab renderer with @fragment
@st.fragment(run_every=cfg.get("refresh_interval", 3))
def render_live_overview_tab(db, controls): ...


@st.fragment(run_every=cfg.get("refresh_interval", 3))
def render_stream_analytics_tab(db, controls): ...


@st.fragment(run_every=cfg.get("refresh_interval", 3))
def render_link_analysis_tab(db, controls): ...


# ... similarly for alerts, history, pipeline
```

### Step 3: Update Sidebar to Control Refresh

```python
# In render_sidebar()
refresh_interval = st.slider("Refresh Interval (s)", 1, 30, 3)
return {..., "refresh_interval": refresh_interval}
```

### Step 3: Run Test → Pass

Run: `pytest tests/unit/test_dashboard.py::test_app_auto_refresh_works -v`

### Step 4: Commit

```bash
git add dashboard/app.py dashboard/components/*.py
git commit -m "feat(dashboard): add auto-refresh via st.fragment

- Each tab wrapped with @st.fragment(run_every=N)
- Sidebar refresh interval slider (1-30s)
- AppTest smoke test verifies fragment rendering
- Ruff clean"
```

---

## Task 4: Multi-Window Support (P10)

### Files:
- Modify: `dashboard/app.py` (sidebar toggle)
- Modify: `dashboard/components/*.py` (respect `window_len_s` control)

### Step 1: Write Failing Test

```python
# tests/unit/test_dashboard.py
def test_multi_window_toggle_works():
    """Sidebar window length selector changes query window."""
    from dashboard.components.live_overview import render_live_overview_tab

    mock_db = Mock()
    mock_db.get_latest_window_metrics.return_value = pd.DataFrame(
        {
            "window_start": pd.date_range("2026-09-24", periods=5, freq="30s"),
            "pps": [100, 120, 110],
            "bps": [10000, 12000, 11000],
        }
    )

    controls = {"window_len_s": 30, "protocol_filter": ["TCP", "UDP"]}

    with patch("streamlit.plotly_chart") as mock_plotly, patch("streamlit.columns"):
        render_live_overview_tab(mock_db, controls)
        # Verify query called with window_len_s=30
        # (mock_db.get_latest_window_metrics called with window_len_s=30)
```

### Step 2: Run Test → Fail

### Step 2: Update Sidebar in `app.py`

```python
# In render_sidebar()
window_len = st.selectbox("Window Length", [10, 30, 60], index=0, format_func=lambda x: f"{x}s")
return {..., "window_len_s": window_len}
```

### Step 3: Update All Components to Use `controls["window_len_s"]`

Already done in components (they use `controls["window_len_s"]`). Just verify.

### Step 4: Run Test → Pass

### Step 4: Commit

```bash
git add dashboard/app.py
git commit -m "feat(dashboard): multi-window support (10s/30s/60s)

- Sidebar window length selector (10s/30s/60s)
- All components respect controls['window_len_s']
- Ruff clean"
```

---

## Task 5: Dashboard Tests Expansion (P11)

### Files:
- Create: `tests/unit/test_dashboard.py` (expand)
- Create: `tests/unit/test_components.py` (expand)

### Step 1: Write Failing Tests for Each Tab

```python
# tests/unit/test_dashboard.py - add these tests
def test_stream_analytics_tab_renders_7_cards():
    """Stream Analytics renders all 7 concept cards."""
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    assert not at.exception

    # Click Stream Analytics tab
    at.tabs[1].click().run()
    assert not at.exception

    # Verify 7 concept cards rendered
    markdown_text = " ".join([m.value for m in at.markdown])
    concepts = [
        "FILTERING",
        "SAMPLING",
        "COUNT DISTINCT",
        "COUNTING ONES",
        "MOMENTS",
        "DECAY",
        "ITEMSETS",
    ]
    for concept in concepts:
        assert concept in markdown_text


def test_link_analysis_tab_renders_graph_and_pagerank():
    """Link Analysis renders graph + PageRank table."""
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    at.tabs[2].click().run()
    assert not at.exception

    # Check for Plotly chart (graph) and table
    assert len(at.plotly_chart) >= 1
    assert len(at.dataframe) >= 1


def test_alerts_tab_shows_cards_and_timeline():
    """Alerts tab shows active alerts with timeline."""
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    at.tabs[3].click().run()
    assert not at.exception

    # Check for alert cards
    markdown_text = " ".join([m.value for m in at.markdown])
    assert "TRAFFIC_SPIKE" in markdown_text or "WARN" in markdown_text


def test_history_tab_shows_query_selector():
    """History tab has query selector and results."""
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    at.tabs[4].click().run()
    assert not at.exception

    # Check for selectbox and dataframe
    assert len(at.sidebar.selectbox) >= 1


def test_pipeline_tab_shows_stage_tiles():
    """Pipeline tab shows 5 stage tiles."""
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    at.tabs[5].click().run()
    assert not at.exception

    # Check for 5 columns (stage tiles)
    # Check for batch duration chart
    assert len(at.plotly_chart) >= 1
```

### Step 2: Run Tests → Fail (tabs not fully implemented for real data)

### Step 2: Implement Missing Tab Renderers

Update `dashboard/components/*.py` to render real data when available, fallback to placeholder.

### Step 3: Run Tests → Pass

### Step 4: Commit

```bash
git add tests/unit/test_dashboard.py dashboard/components/*.py
git commit -m "test(dashboard): expand AppTest smoke tests for all 6 tabs

- Live Overview: traffic chart, protocol donut, top ports, decay
- Stream Analytics: 7 concept cards verified
- Link Analysis: graph + PageRank table verified
- Alerts: cards + timeline + filters verified
- History: query selector + results table verified
- Pipeline: stage tiles + batch duration + counters verified
- All 4 AppTest smoke tests passing
- Ruff clean"
```

---

## Task 6: Verification & Final Integration

### Step 1: Run Full Test Suite

```bash
cd /mnt/Project/bda/Live-Network-Traffic-Analyser
PYTHONPATH=. python -m pytest tests/unit/test_linkanalysis.py tests/unit/test_alerts.py tests/unit/test_components.py tests/unit/test_dashboard.py -v
```

Expected: **49 tests passing**

### Step 2: Lint Check

```bash
cd /mnt/Project/bda/Live-Network-Traffic-Analyser
python -m ruff check dashboard/ linkanalysis/ tests/
```

Expected: **Clean** (only acceptable BLE001 exceptions)

### Step 3: Manual Dashboard Verification

```bash
export LNTA_MOCK=true
PYTHONPATH=. streamlit run dashboard/app.py --server.headless=true --server.port=8501 &
sleep 10
curl -s http://localhost:8501/_stcore/health
```

Verify in browser:
- [ ] All 6 tabs render
- [ ] Header shows KPI values + delta + sparkline
- [ ] Sidebar: window length (10/30/60s), refresh interval, protocol filter, top-N
- [ ] Auto-refresh works (data updates every 3s)
- [ ] Multi-window toggle works (10s/30s/60s)
- [ ] Loading spinners appear
- [ ] Empty states show properly
- [ ] Keyboard navigation works (Tab, Enter, Esc)
- [ ] Focus visible on all interactive elements
- [ ] Reduced motion respected

### Step 4: Accessibility Audit

```bash
npx @axe-core/cli http://localhost:8501 --exit
```

Expected: **Zero violations**

### Step 5: Commit Final Integration

```bash
git add -A
git commit -m "feat(priyan): M3 Polish complete - states, accessibility, auto-refresh, multi-window, tests

- P8: WCAG 2.1 AA accessibility + loading/empty/error/stale states on all 6 components
- P7: Header/KPI real data with delta% + sparkline
- P9: Auto-refresh via st.fragment (configurable interval)
- P10: Multi-window support (10s/30s/60s toggle)
- P11: AppTest smoke tests for all 6 tabs (4 new tests)
- P12-P15: Ruff clean, accessibility audit passed, manual verification complete
- All 49 unit tests + 4 AppTest smoke tests passing"
```

---

## Verification Checklist

### Before Marking Complete:

- [ ] All 49 unit tests pass
- [ ] 4 AppTest smoke tests pass
- [ ] Ruff check clean (only acceptable BLE001)
- [ ] Axe-core accessibility scan: 0 violations
- [ ] Manual dashboard check: all 6 tabs render, auto-refresh works, multi-window toggle works
- [ ] Keyboard navigation works (Tab, Enter, Esc, arrows)
- [ ] Focus visible on all interactive elements
- [ ] Reduced motion respected
- [ ] Loading/empty/error/stale states show correctly
- [ ] Header shows real KPI data with delta + sparkline
- [ ] Multi-window toggle (10s/30s/60s) works
- [ ] Auto-refresh via `st.fragment` works
- [ ] All commits follow conventional commit format

---

## Execution Handoff

**Plan complete and saved to `docs/plans/2026-09-26-polish-first.md`. Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh sub-agent per task using the `task` tool, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
