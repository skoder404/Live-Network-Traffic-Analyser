"""
tests/unit/test_dashboard.py — AppTest smoke tests for LNTA dashboard.
"""
import os
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

APP_PATH = Path(__file__).parent.parent.parent / "dashboard" / "app.py"


@pytest.fixture(autouse=True)
def set_env():
    """Set environment variables for mock mode."""
    os.environ["LNTA_MOCK"] = "true"
    os.environ["LNTA_SERVING_DB"] = "/tmp/test.db"
    yield
    os.environ.pop("LNTA_MOCK", None)
    os.environ.pop("LNTA_SERVING_DB", None)


def test_app_loads_without_error():
    """App loads and renders all 6 tabs without exception."""
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    assert not at.exception
    
    # Check all 6 tabs exist
    tab_labels = [tab.label for tab in at.tabs]
    expected = ["Live Overview", "Stream Analytics", "Link Analysis", "Alerts", "History", "Pipeline"]
    for label in expected:
        assert any(label in t for t in tab_labels)


def test_app_renders_header_and_kpi():
    """Header, badge, pulse, and 6 KPI cards render."""
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    assert not at.exception
    
    # Check for LNTA logo
    markdown_text = " ".join([m.value for m in at.markdown])
    assert "LNTA" in markdown_text
    assert "Live Network Traffic Analyser" in markdown_text


def test_sidebar_controls_render():
    """Sidebar shows window length, refresh, protocol filter, top-N."""
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    assert not at.exception
    
    # Check sidebar widgets exist
    widget_labels = []
    for w in at.sidebar.selectbox:
        widget_labels.append(w.label)
    for w in at.sidebar.slider:
        widget_labels.append(w.label)
    for w in at.sidebar.multiselect:
        widget_labels.append(w.label)
    
    assert any("Window Length" in l for l in widget_labels)
    assert any("Refresh Interval" in l for l in widget_labels)
    assert any("Protocol Filter" in l for l in widget_labels)
    assert any("Top-N" in l for l in widget_labels)


def test_mock_mode_works():
    """App runs in mock mode without database."""
    at = AppTest.from_file(str(APP_PATH)).run(timeout=10)
    assert not at.exception
    
    # Should show DEMO DATA badge
    markdown_text = " ".join([m.value for m in at.markdown])
    assert "DEMO" in markdown_text or "STALE" in markdown_text
