"""
tests/unit/test_components_cyberpunk.py - Unit tests for cyberpunk dashboard components.
"""
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
from dashboard.data import MockServingDB


def test_cyberpunk_header_renders():
    from dashboard.components.header import render_header

    db = MockServingDB()
    snapshot = db.snapshot(30)

    mock_col = MagicMock()
    mock_col.__enter__ = Mock(return_value=mock_col)
    mock_col.__exit__ = Mock(return_value=False)

    with patch("streamlit.columns") as mock_columns, patch(
        "streamlit.markdown"
    ) as mock_markdown:
        mock_columns.return_value = [mock_col for _ in range(4)]

        render_header(snapshot, 30, 0)

        header_calls = [c for c in mock_markdown.call_args_list if "LNTA" in str(c)]
        assert len(header_calls) > 0


def test_cyberpunk_live_overview_renders():
    from dashboard.components.live_overview import render_live_overview
    from dashboard.theme import apply_theme

    with patch("streamlit.markdown"), patch("streamlit.components.v1.html"):
        apply_theme()

    db = MockServingDB()
    snapshot = db.snapshot(30)
    ctx = {"db": db}

    with patch("streamlit.columns") as mock_columns, patch(
        "streamlit.area_chart"
    ), patch("streamlit.bar_chart"), patch("streamlit.dataframe"):
        mock_col = MagicMock()
        mock_col.__enter__ = Mock(return_value=mock_col)
        mock_col.__exit__ = Mock(return_value=False)
        mock_columns.return_value = [mock_col, mock_col]

        render_live_overview(snapshot, 30, ctx)


def test_other_tabs_render():
    from dashboard.components.alerts import render_alerts
    from dashboard.components.history import render_history
    from dashboard.components.link_analysis import render_link_analysis
    from dashboard.components.pipeline import render_pipeline
    from dashboard.components.stream_analytics import render_stream_analytics

    db = MockServingDB()
    snapshot = db.snapshot(30)
    ctx = {"db": db}

    mock_col = MagicMock()
    mock_col.__enter__ = Mock(return_value=mock_col)
    mock_col.__exit__ = Mock(return_value=False)
    mock_col.radio.return_value = "packets"
    mock_col.slider.return_value = 30

    with patch("streamlit.markdown"), patch("streamlit.columns", return_value=[mock_col, mock_col]), patch(
        "streamlit.dataframe"
    ), patch("streamlit.selectbox", return_value="Question"), patch(
        "streamlit.multiselect", return_value=["WARN", "CRITICAL"]
    ), patch(
        "streamlit.button", return_value=False
    ), patch(
        "streamlit.download_button"
    ), patch(
        "streamlit.bar_chart"
    ), patch(
        "streamlit.plotly_chart"
    ), patch(
        "streamlit.subheader"
    ), patch(
        "streamlit.caption"
    ), patch(
        "streamlit.slider", return_value=30
    ), patch(
        "streamlit.radio", return_value="packets"
    ):
        render_stream_analytics(snapshot, 30, ctx)
        render_link_analysis(snapshot, 30, ctx)
        render_alerts(snapshot, 30, ctx)
        render_history(snapshot, 30, ctx)
        render_pipeline(snapshot, 30, ctx)