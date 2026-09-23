"""
tests/unit/test_components.py — Unit tests for dashboard components.
"""
from unittest.mock import MagicMock, Mock, patch

import pandas as pd


def test_render_header_renders_logo_and_badge():
    """Header renders LNTA logo and source badge."""
    from dashboard.components.header import render_header
    
    db_health = {"connected": True, "table_status": {"window_metrics": "2026-09-24T12:00:00Z"}}
    
    # Create mock context managers for columns
    mock_col = MagicMock()
    mock_col.__enter__ = Mock(return_value=mock_col)
    mock_col.__exit__ = Mock(return_value=False)
    
    with patch("streamlit.columns") as mock_columns, \
         patch("streamlit.markdown") as mock_markdown, \
         patch("streamlit.caption"):
        
        mock_columns.return_value = [mock_col for _ in range(10)]
        
        render_header(db_health, "LIVE")
        
        # Verify logo markdown called
        logo_calls = [c for c in mock_markdown.call_args_list if "LNTA" in str(c)]
        assert len(logo_calls) > 0
        
        # Verify badge rendered
        badge_calls = [c for c in mock_markdown.call_args_list if "source-badge" in str(c)]
        assert len(badge_calls) > 0


def test_render_live_overview_tab_renders_charts():
    """Live Overview renders traffic chart, protocol donut, top ports, decay."""
    # Register theme first
    from dashboard.theme import register_lnta_theme
    register_lnta_theme()
    
    from dashboard.components.live_overview import render_live_overview_tab
    
    # Create a mock db with the required methods
    mock_db = Mock()
    mock_db.get_latest_window_metrics.return_value = pd.DataFrame({
        "window_start": pd.date_range("2026-09-24", periods=5, freq="10s"),
        "pps": [100, 120, 110, 130, 125],
        "bps": [10000, 12000, 11000, 13000, 12500],
    })
    mock_db.get_protocol_counts.return_value = pd.DataFrame({
        "protocol": ["TCP", "UDP", "ICMP"],
        "packets": [100, 30, 10],
    })
    mock_db.get_port_counts.return_value = pd.DataFrame({
        "port": [443, 53, 80],
        "total_packets": [80, 25, 15],
    })
    mock_db.get_decay_traffic.return_value = pd.DataFrame({
        "ts": pd.date_range("2026-09-24", periods=5, freq="10s"),
        "score": [0.5, 0.6, 0.55, 0.65, 0.6],
    })
    
    controls = {"window_len_s": 10, "protocol_filter": ["TCP", "UDP", "ICMP", "OTHER"]}
    
    mock_col = MagicMock()
    mock_col.__enter__ = Mock(return_value=mock_col)
    mock_col.__exit__ = Mock(return_value=False)
    
    with patch("streamlit.markdown"), \
         patch("streamlit.plotly_chart") as mock_plotly, \
         patch("streamlit.columns") as mock_columns:
        
        mock_columns.return_value = [mock_col, mock_col]
        
        render_live_overview_tab(mock_db, controls)
        
        # Should render at least 3 charts: traffic, protocol, ports, decay
        assert mock_plotly.call_count >= 3
