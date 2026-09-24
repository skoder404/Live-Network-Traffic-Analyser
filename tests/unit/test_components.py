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
    
    controls = {"window_len_s": 10, "protocol_filter": ["TCP", "UDP", "ICMP", "OTHER"]}
    
    mock_col = MagicMock()
    mock_col.__enter__ = Mock(return_value=mock_col)
    mock_col.__exit__ = Mock(return_value=False)
    
    with patch("streamlit.markdown"), \
         patch("streamlit.plotly_chart") as mock_plotly, \
         patch("streamlit.columns") as mock_columns, \
         patch("dashboard.components.live_overview.get_latest_window_metrics") as mock_get_metrics, \
         patch("dashboard.components.live_overview.get_protocol_counts") as mock_get_protocol, \
         patch("dashboard.components.live_overview.get_port_counts") as mock_get_ports, \
         patch("dashboard.components.live_overview.get_decay_traffic") as mock_get_decay:
        
        mock_columns.return_value = [mock_col, mock_col]
        
        # Mock the convenience functions
        mock_get_metrics.return_value = pd.DataFrame({
            "window_start": pd.date_range("2026-09-24", periods=5, freq="10s"),
            "pps": [100, 120, 110, 130, 125],
            "bps": [10000, 12000, 11000, 13000, 12500],
        })
        mock_get_protocol.return_value = pd.DataFrame({
            "protocol": ["TCP", "UDP", "ICMP"],
            "packets": [100, 30, 10],
        })
        mock_get_ports.return_value = pd.DataFrame({
            "port": [443, 53, 80],
            "total_packets": [80, 25, 15],
        })
        mock_get_decay.return_value = pd.DataFrame({
            "ts": pd.date_range("2026-09-24", periods=5, freq="10s"),
            "score": [0.5, 0.6, 0.55, 0.65, 0.6],
        })
        
        mock_col = MagicMock()
        mock_col.__enter__ = Mock(return_value=mock_col)
        mock_col.__exit__ = Mock(return_value=False)
        mock_columns.return_value = [mock_col, mock_col]
        
        from dashboard.components.live_overview import render_live_overview_tab
        render_live_overview_tab(mock_db, controls)
        
        # Should render at least 3 charts: traffic, protocol, ports, decay
        assert mock_plotly.call_count >= 3


def test_render_history_tab_shows_query_selector_and_results():
    """History tab shows query selector, results table, and chart."""
    from dashboard.theme import register_lnta_theme
    register_lnta_theme()
    
    from dashboard.components.history import render_history_tab
    
    mock_db = Mock()
    
    controls = {"window_len_s": 10}
    
    with patch("streamlit.selectbox") as mock_select, \
         patch("streamlit.dataframe") as mock_df, \
         patch("streamlit.plotly_chart"), \
         patch("dashboard.components.history.get_hist_results") as mock_get_hist:
        
        mock_select.return_value = "protocol_totals"
        mock_get_hist.return_value = pd.DataFrame({
            "query_name": ["protocol_totals", "top_src_ips"],
            "run_at": ["2026-09-24T10:00:00Z", "2026-09-24T10:00:00Z"],
            "columns_json": ['["protocol", "total_packets"]', '["src_ip", "packets"]'],
            "rows_json": ['[["TCP", 1000], ["UDP", 200]]', '[["192.168.1.1", 500]]'],
        })
        
        from dashboard.components.history import render_history_tab
        render_history_tab(mock_db, controls)
        
        # Should show query selector
        mock_select.assert_called()
        # Should show results table
        mock_df.assert_called()


def test_render_pipeline_tab_shows_stage_tiles_and_metrics():
    """Pipeline tab shows stage tiles, batch duration chart, and health counters."""
    from dashboard.theme import register_lnta_theme
    register_lnta_theme()
    
    from dashboard.components.pipeline import render_pipeline_tab
    
    mock_db = Mock()
    
    controls = {"window_len_s": 10}
    
    mock_col = MagicMock()
    mock_col.__enter__ = Mock(return_value=mock_col)
    mock_col.__exit__ = Mock(return_value=False)
    
    with patch("streamlit.columns") as mock_cols, \
         patch("streamlit.plotly_chart") as mock_plotly, \
         patch("streamlit.metric"), \
         patch("dashboard.components.pipeline.get_pipeline_health") as mock_get_health:
        
        mock_cols.return_value = [mock_col for _ in range(5)]
        mock_get_health.return_value = pd.DataFrame({
            "ts": pd.date_range("2026-09-24", periods=10, freq="10s"),
            "component": ["capture"]*5 + ["flume"]*5,
            "metric": ["batch_duration_ms"]*10,
            "value": [50, 55, 48, 52, 51, 100, 95, 105, 98, 102],
        })
        
        mock_col = MagicMock()
        mock_col.__enter__ = Mock(return_value=mock_col)
        mock_col.__exit__ = Mock(return_value=False)
        mock_cols.return_value = [mock_col for _ in range(5)]
        
        from dashboard.components.pipeline import render_pipeline_tab
        render_pipeline_tab(mock_db, controls)
        
        # Should show 5 stage tiles
        assert mock_cols.called
        # Should show batch duration chart
        mock_plotly.assert_called()


def test_components_show_loading_state_when_db_slow():
    """Components show skeleton loader when DB query is slow."""
    from dashboard.theme import register_lnta_theme
    register_lnta_theme()
    
    import time

    from dashboard.components.live_overview import render_live_overview_tab
    
    mock_db = Mock()
    def slow_query(*args, **kwargs):
        time.sleep(0.1)
        return pd.DataFrame()
    
    controls = {"window_len_s": 10}
    
    mock_col = MagicMock()
    mock_col.__enter__ = Mock(return_value=mock_col)
    mock_col.__exit__ = Mock(return_value=False)
    
    with patch("streamlit.markdown"), \
         patch("streamlit.columns") as mock_columns, \
         patch("streamlit.spinner") as mock_spinner, \
         patch("dashboard.components.live_overview.get_latest_window_metrics") as mock_get_metrics, \
         patch("dashboard.components.live_overview.get_protocol_counts") as mock_get_protocol, \
         patch("dashboard.components.live_overview.get_port_counts") as mock_get_ports, \
         patch("dashboard.components.live_overview.get_decay_traffic") as mock_get_decay:
        
        mock_columns.return_value = [mock_col, mock_col]
        mock_spinner.return_value.__enter__ = Mock(return_value=None)
        mock_spinner.return_value.__exit__ = Mock(return_value=False)
        
        # Mock the convenience functions to be slow
        mock_get_metrics.side_effect = slow_query
        mock_get_protocol.return_value = pd.DataFrame()
        mock_get_ports.return_value = pd.DataFrame()
        mock_get_decay.return_value = pd.DataFrame()
        
        mock_col = MagicMock()
        mock_col.__enter__ = Mock(return_value=mock_col)
        mock_col.__exit__ = Mock(return_value=False)
        mock_columns.return_value = [mock_col, mock_col]
        
        from dashboard.components.live_overview import render_live_overview_tab
        render_live_overview_tab(mock_db, controls)
        
        # Should show spinner/loading
        mock_spinner.assert_called()
