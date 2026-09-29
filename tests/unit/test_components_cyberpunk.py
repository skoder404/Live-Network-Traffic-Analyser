"""
tests/unit/test_components_cyberpunk.py - Unit tests for cyberpunk dashboard components.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd


def test_cyberpunk_header_renders_neon_kpi():
    from dashboard.components.header import render_header
    
    db_health = {"connected": True, "table_status": {"window_metrics": "2026-09-24T12:00:00Z"}}
    
    mock_col = MagicMock()
    mock_col.__enter__ = Mock(return_value=mock_col)
    mock_col.__exit__ = Mock(return_value=False)
    
    with patch("streamlit.columns") as mock_columns, \
         patch("streamlit.markdown") as mock_markdown, \
         patch("streamlit.caption") as mock_caption:
    
        mock_columns.return_value = [mock_col for _ in range(10)]
        
        from dashboard.components.header import render_header
        render_header({"connected": True, "table_status": {"window_metrics": "2026-09-24T12:00:00Z"}}, "LIVE")
        
        logo_calls = [c for c in mock_markdown.call_args_list if "LNTA" in str(c)]
        assert len(logo_calls) > 0
        
        badge_calls = [c for c in mock_markdown.call_args_list if "neon-badge" in str(c)]
        assert len(badge_calls) > 0


def test_cyberpunk_live_overview_renders_neon_charts():
    from dashboard.theme import register_lnta_theme
    register_lnta_theme()
    
    from dashboard.components.live_overview import render_live_overview_tab
    
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
         patch("streamlit.columns") as mock_columns, \
         patch("streamlit.spinner"), \
         patch("streamlit.warning"), \
         patch("streamlit.info"):
    
        mock_columns.return_value = [mock_col, mock_col]
        
        with patch("dashboard.components.live_overview.get_latest_window_metrics") as mock_metrics, \
             patch("dashboard.components.live_overview.get_protocol_counts") as mock_proto, \
             patch("dashboard.components.live_overview.get_port_counts") as mock_ports, \
             patch("dashboard.components.live_overview.get_decay_traffic") as mock_decay:
            
            mock_metrics.return_value = pd.DataFrame({
                "window_start": pd.date_range("2026-09-24", periods=5, freq="10s"),
                "pps": [100, 120, 110, 130, 125],
                "bps": [10000, 12000, 11000, 13000, 12500],
            })
            mock_proto.return_value = pd.DataFrame({
                "protocol": ["TCP", "UDP", "ICMP"],
                "packets": [100, 30, 10],
            })
            mock_ports.return_value = pd.DataFrame({
                "port": [443, 53, 80],
                "total_packets": [80, 25, 15],
            })
            mock_decay.return_value = pd.DataFrame({
                "ts": pd.date_range("2026-09-24", periods=5, freq="10s"),
                "score": [0.5, 0.6, 0.55, 0.65, 0.6],
            })
            
            controls = {"window_len_s": 10, "protocol_filter": ["TCP", "UDP", "ICMP", "OTHER"]}
            
            mock_col = MagicMock()
            mock_col.__enter__ = Mock(return_value=mock_col)
            mock_col.__exit__ = Mock(return_value=False)
            mock_columns.return_value = [mock_col, mock_col]
            
            from dashboard.components.live_overview import render_live_overview_tab
            mock_db = Mock()
            render_live_overview_tab(mock_db, controls)
            
            assert mock_plotly.call_count >= 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def test_other_tabs_render():
    from dashboard.components.node_analysis import render_node_analysis_tab
    from dashboard.components.alerts import render_alerts_tab
    from dashboard.components.graphs import render_graphs_tab
    from dashboard.components.historical import render_historical_tab
    from dashboard.components.settings import render_settings_tab
    from dashboard.theme import register_lnta_theme
    register_lnta_theme()
    
    mock_db = Mock()
    mock_db.get_alerts.return_value = pd.DataFrame()
    mock_db.get_recent_windows.return_value = []
    
    controls = {"window_len_s": 10, "protocol_filter": ["TCP", "UDP"]}
    
    with patch("streamlit.markdown"), patch("streamlit.columns"), patch("streamlit.plotly_chart"), patch("streamlit.dataframe"), patch("streamlit.text_input"), patch("streamlit.selectbox"):
        render_node_analysis_tab(mock_db, controls)
        render_alerts_tab(mock_db, controls)
        render_graphs_tab(mock_db, controls)
        render_historical_tab(mock_db, controls)
        render_settings_tab(mock_db, controls)