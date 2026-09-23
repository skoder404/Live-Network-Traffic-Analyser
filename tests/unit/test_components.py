"""
tests/unit/test_components.py — Unit tests for dashboard components.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
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
         patch("streamlit.caption") as mock_caption:
        
        mock_columns.return_value = [mock_col for _ in range(10)]
        
        render_header(db_health, "LIVE")
        
        # Verify logo markdown called
        logo_calls = [c for c in mock_markdown.call_args_list if "LNTA" in str(c)]
        assert len(logo_calls) > 0
        
        # Verify badge rendered
        badge_calls = [c for c in mock_markdown.call_args_list if "source-badge" in str(c)]
        assert len(badge_calls) > 0
