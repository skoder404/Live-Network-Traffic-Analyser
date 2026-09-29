from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parents[2] / "dashboard" / "app.py")


def _run():
    return AppTest.from_file(APP, default_timeout=60).run()


def test_runs_clean():
    assert not _run().exception


def test_six_tabs():
    at = _run()
    nav_radio = [r for r in at.sidebar.radio if r.key == "sidebar_nav"][0]
    assert len(nav_radio.options) == 6


def test_window_toggle():
    at = _run()
    win_radio = [r for r in at.sidebar.radio if r.label == "Window"][0]
    win_radio.set_value("60s").run()
    assert not at.exception


def test_auto_refresh_off():
    at = _run()
    at.toggle[0].set_value(False).run()
    assert not at.exception
