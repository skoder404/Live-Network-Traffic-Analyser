from datetime import datetime, timezone

from ingestion.scripts.cleanup_stream_in import eligible_files
from ingestion.scripts.compact_raw import select_hour


def test_select_explicit_hour_and_last_closed_hour() -> None:
    assert select_hour("2026-09-29", "3") == ("2026-09-29", "03")
    now = datetime(2026, 9, 29, 0, 10, tzinfo=timezone.utc)
    assert select_hour(now=now) == ("2026-09-28", "23")


def test_cleanup_filters_new_tmp_and_hidden_files() -> None:
    listing = "\n".join(
        [
            "-rw-r--r-- 1 user group 10 2026-09-27 00:00 /traffic/stream_in/old.csv",
            "-rw-r--r-- 1 user group 10 2026-09-29 00:00 /traffic/stream_in/new.csv",
            "-rw-r--r-- 1 user group 10 2026-09-27 00:00 /traffic/stream_in/.open.tmp",
            "-rw-r--r-- 1 user group 10 2026-09-27 00:00 /traffic/stream_in/.hidden.csv",
        ]
    )
    cutoff = datetime(2026, 9, 28, tzinfo=timezone.utc)
    assert eligible_files(listing, cutoff) == ["/traffic/stream_in/old.csv"]
