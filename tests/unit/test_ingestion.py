import json

from ingestion.layout import StorageLayout, create_local_zones
from ingestion.spark_hdfs import streaming_paths
from ingestion.verification import verify_directory, write_report


def test_local_zones_and_verification(tmp_path):
    layout = StorageLayout(str(tmp_path / "traffic"))
    create_local_zones(layout)
    csv_file = tmp_path / "traffic" / "stream_in" / "batch.csv"
    row = "2026-09-23 10:00:00.000,10.0.0.1,10.0.0.2,123,443,TCP,100,,,,1.0\n"
    csv_file.write_text(row + row + "bad\n", encoding="utf-8")

    report = verify_directory(layout.stream_in)

    assert report["records"] == 3
    assert report["valid_records"] == 2
    assert report["malformed_records"] == 1
    assert report["duplicate_records"] == 1
    assert report["latency_ms"]["min"] is not None


def test_report_is_written_atomically(tmp_path):
    destination = tmp_path / "logs" / "status.json"
    write_report({"ok": True}, destination)
    assert json.loads(destination.read_text(encoding="utf-8")) == {"ok": True}


def test_spark_paths_use_configured_hdfs_root():
    paths = streaming_paths(
        {
            "hdfs": {"namenode_uri": "hdfs://namenode:9000", "root": "/traffic"},
            "spark": {"checkpoint_root": "hdfs://namenode:9000/traffic/checkpoints"},
        }
    )
    assert paths["stream_in"] == "hdfs://namenode:9000/traffic/stream_in"
    assert paths["checkpoint"].endswith("/checkpoints")
