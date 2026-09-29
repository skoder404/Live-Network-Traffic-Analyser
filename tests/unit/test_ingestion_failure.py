from unittest.mock import MagicMock, patch

import pytest

from ingestion.layout import StorageLayout, create_hdfs_zones, create_local_zones, hdfs_available
from ingestion.spark_hdfs import streaming_paths
from ingestion.verification import verify_files


def test_flume_restart_duplicate_recovery(tmp_path):
    """Test verification handles duplicate records caused by Flume restart (at-least-once delivery)."""
    f1 = tmp_path / "data1.csv"
    # Create two files with overlapping data. 11 fields required.
    valid_row1 = "2023-01-01 10:00:00.000,192.168.1.1,10.0.0.1,80,443,TCP,100,00:00:00:00:00:00,00:00:00:00:00:01,0x0018,10.5\n"
    valid_row2 = "2023-01-01 10:00:01.000,192.168.1.1,10.0.0.1,80,443,TCP,100,00:00:00:00:00:00,00:00:00:00:00:01,0x0018,10.5\n"
    
    f1.write_text(valid_row1)
    f2 = tmp_path / "data2.csv"
    f2.write_text(valid_row1 + valid_row2)
    
    report = verify_files([f1, f2])
    assert report["records"] == 3
    assert report["valid_records"] == 3
    assert report["duplicate_records"] == 1
    assert report["loss_check"] == "not_available_without_sequence_id"

def test_data_loss_detection(tmp_path):
    """Test loss detection signals when malformed records occur."""
    f1 = tmp_path / "data.csv"
    valid_row1 = "2023-01-01 10:00:00.000,192.168.1.1,10.0.0.1,80,443,TCP,100,00:00:00:00:00:00,00:00:00:00:00:01,0x0018,10.5\n"
    invalid_row1 = "invalid_row_with_one_column\n"
    invalid_row2 = "2023-01-01 10:00:01.000,src\n"
    f1.write_text(valid_row1 + invalid_row1 + invalid_row2)
    
    report = verify_files([f1])
    assert report["records"] == 3
    assert report["malformed_records"] == 2
    assert report["valid_records"] == 1

def test_layout_creation_idempotent(tmp_path):
    """Test local layout creation doesn't fail on existing directories (idempotency)."""
    layout = StorageLayout(root=str(tmp_path / "traffic"))
    paths1 = create_local_zones(layout)
    assert all(p.exists() for p in paths1)
    
    # Second time should be idempotent
    paths2 = create_local_zones(layout)
    assert all(p.exists() for p in paths2)

@patch("subprocess.run")
def test_hdfs_connection_failure_handling(mock_run):
    """Test that HDFS connection failures are properly handled."""
    # Simulate hdfs not found
    mock_run.side_effect = OSError("No such file or directory")
    
    assert hdfs_available("hdfs") is False
    
    layout = StorageLayout(root="/test")
    with pytest.raises(RuntimeError, match="Unable to execute 'hdfs'"):
        create_hdfs_zones(layout, hdfs_bin="hdfs")

@patch("subprocess.run")
def test_hdfs_command_failure(mock_run):
    """Test when hdfs is available but command fails."""
    mock_process = MagicMock()
    mock_process.returncode = 1
    mock_process.stderr = "Permission denied"
    mock_run.return_value = mock_process
    
    layout = StorageLayout(root="/test")
    with pytest.raises(RuntimeError, match="Permission denied"):
        create_hdfs_zones(layout, hdfs_bin="hdfs")

def test_streaming_paths_fallback():
    """Test spark_hdfs streaming paths generation with defaults (handling unavailable config)."""
    paths = streaming_paths({})
    assert "hdfs://localhost:9000/traffic/raw" == paths["raw"]
    assert "hdfs://localhost:9000/traffic/stream_in" == paths["stream_in"]
    assert "hdfs://localhost:9000/traffic/processed" == paths["processed"]
    assert "hdfs://localhost:9000/traffic/checkpoints" == paths["checkpoint"]
