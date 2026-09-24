"""
tests/unit/test_writers.py — Unit tests for capture/writers.py.
"""

import json
import socket
import threading
import time
from pathlib import Path

from capture.parser import Record
from capture.writers import (
    CsvRotatingWriter,
    JsonLinesWriter,
    StdoutWriter,
    TcpLineWriter,
)


def sample_record(src_port: int = 12345, length: int = 100) -> Record:
    return Record(
        timestamp="2026-09-22 10:00:00.123",
        src_ip="192.168.1.10",
        dst_ip="8.8.8.8",
        src_port=src_port,
        dst_port=443,
        protocol="TCP",
        packet_length=length,
        src_mac="aa:bb:cc:dd:ee:ff",
        dst_mac="11:22:33:44:55:66",
        tcp_flags="0x0018",
        iat_ms=1.5,
    )


def test_csv_rotating_writer_lazy_creation(tmp_path: Path):
    writer = CsvRotatingWriter(out_dir=tmp_path, rotate_seconds=60)
    # No file created before first write
    assert list(tmp_path.glob("*.csv")) == []

    rec = sample_record()
    writer.write(rec)
    writer.close()

    csv_files = list(tmp_path.glob("*.csv"))
    assert len(csv_files) == 1
    content = csv_files[0].read_text(encoding="utf-8").strip().splitlines()
    assert len(content) == 1
    # Check no header line (starts with timestamp)
    assert content[0].startswith("2026-09-22")
    assert "192.168.1.10" in content[0]


def test_csv_rotating_writer_time_rotation(tmp_path: Path):
    curr_time = 1758600000.0  # arbitrary epoch

    def fake_clock():
        return curr_time

    writer = CsvRotatingWriter(
        out_dir=tmp_path,
        rotate_seconds=10,
        clock=fake_clock,
    )

    writer.write(sample_record(src_port=1001))

    # Advance time by 12 seconds
    curr_time += 12.0
    writer.write(sample_record(src_port=1002))
    writer.close()

    csv_files = sorted(tmp_path.glob("*.csv"))
    assert len(csv_files) == 2

    lines_file1 = csv_files[0].read_text(encoding="utf-8").strip().splitlines()
    lines_file2 = csv_files[1].read_text(encoding="utf-8").strip().splitlines()
    assert len(lines_file1) == 1
    assert "1001" in lines_file1[0]
    assert len(lines_file2) == 1
    assert "1002" in lines_file2[0]


def test_csv_rotating_writer_size_rotation(tmp_path: Path):
    writer = CsvRotatingWriter(
        out_dir=tmp_path,
        rotate_seconds=3600,
        rotate_mb=1,
    )
    # Directly set rotate_bytes to small number for test
    writer.rotate_bytes = 120

    writer.write(sample_record(length=100))
    writer.write(sample_record(length=200))
    writer.write(sample_record(length=300))
    writer.close()

    csv_files = list(tmp_path.glob("*.csv"))
    assert len(csv_files) >= 2


def test_json_lines_writer(tmp_path: Path):
    json_path = tmp_path / "traffic.json"
    writer = JsonLinesWriter(json_path)

    rec = sample_record()
    writer.write(rec)
    writer.close()

    lines = json_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    obj = json.loads(lines[0])
    assert obj["src_ip"] == "192.168.1.10"
    assert obj["dst_port"] == 443
    assert obj["protocol"] == "TCP"
    assert obj["packet_length"] == 100


def test_stdout_writer(capsys):
    writer = StdoutWriter()
    rec = sample_record()
    writer.write(rec)
    captured = capsys.readouterr()
    assert "192.168.1.10,8.8.8.8,12345,443,TCP,100" in captured.out


def test_tcp_line_writer_reconnect():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("127.0.0.1", 0))
    port = server_socket.getsockname()[1]
    server_socket.listen(1)

    received_lines = []
    server_ready = threading.Event()

    def server_thread():
        try:
            server_ready.set()
            conn, _ = server_socket.accept()
            with conn:
                while True:
                    chunk = conn.recv(1024)
                    if not chunk:
                        break
                    received_lines.extend(chunk.decode("utf-8").splitlines())
        except Exception:
            pass

    t = threading.Thread(target=server_thread, daemon=True)
    t.start()
    assert server_ready.wait(timeout=2.0)

    writer = TcpLineWriter(host="127.0.0.1", port=port, buffer_size=10)
    rec = sample_record()
    writer.write(rec)
    writer.flush()
    time.sleep(0.05)
    writer.close()
    server_socket.close()
    t.join(timeout=2.0)

    assert any("192.168.1.10" in line for line in received_lines)
