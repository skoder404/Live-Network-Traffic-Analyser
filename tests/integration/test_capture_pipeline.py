"""
tests/integration/test_capture_pipeline.py — Integration test for raw fixture -> parser -> writer pipeline.
"""

from pathlib import Path

from capture.parser import Record, RejectReason, parse_line
from capture.writers import CsvRotatingWriter
from contracts.record_schema import parse_line as contract_parse
from contracts.record_schema import validate_row


def test_tshark_sample_pipeline(tmp_path: Path):
    fixture_path = Path("tests/fixtures/tshark_sample.txt")
    assert fixture_path.exists(), "tshark_sample.txt fixture not found"

    lines = fixture_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 200

    writer = CsvRotatingWriter(
        out_dir=tmp_path,
        rotate_seconds=3600,
        rotate_mb=100,
    )

    records_count = 0
    rejects_count = 0
    reject_reasons = {}

    for line in lines:
        res = parse_line(line)
        if isinstance(res, Record):
            writer.write(res)
            records_count += 1
        else:
            rejects_count += 1
            reject_reasons[res.reason] = reject_reasons.get(res.reason, 0) + 1

    writer.close()

    assert records_count == 190
    assert rejects_count == 10
    assert RejectReason.NON_IP in reject_reasons

    # Check that CSV file was created and contains valid records
    csv_files = list(tmp_path.glob("*.csv"))
    assert len(csv_files) == 1

    output_lines = csv_files[0].read_text(encoding="utf-8").strip().splitlines()
    assert len(output_lines) == 190

    # Assert every single line in output CSV satisfies the contract
    for row in output_lines:
        fields = contract_parse(row)
        ok, reason = validate_row(fields)
        assert ok, f"Output CSV contains invalid row: {reason}"
