from capture.tshark_cmd import TSHARK_FIELDS, build_tshark_command


def test_build_tshark_command_contains_required_options():
    command = build_tshark_command("2")

    assert command[:4] == ["tshark", "-i", "2", "-l"]
    assert "-n" in command
    assert "-T" in command
    assert command[command.index("-T") + 1] == "fields"

    assert ["-E", "separator=,"] in [
        command[i:i + 2] for i in range(len(command) - 1)
    ]
    assert ["-E", "occurrence=f"] in [
        command[i:i + 2] for i in range(len(command) - 1)
    ]
    assert ["-E", "header=n"] in [
        command[i:i + 2] for i in range(len(command) - 1)
    ]
    assert ["-E", "quote=n"] in [
        command[i:i + 2] for i in range(len(command) - 1)
    ]


def test_build_tshark_command_contains_all_fields():
    command = build_tshark_command("2")

    for field in TSHARK_FIELDS:
        assert ["-e", field] in [
            command[i:i + 2] for i in range(len(command) - 1)
        ]


def test_build_tshark_command_with_bpf_filter():
    command = build_tshark_command("2", bpf_filter="tcp")

    assert command[-2:] == ["-f", "tcp"]


def test_build_tshark_command_with_duration():
    command = build_tshark_command("2", duration_seconds=60)

    assert command[-2:] == ["-a", "duration:60"]


def test_build_tshark_command_with_filter_and_duration():
    command = build_tshark_command(
        "2",
        bpf_filter="tcp",
        duration_seconds=60,
    )

    assert command[-4:] == [
        "-f",
        "tcp",
        "-a",
        "duration:60",
    ]


def test_build_tshark_command_rejects_invalid_duration():
    try:
        build_tshark_command("2", duration_seconds=0)
    except ValueError as exc:
        assert "greater than 0" in str(exc)
    else:
        raise AssertionError("Expected ValueError")