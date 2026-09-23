"""
tests/fixtures/fake_tshark.py — Mock TShark script for runner and integration testing.

Simulates TShark output by emitting realistic raw CSV lines to stdout.
Supports:
--crash-after N: Exit with error code 1 after emitting N lines (to test auto-restart)
--max-lines N: Exit cleanly after N lines
--rate PPS: Control rate of packet generation
"""

import argparse
import sys
import time


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--iface", default="test_iface")
    parser.add_argument("-l", action="store_true")
    parser.add_argument("-n", action="store_true")
    parser.add_argument("-T", dest="t_format")
    parser.add_argument("-E", dest="extra", action="append")
    parser.add_argument("-e", dest="fields", action="append")
    parser.add_argument("-f", dest="filter")
    parser.add_argument("-a", dest="duration")
    parser.add_argument("--crash-after", type=int, default=-1)
    parser.add_argument("--max-lines", type=int, default=50)
    parser.add_argument("--delay", type=float, default=0.01)
    args, unknown = parser.parse_known_args()

    line_count = 0
    now = time.time()

    valid_line = (
        "{epoch},192.168.1.10,8.8.8.8,,,54321,443,,,6,,1420,"
        "aa:bb:cc:dd:ee:ff,11:22:33:44:55:66,0x0018,0.010\n"
    )

    while True:
        if args.max_lines > 0 and line_count >= args.max_lines:
            break
        if args.crash_after > 0 and line_count >= args.crash_after:
            sys.stderr.write("fake_tshark: simulated crash\n")
            sys.stderr.flush()
            sys.exit(1)

        sys.stdout.write(valid_line.format(epoch=now + line_count * 0.01))
        sys.stdout.flush()
        line_count += 1
        time.sleep(args.delay)


if __name__ == "__main__":
    main()
