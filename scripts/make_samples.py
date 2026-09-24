"""
scripts/make_samples.py — Generate and commit standard benchmark datasets to data/sample/.

Produces six 300-second synthetic datasets with companion .truth.json metadata:
- normal: Baseline web, DNS and ping mix
- spike: Rate surge in final third (triggers spike alert)
- portscan_like: Single source probing >25 ports in 10s (triggers port scan alert)
- fanout: Single source connecting to >40 IPs in 10s (triggers fanout alert)
- dns_heavy: High UDP 53 volume
- multi_host_graph: Fixed graph structure for PageRank and Markov verification
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from capture.generate import generate_records, write_dataset  # noqa: E402

SAMPLE_DIR = REPO_ROOT / "data" / "sample"

CONFIGS = [
    {"scenario": "normal", "duration": 300.0, "rate": 15.0, "seed": 42},
    {"scenario": "spike", "duration": 300.0, "rate": 15.0, "seed": 101},
    {"scenario": "portscan_like", "duration": 300.0, "rate": 15.0, "seed": 202},
    {"scenario": "fanout", "duration": 300.0, "rate": 15.0, "seed": 303},
    {"scenario": "dns_heavy", "duration": 300.0, "rate": 15.0, "seed": 404},
    {"scenario": "multi_host_graph", "duration": 300.0, "rate": 15.0, "seed": 505},
]


def main() -> None:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Generating benchmark datasets into {SAMPLE_DIR}...")

    for cfg in CONFIGS:
        scenario = cfg["scenario"]
        out_csv = SAMPLE_DIR / f"{scenario}.csv"
        print(f"  Generating '{scenario}' (duration={cfg['duration']}s, rate={cfg['rate']} pps)...")
        records = generate_records(
            scenario=scenario,
            duration_s=cfg["duration"],
            rate_pps=cfg["rate"],
            seed=cfg["seed"],
        )
        write_dataset(records, out_csv)
        truth_file = SAMPLE_DIR / f"{scenario}.truth.json"
        print(f"  -> Wrote {len(records)} records to {out_csv.name} and {truth_file.name}")

    print("All sample datasets successfully generated.")


if __name__ == "__main__":
    main()
