"""
tests/unit/test_moments.py — Unit tests for AMS F2 second-moment estimator and Moments Analytic.
"""

import random
import unittest
from collections import Counter

from streaming.analytics.moments import (
    AMSF2,
    compute_iat_from_timestamps,
    exact_f2,
)


class TestMoments(unittest.TestCase):
    def test_exact_f2_calculation(self):
        """exact_f2 must equal sum of squared frequency counts."""
        counter = Counter({"192.168.1.1": 10, "8.8.8.8": 5, "1.1.1.1": 2})
        # 10^2 + 5^2 + 2^2 = 100 + 25 + 4 = 129
        self.assertEqual(exact_f2(counter), 129.0)

    def test_ams_f2_uniform_stream_estimation(self):
        """
        Tests AMS F2 estimator on a uniform stream with multiple distinct keys.
        """
        rng = random.Random(42)
        stream_items = [f"192.168.1.{rng.randint(1, 20)}" for _ in range(1000)]

        counter = Counter(stream_items)
        real_f2 = exact_f2(counter)

        ams = AMSF2(num_estimators=100, num_groups=10, seed=42)
        ams.add_many(stream_items)
        est_f2 = ams.estimate()

        rel_err = abs(est_f2 - real_f2) / real_f2
        # On uniform stream of 1000 items with 100 estimators, error is typically within 25%
        self.assertLess(
            rel_err,
            0.25,
            f"AMS F2 relative error {rel_err:.2%} exceeded tolerance (est={est_f2}, real={real_f2})",
        )

    def test_ams_f2_skewed_stream_estimation(self):
        """
        Tests AMS F2 estimator on a highly skewed stream (heavy-hitter IP).
        """
        rng = random.Random(123)
        # Heavy hitter represents 70% of traffic
        items = ["192.168.1.100"] * 700 + [f"10.0.0.{rng.randint(1, 50)}" for _ in range(300)]
        rng.shuffle(items)

        counter = Counter(items)
        real_f2 = exact_f2(counter)

        ams = AMSF2(num_estimators=120, num_groups=12, seed=123)
        ams.add_many(items)
        est_f2 = ams.estimate()

        rel_err = abs(est_f2 - real_f2) / real_f2
        # Heavy hitter makes F2 dominate and AMS estimate very accurate (< 20%)
        self.assertLess(
            rel_err,
            0.20,
            f"AMS skewed error {rel_err:.2%} exceeded tolerance (est={est_f2}, real={real_f2})",
        )

    def test_compute_iat_from_timestamps_iso_and_epochs(self):
        """Tests inter-arrival calculation for missing iat_ms values."""
        # 1. ISO string timestamps spaced by 500ms and 1200ms
        ts_strings = [
            "2026-09-24T12:00:00.000Z",
            "2026-09-24T12:00:00.500Z",
            "2026-09-24T12:00:01.700Z",
        ]
        iats = compute_iat_from_timestamps(ts_strings)
        self.assertEqual(len(iats), 2)
        self.assertAlmostEqual(iats[0], 500.0, places=2)
        self.assertAlmostEqual(iats[1], 1200.0, places=2)

        # 2. Float epoch seconds
        epochs = [1700000000.0, 1700000000.25, 1700000001.0]
        iats_epoch = compute_iat_from_timestamps(epochs)
        self.assertEqual(len(iats_epoch), 2)
        self.assertAlmostEqual(iats_epoch[0], 250.0, places=2)
        self.assertAlmostEqual(iats_epoch[1], 750.0, places=2)

        # 3. Edge cases
        self.assertEqual(compute_iat_from_timestamps([]), [])
        self.assertEqual(compute_iat_from_timestamps(["2026-09-24T12:00:00Z"]), [])
        self.assertEqual(compute_iat_from_timestamps([None, "invalid"]), [])

    def test_ams_snapshot_restore_round_trip(self):
        """Tests state serialization and restore for AMS estimator."""
        ams = AMSF2(num_estimators=40, num_groups=5, seed=99)
        ams.add_many(["ip1", "ip2", "ip1", "ip3", "ip1", "ip2"])

        orig_estimate = ams.estimate()
        state = ams.snapshot()

        ams_restored = AMSF2()
        ams_restored.restore(state)

        self.assertEqual(ams_restored.n, ams.n)
        self.assertEqual(ams_restored.estimate(), orig_estimate)

        # Adding subsequent items preserves stream trajectory
        ams.add("ip1")
        ams_restored.add("ip1")
        self.assertEqual(ams.estimate(), ams_restored.estimate())


if __name__ == "__main__":
    unittest.main()
