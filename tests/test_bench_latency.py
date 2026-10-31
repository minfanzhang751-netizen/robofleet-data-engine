"""Tests for Module 3 benchmark latency math. No Kafka broker required."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import bench_latency  # noqa: E402


def _utc(text: str) -> datetime:
    return bench_latency.parse_iso(text)


class PercentileTests(unittest.TestCase):
    """Nearest-rank p50/p95 over a sorted sample."""

    def test_nearest_rank_p50_and_p95(self) -> None:
        values = [float(item) for item in range(1, 101)]
        self.assertEqual(bench_latency.nearest_rank_percentile(values, 50), 50.0)
        self.assertEqual(bench_latency.nearest_rank_percentile(values, 95), 95.0)

    def test_empty_summary_is_not_zero(self) -> None:
        summary = bench_latency.summarize_delays([])
        self.assertEqual(summary["p50"], "no samples")
        self.assertEqual(summary["p95"], "no samples")
        self.assertEqual(summary["n"], "0")

    def test_summary_formats_seconds(self) -> None:
        summary = bench_latency.summarize_delays([30.0, 31.5, 32.25, 40.0])
        self.assertEqual(summary["n"], "4")
        self.assertEqual(summary["p50"], "31.500")
        self.assertEqual(summary["p95"], "40.000")


class SampleWindowTests(unittest.TestCase):
    """Keep only windows emitted during the bench interval."""

    def test_bounds_are_inclusive(self) -> None:
        start = _utc("2026-10-24T17:00:00Z")
        end = _utc("2026-10-24T17:02:00Z")
        self.assertTrue(
            bench_latency.in_sample_window(_utc("2026-10-24T17:00:00Z"), start, end)
        )
        self.assertTrue(
            bench_latency.in_sample_window(_utc("2026-10-24T17:02:00Z"), start, end)
        )
        self.assertFalse(
            bench_latency.in_sample_window(_utc("2026-10-24T16:59:59Z"), start, end)
        )

    def test_delay_and_filter(self) -> None:
        start = _utc("2026-10-24T17:00:00Z")
        end = _utc("2026-10-24T17:02:00Z")
        records = [
            {
                "window_end": "2026-10-24T17:00:10.000Z",
                "emitted_at": "2026-10-24T17:00:41.000Z",
            },
            {
                "window_end": "2026-10-24T16:00:00+00:00",
                "emitted_at": "2026-10-24T16:00:31+00:00",
            },
            {"window_end": "2026-10-24T17:00:20Z"},
        ]
        delays = bench_latency.delays_for_records(records, start, end)
        self.assertEqual(delays, [31.0])


class LagParseTests(unittest.TestCase):
    """Max LAG from consumer-group describe text."""

    def test_max_across_partitions(self) -> None:
        text = """
Consumer group 'robofleet-baseline-windows' has no active members.

GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
robofleet-baseline-windows robot-telemetry 0 10 12 2 - - -
robofleet-baseline-windows robot-telemetry 1 8 8 0 - - -
robofleet-baseline-windows robot-telemetry 2 1 9 7 - - -
"""
        self.assertEqual(bench_latency.max_lag_from_describe(text), "7")

    def test_missing_group(self) -> None:
        text = "Error: Consumer group 'missing' does not exist."
        self.assertEqual(bench_latency.max_lag_from_describe(text), "group not found")


if __name__ == "__main__":
    unittest.main()
