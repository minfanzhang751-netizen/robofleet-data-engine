"""Unit tests for Module 3 event-time window gold semantics.

These tests lock the shared rules in ``stream.window_semantics`` that Spark
mirrors via Structured Streaming (60s/10s windows, 30s watermark). No live
Kafka or Spark cluster is required.
"""

import unittest
from datetime import datetime, timedelta, timezone

from stream.window_semantics import (
    EventTimeWindowEngine,
    gold_window_averages,
    sliding_window_bounds,
)


class SlidingWindowBoundsTests(unittest.TestCase):
    """Pure function checks for epoch-aligned sliding window membership."""

    def test_event_belongs_to_six_windows(self) -> None:
        """size/slide = 60/10 ⇒ each event belongs to exactly six [start, end) windows."""
        event_time = datetime(2026, 1, 1, 12, 0, 25, tzinfo=timezone.utc)
        bounds = sliding_window_bounds(event_time)
        self.assertEqual(len(bounds), 6)
        for start, end in bounds:
            self.assertEqual(end - start, timedelta(seconds=60))
            self.assertLessEqual(start, event_time)
            self.assertLess(event_time, end)
            self.assertEqual(int(start.timestamp()) % 10, 0)


class EventTimeWindowEngineTests(unittest.TestCase):
    """Engine ingest / watermark / late-drop behavior."""

    def test_averages_match_gold_for_on_time_events(self) -> None:
        """Closed-window means must match offline gold averages (correctness metric)."""
        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        events = [
            {
                "bot_id": 1,
                "timestamp": (t0 + timedelta(seconds=offset)).isoformat(),
                "metrics": {
                    "battery_level_pct": level,
                    "battery_temp_c": 40.0,
                    "motor_torque_nm": 100.0,
                },
            }
            for offset, level in ((5, 10.0), (15, 30.0), (25, 50.0))
        ]
        engine = EventTimeWindowEngine(processor="baseline")
        emitted: list[object] = []
        for event in events:
            closed, dropped = engine.ingest(
                bot_id=1,
                event_time=datetime.fromisoformat(str(event["timestamp"])),
                metrics=event["metrics"],  # type: ignore[arg-type]
            )
            self.assertFalse(dropped)
            emitted.extend(closed)

        # Advance watermark far enough to close early windows.
        closed, _ = engine.ingest(
            bot_id=1,
            event_time=t0 + timedelta(seconds=120),
            metrics={
                "battery_level_pct": 1.0,
                "battery_temp_c": 40.0,
                "motor_torque_nm": 100.0,
            },
        )
        emitted.extend(closed)
        self.assertGreaterEqual(len(emitted), 1)

        first = emitted[0]
        assert isinstance(first, dict)
        start = datetime.fromisoformat(first["window_start"])
        end = datetime.fromisoformat(first["window_end"])
        gold = gold_window_averages(events, bot_id=1, window_start=start, window_end=end)
        self.assertIsNotNone(gold)
        self.assertEqual(first["metrics_avg"], gold)

    def test_late_event_is_dropped(self) -> None:
        """Events older than watermark must not pollute aggregates."""
        engine = EventTimeWindowEngine(processor="baseline")
        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        engine.ingest(
            bot_id=7,
            event_time=t0 + timedelta(seconds=100),
            metrics={
                "battery_level_pct": 50.0,
                "battery_temp_c": 40.0,
                "motor_torque_nm": 100.0,
            },
        )
        closed, dropped = engine.ingest(
            bot_id=7,
            event_time=t0 + timedelta(seconds=10),
            metrics={
                "battery_level_pct": 99.0,
                "battery_temp_c": 40.0,
                "motor_torque_nm": 100.0,
            },
        )
        self.assertTrue(dropped)
        # Late event must not create aggs keyed at early windows with 99%.
        for window_event in engine.flush_remaining():
            if window_event["bot_id"] == 7:
                self.assertNotEqual(window_event["metrics_avg"]["battery_level_pct"], 99.0)


if __name__ == "__main__":
    unittest.main()
