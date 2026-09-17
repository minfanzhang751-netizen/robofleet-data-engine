"""Tests for the RoboFleet telemetry generator."""

import argparse
import io
import json
import signal
import unittest
import uuid
from datetime import datetime, timezone
from threading import Event
from unittest.mock import Mock, patch

import bot_generator


def metrics(
    battery_level_pct: float = 50.0,
    battery_temp_c: float = 40.0,
    motor_torque_nm: float = 100.0,
) -> bot_generator.TelemetryMetrics:
    """Build metrics for diagnostic boundary tests."""
    return bot_generator.TelemetryMetrics(
        battery_level_pct=battery_level_pct,
        battery_temp_c=battery_temp_c,
        motor_torque_nm=motor_torque_nm,
    )


class TelemetryEventTests(unittest.TestCase):
    """Validate the version 1 event contract."""

    def test_event_matches_v1_contract(self) -> None:
        before = datetime.now(timezone.utc)
        event = bot_generator.generate_telemetry_event(bot_id=42)
        after = datetime.now(timezone.utc)

        self.assertEqual(
            set(event),
            {
                "schema_version",
                "event_id",
                "timestamp",
                "bot_id",
                "metrics",
                "diagnostics",
            },
        )
        self.assertEqual(event["schema_version"], "1")
        self.assertEqual(uuid.UUID(event["event_id"]).version, 4)
        self.assertIsInstance(event["bot_id"], int)
        self.assertEqual(event["bot_id"], 42)
        self.assertIsInstance(event["diagnostics"], str)

        event_time = datetime.fromisoformat(event["timestamp"])
        self.assertEqual(event_time.utcoffset(), timezone.utc.utcoffset(event_time))
        self.assertLessEqual(before, event_time)
        self.assertLessEqual(event_time, after)

        event_metrics = event["metrics"]
        self.assertEqual(
            set(event_metrics),
            {"battery_level_pct", "battery_temp_c", "motor_torque_nm"},
        )
        self.assertIsInstance(event_metrics["battery_level_pct"], float)
        self.assertIsInstance(event_metrics["battery_temp_c"], float)
        self.assertIsInstance(event_metrics["motor_torque_nm"], float)
        self.assertGreaterEqual(event_metrics["battery_level_pct"], 0.0)
        self.assertLessEqual(event_metrics["battery_level_pct"], 100.0)
        self.assertGreaterEqual(event_metrics["battery_temp_c"], 30.0)
        self.assertLessEqual(event_metrics["battery_temp_c"], 60.0)
        self.assertGreaterEqual(event_metrics["motor_torque_nm"], 0.0)
        self.assertLessEqual(event_metrics["motor_torque_nm"], 300.0)

    def test_metric_diagnostics_follow_priority_and_boundaries(self) -> None:
        cases = (
            (
                metrics(
                    battery_level_pct=15.0,
                    battery_temp_c=55.0,
                    motor_torque_nm=270.0,
                ),
                "Battery voltage low.",
            ),
            (
                metrics(
                    battery_level_pct=15.01,
                    battery_temp_c=55.0,
                    motor_torque_nm=270.0,
                ),
                "Motor temperature high.",
            ),
            (
                metrics(
                    battery_level_pct=15.01,
                    battery_temp_c=54.99,
                    motor_torque_nm=270.0,
                ),
                "Overcurrent protection active.",
            ),
        )

        for event_metrics, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(
                    bot_generator.select_diagnostic(event_metrics),
                    expected,
                )

    @patch("bot_generator.random.choice", return_value="All systems operational.")
    def test_normal_metrics_use_general_diagnostics(
        self,
        _mock_choice: object,
    ) -> None:
        self.assertEqual(
            bot_generator.select_diagnostic(metrics()),
            "All systems operational.",
        )


class CommandLineTests(unittest.TestCase):
    """Validate command-line boundaries before the loop starts."""

    def test_positive_arguments_are_accepted(self) -> None:
        args = bot_generator.parse_args(["--eps", "10", "--bots", "3"])
        self.assertEqual(args.eps, 10)
        self.assertEqual(args.bots, 3)

    def test_non_positive_arguments_are_rejected(self) -> None:
        invalid_arguments = (
            ["--eps", "0"],
            ["--eps", "-1"],
            ["--bots", "0"],
            ["--bots", "-1"],
        )
        for argv in invalid_arguments:
            with self.subTest(argv=argv):
                with self.assertRaises(SystemExit), patch(
                    "sys.stderr",
                    new_callable=io.StringIO,
                ):
                    bot_generator.parse_args(argv)

    def test_positive_int_returns_argparse_error_for_zero(self) -> None:
        with self.assertRaises(argparse.ArgumentTypeError):
            bot_generator.positive_int("0")


class LifecycleTests(unittest.TestCase):
    """Validate stop handling and output stream separation."""

    @patch("bot_generator.signal.signal")
    def test_signal_handlers_request_stop(
        self,
        mock_signal: Mock,
    ) -> None:
        stop_event = Event()
        bot_generator.install_signal_handlers(stop_event)

        handlers = {
            registered_signal: handler
            for registered_signal, handler in (
                call.args for call in mock_signal.call_args_list
            )
        }
        self.assertEqual(set(handlers), {signal.SIGINT, signal.SIGTERM})
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        self.assertTrue(stop_event.is_set())

    def test_stop_event_ends_loop_and_stdout_contains_only_json(self) -> None:
        stop_event = Event()
        stdout = io.StringIO()
        stderr = io.StringIO()
        original_generate = bot_generator.generate_telemetry_event

        def generate_once(bot_id: int) -> bot_generator.TelemetryEvent:
            event = original_generate(bot_id)
            stop_event.set()
            return event

        with patch(
            "bot_generator.generate_telemetry_event",
            side_effect=generate_once,
        ):
            bot_generator.run_generator(
                eps=5,
                total_bots=2,
                stop_event=stop_event,
                stdout=stdout,
                stderr=stderr,
            )

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        event = json.loads(lines[0])
        self.assertEqual(event["schema_version"], "1")
        self.assertIn(event["bot_id"], (1, 2))
        self.assertEqual(stderr.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
