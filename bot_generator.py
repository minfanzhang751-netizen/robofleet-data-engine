import argparse
import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from threading import Event
from types import FrameType
from typing import IO, Sequence, TypedDict


SCHEMA_VERSION = "1"
LOW_BATTERY_THRESHOLD_PCT = 15.0
HIGH_BATTERY_TEMP_THRESHOLD_C = 55.0
HIGH_MOTOR_TORQUE_THRESHOLD_NM = 270.0
WARNING_THROTTLE_SECONDS = 5.0

GENERAL_DIAGNOSTICS: tuple[str, ...] = (
    "All systems operational.",
    "Wheel encoder error detected.",
    "Obstacle detected in path.",
    "GPS signal lost.",
    "Communications nominal.",
    "Sensor calibration required.",
    "Routine maintenance needed.",
    "Unexpected shutdown detected.",
    "Software update available.",
    "Power cycle initiated.",
    "Navigation target reached.",
    "Data link interrupted.",
)


class TelemetryMetrics(TypedDict):
    """Numeric measurements included in a telemetry event."""

    battery_level_pct: float
    battery_temp_c: float
    motor_torque_nm: float


class TelemetryEvent(TypedDict):
    """Version 1 telemetry event contract."""

    schema_version: str
    event_id: str
    timestamp: str
    bot_id: int
    metrics: TelemetryMetrics
    diagnostics: str


def positive_int(value: str) -> int:
    """Parse a strictly positive integer for an argparse option."""
    parsed_value = int(value)
    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed_value


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for simulation configuration."""
    parser = argparse.ArgumentParser(
        description="Simulate telemetry data for a fleet of robots."
    )
    parser.add_argument(
        "--eps",
        type=positive_int,
        default=5,
        help="Events per second to generate (default: 5)",
    )
    parser.add_argument(
        "--bots",
        type=positive_int,
        default=100,
        help="Total number of bots in the fleet (default: 100)",
    )
    return parser.parse_args(argv)


def select_diagnostic(metrics: TelemetryMetrics) -> str:
    """Choose a diagnostic consistent with metrics using fixed priority."""
    if metrics["battery_level_pct"] <= LOW_BATTERY_THRESHOLD_PCT:
        return "Battery voltage low."
    if metrics["battery_temp_c"] >= HIGH_BATTERY_TEMP_THRESHOLD_C:
        return "Motor temperature high."
    if metrics["motor_torque_nm"] >= HIGH_MOTOR_TORQUE_THRESHOLD_NM:
        return "Overcurrent protection active."
    return random.choice(GENERAL_DIAGNOSTICS)


def generate_telemetry_event(bot_id: int) -> TelemetryEvent:
    """Generate one version 1 telemetry event for the given robot."""
    metrics = TelemetryMetrics(
        battery_level_pct=round(random.uniform(0, 100), 2),
        battery_temp_c=round(random.uniform(30, 60), 2),
        motor_torque_nm=round(random.uniform(0, 300), 2),
    )
    return TelemetryEvent(
        schema_version=SCHEMA_VERSION,
        event_id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc).isoformat(),
        bot_id=bot_id,
        metrics=metrics,
        diagnostics=select_diagnostic(metrics),
    )


def install_signal_handlers(stop_event: Event) -> None:
    """Set SIGINT and SIGTERM handlers that request a graceful stop."""

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)


def run_generator(
    eps: int,
    total_bots: int,
    stop_event: Event,
    stdout: IO[str] = sys.stdout,
    stderr: IO[str] = sys.stderr,
) -> None:
    """Emit evenly paced JSON Lines until a graceful stop is requested."""
    interval_seconds = 1.0 / eps
    bot_ids = tuple(range(1, total_bots + 1))
    next_deadline = time.monotonic()
    last_warning_at: float | None = None

    while not stop_event.is_set():
        wait_seconds = next_deadline - time.monotonic()
        if wait_seconds > 0 and stop_event.wait(wait_seconds):
            break

        event = generate_telemetry_event(random.choice(bot_ids))
        print(
            json.dumps(event, separators=(",", ":")),
            file=stdout,
            flush=True,
        )

        completed_at = time.monotonic()
        lag_seconds = completed_at - next_deadline
        if lag_seconds >= interval_seconds:
            warning_due = (
                last_warning_at is None
                or completed_at - last_warning_at >= WARNING_THROTTLE_SECONDS
            )
            if warning_due:
                print(
                    "Generator is behind schedule by "
                    f"{lag_seconds:.3f}s at target {eps} EPS.",
                    file=stderr,
                    flush=True,
                )
                last_warning_at = completed_at
            next_deadline = completed_at + interval_seconds
        else:
            next_deadline += interval_seconds


def main() -> None:
    """Run the telemetry simulator until SIGINT or SIGTERM."""
    args = parse_args()
    stop_event = Event()
    install_signal_handlers(stop_event)

    run_generator(args.eps, args.bots, stop_event)
    print("Telemetry generator stopped gracefully.", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()