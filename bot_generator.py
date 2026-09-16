import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for simulation configuration.
    """
    parser = argparse.ArgumentParser(
        description="Simulate telemetry data for a fleet of robots."
    )
    parser.add_argument(
        "--eps", type=int, default=5,
        help="Events per second to generate (default: 5)"
    )
    parser.add_argument(
        "--bots", type=int, default=100,
        help="Total number of bots in the fleet (default: 100)"
    )
    return parser.parse_args()

def get_diagnostics_messages() -> List[str]:
    """
    Returns a list of possible diagnostic messages for the robots.
    """
    return [
        "All systems operational.",
        "Battery voltage low.",
        "Motor temperature high.",
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
        "Overcurrent protection active."
    ]

def generate_telemetry_event(bot_id: int, diagnostics_pool: List[str]) -> Dict[str, Any]:
    """
    Generates a single telemetry event for a given bot.
    """
    # Random metrics for the bot
    battery_level_pct: float = round(random.uniform(0, 100), 2)
    battery_temp_c: float = round(random.uniform(30, 60), 2)
    motor_torque_nm: float = round(random.uniform(0, 300), 2)
    diagnostics_msg: str = random.choice(diagnostics_pool)
    
    # Compose the telemetry event as a dict
    event: Dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "bot_id": bot_id,
        "metrics": {
            "battery_level_pct": battery_level_pct,
            "battery_temp_c": battery_temp_c,
            "motor_torque_nm": motor_torque_nm
        },
        "diagnostics": diagnostics_msg
    }
    return event

def main() -> None:
    """
    Main entry point for the telemetry simulation script.
    """
    args = parse_args()
    eps: int = args.eps
    total_bots: int = args.bots
    diagnostics_pool: List[str] = get_diagnostics_messages()

    bot_ids: List[int] = list(range(1, total_bots + 1))
    if eps <= 0:
        raise ValueError("Events per second (eps) must be greater than zero.")
    if total_bots <= 0:
        raise ValueError("Total number of bots must be greater than zero.")

    try:
        while True:
            tick_start = time.time()
            events_to_emit = []
            # Randomly pick bot IDs to generate eps events
            selected_bot_ids = random.choices(bot_ids, k=eps)
            for bot_id in selected_bot_ids:
                event = generate_telemetry_event(bot_id, diagnostics_pool)
                events_to_emit.append(event)
            for event in events_to_emit:
                print(json.dumps(event), flush=True)
            # Sleep to maintain the required frequency even under load
            elapsed = time.time() - tick_start
            time_to_sleep = max(0, 1.0 - elapsed)
            time.sleep(time_to_sleep)
    except KeyboardInterrupt:
        print("\nSimulation interrupted by user.", file=sys.stderr)

if __name__ == "__main__":
    main()