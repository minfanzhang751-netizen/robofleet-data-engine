"""Tests for mock classifier rules (classified-schema-v1 decision table).

No Kafka broker required; only ``MockClassifier`` thresholds are exercised.
"""

import unittest

from services.llm_client.classifier import MockClassifier


class MockClassifierTests(unittest.TestCase):
    """Priority-ordered Safe / Warning / Critical labeling."""

    def setUp(self) -> None:
        self.classifier = MockClassifier()

    def _window(self, level: float, temp: float, torque: float) -> dict[str, object]:
        """Minimal window payload containing only ``metrics_avg`` for classify."""
        return {
            "metrics_avg": {
                "battery_level_pct": level,
                "battery_temp_c": temp,
                "motor_torque_nm": torque,
            }
        }

    def test_critical_on_low_battery(self) -> None:
        """battery_level_pct <= 15 ⇒ Critical."""
        result = self.classifier.classify(self._window(10.0, 40.0, 100.0))
        self.assertEqual(result["label"], "Critical")
        self.assertEqual(result["model"], "mock")

    def test_warning_on_elevated_temp(self) -> None:
        """Otherwise temp >= 50 ⇒ Warning."""
        result = self.classifier.classify(self._window(50.0, 52.0, 100.0))
        self.assertEqual(result["label"], "Warning")

    def test_safe_otherwise(self) -> None:
        """Nominal metrics ⇒ Safe."""
        result = self.classifier.classify(self._window(80.0, 40.0, 100.0))
        self.assertEqual(result["label"], "Safe")


if __name__ == "__main__":
    unittest.main()
