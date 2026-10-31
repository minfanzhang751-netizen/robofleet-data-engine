"""Classifier protocol and Module 3 mock implementation.

The LLM client service depends only on ``ClassifierClient``. Module 3 uses
``MockClassifier`` (deterministic rules). Module 4 should add a real HTTP/vLLM
client that still returns ``Classification`` with the same ``label`` enum so
``robot-diagnostics-classified`` stays stable (``docs/classified-schema-v1.md``).
"""

from __future__ import annotations

from typing import Mapping, Protocol, TypedDict


class Classification(TypedDict):
    """Result of one classify call before Kafka envelope fields are added."""

    label: str
    model: str


class ClassifierClient(Protocol):
    """Replaceable backend for Module 4 (real vLLM / HTTP).

    Implementations must be side-effect free aside from the remote call itself;
    timing is measured by ``LlmClientService`` around ``classify``.
    """

    def classify(self, window_event: Mapping[str, object]) -> Classification:
        """Return a label for one window aggregate.

        ``window_event`` is a decoded window-schema-v1 object (at least
        ``metrics_avg`` is required by the mock).
        """


class MockClassifier:
    """Rule-based mock aligned with ``docs/classified-schema-v1.md``.

    Priority (first match wins):

    1. Critical: low battery (<=15), high temp (>=55), or high torque (>=270)
    2. Warning: battery <=30, temp >=50, or torque >=200
    3. Safe: otherwise
    """

    model_name = "mock"

    def classify(self, window_event: Mapping[str, object]) -> Classification:
        """Apply mock thresholds to window ``metrics_avg``."""
        metrics = window_event["metrics_avg"]
        assert isinstance(metrics, Mapping)
        level = float(metrics["battery_level_pct"])
        temp = float(metrics["battery_temp_c"])
        torque = float(metrics["motor_torque_nm"])
        if level <= 15 or temp >= 55 or torque >= 270:
            label = "Critical"
        elif level <= 30 or temp >= 50 or torque >= 200:
            label = "Warning"
        else:
            label = "Safe"
        return Classification(label=label, model=self.model_name)
