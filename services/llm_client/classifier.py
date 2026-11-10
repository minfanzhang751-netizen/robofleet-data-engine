"""Classifier protocol, Module 3 mock, and Module 4 vLLM client.

The Kafka loop depends only on ``ClassifierClient``. ``MockClassifier`` is the
deterministic Module 3 backend. ``VllmClassifier`` calls an in-cluster vLLM
OpenAI-compatible server. ``resolve_window`` retries that call and, when no
legal label appears, returns a DLQ record instead of a mock label.

Output field names stay on ``docs/classified-schema-v1.md``.
"""

from __future__ import annotations

import json
import time
import urllib.request
from datetime import datetime, timezone
from typing import Mapping, Protocol, TypedDict
from urllib.error import HTTPError, URLError

LABELS: tuple[str, ...] = ("Safe", "Warning", "Critical")
LABEL_SET = frozenset(LABELS)

SYSTEM_PROMPT = (
    "You classify one robot telemetry window for a fleet operator. "
    "Read the status log. Use the window averages only as context. "
    "Return JSON with a single field label, whose value is exactly one of "
    "Safe, Warning, or Critical."
)

RESPONSE_FORMAT: dict[str, object] = {
    "type": "json_schema",
    "json_schema": {
        "name": "diagnostics_label",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "label": {"type": "string", "enum": list(LABELS)},
            },
            "required": ["label"],
            "additionalProperties": False,
        },
    },
}

REQUIRED_WINDOW_FIELDS = (
    "bot_id",
    "window_start",
    "window_end",
    "metrics_avg",
    "event_count",
)


class Classification(TypedDict):
    """Result of one classify call before Kafka envelope fields are added."""

    label: str
    model: str


class ResolvedWindow(TypedDict):
    """Either a classified-schema record or a DLQ record. Never both."""

    kind: str
    record: dict[str, object]


class ClassifierClient(Protocol):
    """Replaceable backend. Timing is measured by ``resolve_window``."""

    def classify(self, window_event: Mapping[str, object]) -> Classification:
        """Return a label for one window aggregate."""


class ClassificationError(Exception):
    """One classify attempt did not yield a legal label."""


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


def build_user_prompt(window_event: Mapping[str, object]) -> str:
    """Prompt body: status log first, window averages as context only.

    The text does not restate the mock numeric bands. Those bands stay inside
    ``MockClassifier`` and are not sent to the model.
    """
    status_log = window_event.get("status_log")
    text = status_log.strip() if isinstance(status_log, str) else ""
    metrics = window_event.get("metrics_avg")
    if not isinstance(metrics, Mapping):
        metrics = {}
    return (
        "Status log:\n"
        f"{text}\n\n"
        "Window averages (context only):\n"
        f"battery_level_pct={metrics.get('battery_level_pct')}\n"
        f"battery_temp_c={metrics.get('battery_temp_c')}\n"
        f"motor_torque_nm={metrics.get('motor_torque_nm')}\n"
        f"event_count={window_event.get('event_count')}"
    )


class VllmClassifier:
    """HTTP client for vLLM ``POST /v1/chat/completions`` structured output."""

    def __init__(
        self,
        base_url: str,
        model: str,
        timeout_seconds: float = 30.0,
        opener: object | None = None,
    ) -> None:
        self.model_name = model
        self._url = base_url.rstrip("/") + "/chat/completions"
        self._timeout_seconds = timeout_seconds
        self._opener = urllib.request.urlopen if opener is None else opener

    def classify(self, window_event: Mapping[str, object]) -> Classification:
        """Call vLLM once. Raises ``ClassificationError`` when the label is unusable."""
        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": build_user_prompt(window_event)},
            ],
            "temperature": 0,
            "max_tokens": 32,
            "response_format": RESPONSE_FORMAT,
        }
        request = urllib.request.Request(
            self._url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with self._opener(request, timeout=self._timeout_seconds) as response:
                status = int(getattr(response, "status", 200))
                raw = response.read()
        except HTTPError as exc:
            exc.close()
            raise ClassificationError(f"vLLM HTTP {exc.code}") from exc
        except URLError as exc:
            raise ClassificationError(f"vLLM unreachable: {exc.reason}") from exc
        except TimeoutError as exc:
            raise ClassificationError("vLLM timed out") from exc
        except OSError as exc:
            raise ClassificationError(f"vLLM transport failed: {exc}") from exc
        if status < 200 or status >= 300:
            raise ClassificationError(f"vLLM HTTP {status}")
        return Classification(label=_parse_label(raw), model=self.model_name)


def _parse_label(raw: bytes) -> str:
    """Pull ``choices[0].message.content.label`` and require the enum."""
    try:
        body = json.loads(raw.decode("utf-8"))
        content = body["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        label = parsed["label"]
    except (
        KeyError,
        IndexError,
        TypeError,
        json.JSONDecodeError,
        UnicodeDecodeError,
    ) as exc:
        raise ClassificationError("vLLM response was not a label object") from exc
    if not isinstance(label, str) or label not in LABEL_SET:
        raise ClassificationError(f"illegal label {label!r}")
    return label


def resolve_window(
    raw: bytes,
    classifier: ClassifierClient,
    max_attempts: int,
) -> ResolvedWindow:
    """Classify one Kafka value, or describe why it goes to the DLQ.

    Malformed JSON and incomplete windows are not sent to the classifier.
    Transport errors and illegal labels retry up to ``max_attempts``. The
    failure record has no ``label`` and does not call ``MockClassifier``.
    ``latency_ms`` on success covers only the attempt that returned a legal label.
    """
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return _dlq(raw, f"invalid json: {exc}", None)

    if not isinstance(decoded, dict):
        return _dlq(raw, "window payload is not an object", None)
    window_event: Mapping[str, object] = decoded
    missing = [field for field in REQUIRED_WINDOW_FIELDS if field not in window_event]
    metrics = window_event.get("metrics_avg")
    if missing or not isinstance(metrics, Mapping):
        return _dlq(raw, "window missing required fields", window_event)

    attempts = max(1, max_attempts)
    last_error = "classifier produced no label"
    for _attempt in range(attempts):
        started = time.perf_counter()
        try:
            result = classifier.classify(window_event)
        except ClassificationError as exc:
            last_error = str(exc)
            continue
        latency_ms = (time.perf_counter() - started) * 1000.0
        label = result.get("label")
        model = result.get("model")
        if label not in LABEL_SET or not isinstance(model, str) or not model:
            last_error = f"illegal label {label!r}"
            continue
        return {
            "kind": "classified",
            "record": {
                "schema_version": "1",
                "bot_id": window_event["bot_id"],
                "window_start": window_event["window_start"],
                "window_end": window_event["window_end"],
                "metrics_avg": window_event["metrics_avg"],
                "event_count": window_event["event_count"],
                "label": label,
                "model": model,
                "latency_ms": round(latency_ms, 3),
                "classified_at": datetime.now(timezone.utc).isoformat(),
            },
        }
    return _dlq(raw, last_error, window_event)


def _dlq(
    raw: bytes,
    reason: str,
    window: Mapping[str, object] | None,
) -> ResolvedWindow:
    """Build a DLQ record. ``label`` and ``model`` are intentionally absent."""
    record: dict[str, object] = {
        "schema_version": "1",
        "reason": reason,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "window_raw": raw.decode("utf-8", errors="replace"),
    }
    if window is not None:
        record["window"] = dict(window)
    return {"kind": "dlq", "record": record}
