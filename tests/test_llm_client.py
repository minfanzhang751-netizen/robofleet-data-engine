"""Tests for mock rules and the vLLM client (no GPU, no Kafka broker)."""

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from services.llm_client.classifier import (
    SYSTEM_PROMPT,
    Classification,
    ClassificationError,
    MockClassifier,
    VllmClassifier,
    build_user_prompt,
    resolve_window,
)


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


def _window_bytes(
    level: float = 10.0,
    status_log: str = "Battery pack voltage has fallen into the low-reserve band.",
) -> bytes:
    """A complete window payload. Low battery would be Critical under the mock."""
    return json.dumps(
        {
            "bot_id": 7,
            "window_start": "2026-01-01T00:00:00+00:00",
            "window_end": "2026-01-01T00:01:00+00:00",
            "metrics_avg": {
                "battery_level_pct": level,
                "battery_temp_c": 40.0,
                "motor_torque_nm": 100.0,
            },
            "event_count": 4,
            "status_log": status_log,
        }
    ).encode("utf-8")


class PromptTests(unittest.TestCase):
    """The model sees the status log, not the mock threshold table."""

    def test_prompt_has_log_and_averages_without_threshold_rules(self) -> None:
        prompt = build_user_prompt(
            {
                "status_log": "Pack is warm to the touch.",
                "metrics_avg": {
                    "battery_level_pct": 80.0,
                    "battery_temp_c": 40.0,
                    "motor_torque_nm": 100.0,
                },
                "event_count": 3,
            }
        )
        self.assertIn("Pack is warm to the touch.", prompt)
        self.assertIn("battery_level_pct=80.0", prompt)
        self.assertNotIn("<=", prompt)
        self.assertNotIn("<=", SYSTEM_PROMPT)
        self.assertNotIn("threshold", prompt.lower())


class _ScriptedHandler(BaseHTTPRequestHandler):
    """POST /v1/chat/completions returns the next scripted label or HTTP status."""

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        server = self.server
        assert isinstance(server, _ScriptedServer)
        server.bodies.append(body)
        scripted = server.script.pop(0)
        if isinstance(scripted, int):
            payload = b'{"error":"no"}'
            self.send_response(scripted)
        else:
            content = json.dumps({"label": scripted})
            payload = json.dumps(
                {"choices": [{"message": {"content": content}}]}
            ).encode("utf-8")
            self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, _format: str, *_args: object) -> None:
        return


class _ScriptedServer(ThreadingHTTPServer):
    def __init__(self, script: list[str | int]) -> None:
        super().__init__(("127.0.0.1", 0), _ScriptedHandler)
        self.script = script
        self.bodies: list[dict[str, object]] = []


class VllmClassifierTests(unittest.TestCase):
    """Fake HTTP server: structured-output request, retries, no mock fallback."""

    def _serve(self, script: list[str | int]) -> tuple[_ScriptedServer, threading.Thread]:
        server = _ScriptedServer(script)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def stop() -> None:
            server.shutdown()
            server.server_close()

        self.addCleanup(stop)
        return server, thread

    def _client(self, server: _ScriptedServer) -> VllmClassifier:
        host, port = server.server_address[:2]
        return VllmClassifier(
            base_url=f"http://{host}:{port}/v1",
            model="Qwen/Qwen2.5-3B-Instruct",
            timeout_seconds=5.0,
        )

    def test_legal_label_and_structured_output_request(self) -> None:
        server, _thread = self._serve(["Warning"])
        result = self._client(server).classify(json.loads(_window_bytes()))
        self.assertEqual(result["label"], "Warning")
        self.assertEqual(result["model"], "Qwen/Qwen2.5-3B-Instruct")
        body = server.bodies[0]
        response_format = body["response_format"]
        assert isinstance(response_format, dict)
        schema = response_format["json_schema"]
        assert isinstance(schema, dict)
        inner = schema["schema"]
        assert isinstance(inner, dict)
        properties = inner["properties"]
        assert isinstance(properties, dict)
        label_schema = properties["label"]
        assert isinstance(label_schema, dict)
        self.assertEqual(label_schema["enum"], ["Safe", "Warning", "Critical"])
        messages = body["messages"]
        assert isinstance(messages, list)
        user = messages[1]
        assert isinstance(user, dict)
        self.assertIn("low-reserve band", str(user["content"]))

    def test_illegal_label_retries_then_accepts(self) -> None:
        server, _thread = self._serve(["Banana", "Banana", "Critical"])
        outcome = resolve_window(_window_bytes(), self._client(server), max_attempts=3)
        self.assertEqual(outcome["kind"], "classified")
        self.assertEqual(outcome["record"]["label"], "Critical")
        self.assertEqual(outcome["record"]["model"], "Qwen/Qwen2.5-3B-Instruct")
        self.assertEqual(len(server.bodies), 3)
        self.assertNotIn("mock", outcome["record"].values())

    def test_exhausted_retries_do_not_emit_mock_label(self) -> None:
        """Low battery would be Critical under MockClassifier; DLQ must not say so."""
        server, _thread = self._serve(["Banana", "Banana", "Banana"])
        outcome = resolve_window(_window_bytes(level=10.0), self._client(server), 3)
        self.assertEqual(outcome["kind"], "dlq")
        record = outcome["record"]
        self.assertNotIn("label", record)
        self.assertNotIn("model", record)
        self.assertIn("illegal label", str(record["reason"]))
        self.assertEqual(len(server.bodies), 3)

    def test_http_error_is_classification_error(self) -> None:
        server, _thread = self._serve([503])
        with self.assertRaises(ClassificationError):
            self._client(server).classify(json.loads(_window_bytes()))

    def test_invalid_json_skips_classifier(self) -> None:
        class Boom:
            def classify(self, _window: object) -> Classification:
                raise AssertionError("classifier must not run")

        outcome = resolve_window(b"not-json", Boom(), 3)  # type: ignore[arg-type]
        self.assertEqual(outcome["kind"], "dlq")
        self.assertIn("invalid json", str(outcome["record"]["reason"]))
        self.assertNotIn("window", outcome["record"])


if __name__ == "__main__":
    unittest.main()
