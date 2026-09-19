"""Tests for Kafka sink helpers (no live broker required)."""

import io
import json
import unittest
from threading import Event
from unittest.mock import MagicMock, patch

import bot_generator
import kafka_sink


class KafkaSinkUnitTests(unittest.TestCase):
    """Validate key encoding and env-based sink selection."""

    def test_encode_bot_id_key_is_utf8_decimal(self) -> None:
        self.assertEqual(kafka_sink.encode_bot_id_key(42), b"42")

    def test_build_event_sink_defaults_to_stdout_without_bootstrap(self) -> None:
        stdout = io.StringIO()
        sink = kafka_sink.build_event_sink(environ={}, stdout=stdout)
        self.assertIsInstance(sink, bot_generator.StdoutSink)
        event = bot_generator.generate_telemetry_event(7)
        sink.emit(event)
        sink.close()
        parsed = json.loads(stdout.getvalue().strip())
        self.assertEqual(parsed["schema_version"], "1")
        self.assertEqual(parsed["bot_id"], 7)

    def test_build_event_sink_uses_kafka_when_bootstrap_set(self) -> None:
        with patch("kafka_sink.Producer") as producer_cls:
            producer = MagicMock()
            producer.flush.return_value = 0
            producer_cls.return_value = producer
            sink = kafka_sink.build_event_sink(
                environ={
                    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                    "KAFKA_TOPIC": "robot-telemetry",
                }
            )
            self.assertIsInstance(sink, kafka_sink.KafkaTelemetrySink)
            event = bot_generator.generate_telemetry_event(3)
            sink.emit(event)
            sink.close()
            producer.produce.assert_called()
            kwargs = producer.produce.call_args.kwargs
            self.assertEqual(kwargs["key"], b"3")
            self.assertIn(b'"schema_version":"1"', kwargs["value"])
            producer.flush.assert_called()

    def test_run_generator_emits_through_injected_sink(self) -> None:
        stop_event = Event()
        emitted: list[bot_generator.TelemetryEvent] = []

        class CaptureSink:
            def emit(self, event: bot_generator.TelemetryEvent) -> None:
                emitted.append(event)
                stop_event.set()

            def close(self) -> None:
                return None

        bot_generator.run_generator(
            eps=5,
            total_bots=2,
            stop_event=stop_event,
            sink=CaptureSink(),
            stderr=io.StringIO(),
        )
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0]["schema_version"], "1")


if __name__ == "__main__":
    unittest.main()
