"""Tests for Kafka sink helpers (no live broker required)."""

import io
import json
import unittest
from threading import Event
from unittest.mock import MagicMock, patch

import bot_generator
import kafka_sink


class KafkaSinkUnitTests(unittest.TestCase):
    """Validate key encoding, env selection, and producer failure contract."""

    def test_encode_bot_id_key_is_utf8_decimal(self) -> None:
        self.assertEqual(kafka_sink.encode_bot_id_key(42), b"42")

    def test_build_event_sink_defaults_to_stdout_without_bootstrap(self) -> None:
        stdout = io.StringIO()
        sink = kafka_sink.build_event_sink(environ={}, stdout=stdout)
        self.assertIsInstance(sink, bot_generator.StdoutSink)
        event = bot_generator.generate_telemetry_event(7)
        sink.emit(event)
        self.assertEqual(sink.close(), 0)
        parsed = json.loads(stdout.getvalue().strip())
        self.assertEqual(parsed["schema_version"], "1")
        self.assertEqual(parsed["bot_id"], 7)

    def test_build_event_sink_uses_kafka_when_bootstrap_set(self) -> None:
        stderr = io.StringIO()
        with patch("confluent_kafka.Producer") as producer_cls:
            producer = MagicMock()
            producer.flush.return_value = 0
            producer_cls.return_value = producer
            sink = kafka_sink.build_event_sink(
                environ={
                    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                    "KAFKA_TOPIC": "robot-telemetry",
                },
                stderr=stderr,
            )
            self.assertIsInstance(sink, kafka_sink.KafkaTelemetrySink)
            event = bot_generator.generate_telemetry_event(3)
            sink.emit(event)
            self.assertEqual(sink.close(), 0)
            producer_cls.assert_called_once()
            config = producer_cls.call_args.args[0]
            self.assertEqual(config["bootstrap.servers"], "localhost:9092")
            self.assertEqual(config["acks"], "all")
            self.assertTrue(config["enable.idempotence"])
            self.assertEqual(config["client.id"], kafka_sink.DEFAULT_CLIENT_ID)
            producer.produce.assert_called()
            kwargs = producer.produce.call_args.kwargs
            self.assertEqual(kwargs["key"], b"3")
            self.assertIn(b'"schema_version":"1"', kwargs["value"])
            producer.flush.assert_called()
            self.assertIn("Kafka sink starting:", stderr.getvalue())
            self.assertIn("Kafka sink stopped:", stderr.getvalue())

    def test_empty_topic_env_falls_back_to_default(self) -> None:
        stderr = io.StringIO()
        with patch("confluent_kafka.Producer") as producer_cls:
            producer = MagicMock()
            producer.flush.return_value = 0
            producer_cls.return_value = producer
            sink = kafka_sink.build_event_sink(
                environ={
                    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                    "KAFKA_TOPIC": "   ",
                },
                stderr=stderr,
            )
            self.assertIsInstance(sink, kafka_sink.KafkaTelemetrySink)
            self.assertEqual(sink._topic, kafka_sink.DEFAULT_TOPIC)

    def test_client_id_env_override(self) -> None:
        stderr = io.StringIO()
        with patch("confluent_kafka.Producer") as producer_cls:
            producer = MagicMock()
            producer.flush.return_value = 0
            producer_cls.return_value = producer
            kafka_sink.build_event_sink(
                environ={
                    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                    "KAFKA_CLIENT_ID": "custom-client",
                },
                stderr=stderr,
            )
            config = producer_cls.call_args.args[0]
            self.assertEqual(config["client.id"], "custom-client")

    def test_buffer_error_retries_after_flush(self) -> None:
        stderr = io.StringIO()
        producer = MagicMock()
        producer.flush.return_value = 0
        producer.produce.side_effect = [BufferError("full"), None]
        sink = kafka_sink.KafkaTelemetrySink(
            bootstrap_servers="localhost:9092",
            topic="robot-telemetry",
            stderr=stderr,
            producer=producer,
        )
        sink.emit(bot_generator.generate_telemetry_event(1))
        self.assertEqual(producer.produce.call_count, 2)
        producer.flush.assert_called()
        self.assertEqual(sink.close(), 0)

    def test_buffer_error_after_flush_raises(self) -> None:
        stderr = io.StringIO()
        producer = MagicMock()
        producer.flush.return_value = 0
        producer.produce.side_effect = BufferError("still full")
        sink = kafka_sink.KafkaTelemetrySink(
            bootstrap_servers="localhost:9092",
            topic="robot-telemetry",
            stderr=stderr,
            producer=producer,
        )
        with self.assertRaises(BufferError):
            sink.emit(bot_generator.generate_telemetry_event(1))
        self.assertIn("still full after flush", stderr.getvalue())

    def test_delivery_failure_makes_close_nonzero(self) -> None:
        stderr = io.StringIO()
        producer = MagicMock()
        producer.flush.return_value = 0

        def fake_produce(*_args: object, **kwargs: object) -> None:
            on_delivery = kwargs["on_delivery"]
            on_delivery("broker down", None)

        producer.produce.side_effect = fake_produce
        sink = kafka_sink.KafkaTelemetrySink(
            bootstrap_servers="localhost:9092",
            topic="robot-telemetry",
            stderr=stderr,
            producer=producer,
        )
        sink.emit(bot_generator.generate_telemetry_event(9))
        self.assertEqual(sink.close(), 1)
        self.assertIn("Kafka delivery failed:", stderr.getvalue())
        self.assertIn("delivery_failures=1", stderr.getvalue())

    def test_flush_remaining_makes_close_nonzero(self) -> None:
        stderr = io.StringIO()
        producer = MagicMock()
        producer.flush.return_value = 3
        sink = kafka_sink.KafkaTelemetrySink(
            bootstrap_servers="localhost:9092",
            topic="robot-telemetry",
            stderr=stderr,
            producer=producer,
        )
        self.assertEqual(sink.close(), 1)
        self.assertIn("flush left 3 message(s) unconfirmed", stderr.getvalue())

    def test_run_generator_emits_through_injected_sink(self) -> None:
        stop_event = Event()
        emitted: list[bot_generator.TelemetryEvent] = []

        class CaptureSink:
            def emit(self, event: bot_generator.TelemetryEvent) -> None:
                emitted.append(event)
                stop_event.set()

            def close(self) -> int:
                return 0

        bot_generator.run_generator(
            eps=5,
            total_bots=2,
            stop_event=stop_event,
            sink=CaptureSink(),
            stderr=io.StringIO(),
        )
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0]["schema_version"], "1")

    def test_main_exits_nonzero_when_sink_close_fails(self) -> None:
        class FailingSink:
            def emit(self, event: bot_generator.TelemetryEvent) -> None:
                return None

            def close(self) -> int:
                return 1

        stop_event = Event()
        stop_event.set()
        with (
            patch("bot_generator.parse_args") as parse_args,
            patch("bot_generator.install_signal_handlers"),
            patch("bot_generator.run_generator"),
            patch("kafka_sink.build_event_sink", return_value=FailingSink()),
        ):
            parse_args.return_value = type("Args", (), {"eps": 1, "bots": 1})()
            with self.assertRaises(SystemExit) as raised:
                bot_generator.main()
            self.assertEqual(raised.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
