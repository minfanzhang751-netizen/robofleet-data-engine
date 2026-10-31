"""Consume window aggregates, classify via ClassifierClient, produce labels.

This is the Module 3 → Module 4 boundary service:

- Input: ``robot-telemetry-windows`` (Spark primary path only).
- Processing: ``ClassifierClient.classify`` (mock today).
- Output: ``robot-diagnostics-classified`` (classified-schema-v1).

Baseline windows are intentionally **not** consumed here so A/B comparison
traffic does not pollute the classification topic.

Kafka semantics: separate consumer group ``robofleet-llm-client``, manual
commit after each successful classify+produce (at-least-once).
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from threading import Event
from typing import IO, Mapping

from confluent_kafka import Consumer, KafkaException, Producer

from services.llm_client.classifier import ClassifierClient, MockClassifier

DEFAULT_BOOTSTRAP = "robofleet-kafka-bootstrap.kafka.svc:9092"
DEFAULT_INPUT_TOPIC = "robot-telemetry-windows"
DEFAULT_OUTPUT_TOPIC = "robot-diagnostics-classified"
DEFAULT_GROUP = "robofleet-llm-client"


def encode_bot_id_key(bot_id: int) -> bytes:
    """Encode ``bot_id`` as UTF-8 decimal text for the Kafka message key."""
    return str(bot_id).encode("utf-8")


class LlmClientService:
    """Poll windows → classify → produce classified events until signaled to stop."""

    def __init__(
        self,
        bootstrap_servers: str,
        input_topic: str,
        output_topic: str,
        group_id: str,
        classifier: ClassifierClient,
        stderr: IO[str] = sys.stderr,
    ) -> None:
        self._input_topic = input_topic
        self._output_topic = output_topic
        self._classifier = classifier
        self._stderr = stderr
        self._stop = Event()
        self._consumed = 0
        self._produced = 0
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "client.id": "robofleet-llm-client",
            }
        )

    def request_stop(self, *_args: object) -> None:
        """SIGINT/SIGTERM handler: exit the poll loop on the next iteration."""
        self._stop.set()

    def _handle(self, raw: bytes) -> None:
        """Classify one window message and produce a classified-schema record.

        ``latency_ms`` measures only the classifier call (not Kafka I/O), so
        Module 4 model latency stays attributable in logs and payloads.
        """
        window_event = json.loads(raw.decode("utf-8"))
        started = time.perf_counter()
        result = self._classifier.classify(window_event)
        latency_ms = (time.perf_counter() - started) * 1000.0
        classified = {
            "schema_version": "1",
            "bot_id": window_event["bot_id"],
            "window_start": window_event["window_start"],
            "window_end": window_event["window_end"],
            "metrics_avg": window_event["metrics_avg"],
            "event_count": window_event["event_count"],
            "label": result["label"],
            "model": result["model"],
            "latency_ms": round(latency_ms, 3),
            "classified_at": datetime.now(timezone.utc).isoformat(),
        }
        self._producer.produce(
            self._output_topic,
            key=encode_bot_id_key(int(classified["bot_id"])),
            value=json.dumps(classified, separators=(",", ":")).encode("utf-8"),
        )
        self._producer.poll(0)
        self._produced += 1
        self._consumed += 1
        print(
            f"classified bot_id={classified['bot_id']} label={classified['label']} "
            f"latency_ms={classified['latency_ms']}",
            file=self._stderr,
            flush=True,
        )

    def run(self) -> int:
        """Blocking consume loop; returns process exit code (0 ok, 1 on failure)."""
        self._consumer.subscribe([self._input_topic])
        print(
            "LLM client starting: "
            f"input={self._input_topic} output={self._output_topic}",
            file=self._stderr,
            flush=True,
        )
        exit_code = 0
        try:
            while not self._stop.is_set():
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                self._handle(msg.value())
                self._consumer.commit(asynchronous=False)
        except Exception as exc:  # noqa: BLE001
            print(f"LLM client failed: {exc}", file=self._stderr, flush=True)
            exit_code = 1
        finally:
            remaining = self._producer.flush(30)
            self._consumer.close()
            print(
                "LLM client stopped: "
                f"consumed={self._consumed} produced={self._produced} "
                f"flush_remaining={remaining}",
                file=self._stderr,
                flush=True,
            )
            if remaining > 0:
                exit_code = 1
        return exit_code


def build_from_env(
    environ: Mapping[str, str] | None = None,
    stderr: IO[str] = sys.stderr,
    classifier: ClassifierClient | None = None,
) -> LlmClientService:
    """Construct the service from env; inject a custom classifier for tests/Module 4."""
    env = os.environ if environ is None else environ
    return LlmClientService(
        bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP).strip()
        or DEFAULT_BOOTSTRAP,
        input_topic=env.get("KAFKA_INPUT_TOPIC", DEFAULT_INPUT_TOPIC).strip()
        or DEFAULT_INPUT_TOPIC,
        output_topic=env.get("KAFKA_OUTPUT_TOPIC", DEFAULT_OUTPUT_TOPIC).strip()
        or DEFAULT_OUTPUT_TOPIC,
        group_id=env.get("KAFKA_GROUP_ID", DEFAULT_GROUP).strip() or DEFAULT_GROUP,
        classifier=classifier or MockClassifier(),
        stderr=stderr,
    )


def main() -> None:
    """Container entrypoint: SIGINT/SIGTERM → graceful flush and non-zero on errors."""
    service = build_from_env()
    signal.signal(signal.SIGINT, service.request_stop)
    signal.signal(signal.SIGTERM, service.request_stop)
    raise SystemExit(service.run())


if __name__ == "__main__":
    main()
