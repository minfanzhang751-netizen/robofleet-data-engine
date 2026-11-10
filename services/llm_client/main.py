"""Consume window aggregates, classify via ClassifierClient, produce labels.

This is the Module 3 → Module 4 boundary service:

- Input: ``robot-telemetry-windows`` (Spark primary path only).
- Processing: ``ClassifierClient.classify`` (mock, or vLLM when ``CLASSIFIER=vllm``).
- Output: ``robot-diagnostics-classified`` (classified-schema-v1).
- Failures: ``robot-diagnostics-dlq`` after a fixed number of attempts. The
  offset is committed only after the classified record or the DLQ record is
  acknowledged. Mock rules are not used as a fallback.

Baseline windows are intentionally **not** consumed here so A/B comparison
traffic does not pollute the classification topic.

Kafka semantics: separate consumer group ``robofleet-llm-client``, manual
commit (at-least-once).
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from threading import Event
from typing import IO, Mapping

from confluent_kafka import Consumer, KafkaException, Producer

from services.llm_client.classifier import (
    ClassifierClient,
    MockClassifier,
    VllmClassifier,
    resolve_window,
)

DEFAULT_BOOTSTRAP = "robofleet-kafka-bootstrap.kafka.svc:9092"
DEFAULT_INPUT_TOPIC = "robot-telemetry-windows"
DEFAULT_OUTPUT_TOPIC = "robot-diagnostics-classified"
DEFAULT_DLQ_TOPIC = "robot-diagnostics-dlq"
DEFAULT_GROUP = "robofleet-llm-client"
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_VLLM_BASE_URL = "http://vllm.robofleet.svc:8000/v1"
DEFAULT_VLLM_MODEL = "Qwen/Qwen2.5-3B-Instruct"


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
        dlq_topic: str,
        max_attempts: int,
        stderr: IO[str] = sys.stderr,
    ) -> None:
        self._input_topic = input_topic
        self._output_topic = output_topic
        self._dlq_topic = dlq_topic
        self._classifier = classifier
        self._max_attempts = max_attempts
        self._stderr = stderr
        self._stop = Event()
        self._consumed = 0
        self._produced = 0
        self._dlq = 0
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

    def _produce_sync(
        self,
        topic: str,
        key: bytes | None,
        record: Mapping[str, object],
    ) -> None:
        """Produce one record and wait until the broker ack arrives.

        The caller commits the input offset only after this returns. A delivery
        error or timeout leaves the offset uncommitted.
        """
        done = Event()
        holder: dict[str, object] = {}

        def on_delivery(err: object, _msg: object) -> None:
            holder["err"] = err
            done.set()

        self._producer.produce(
            topic,
            key=key,
            value=json.dumps(dict(record), separators=(",", ":")).encode("utf-8"),
            on_delivery=on_delivery,
        )
        deadline = time.monotonic() + 30
        while not done.is_set():
            self._producer.poll(0.2)
            if time.monotonic() >= deadline:
                raise TimeoutError(f"produce to {topic} timed out")
        if holder.get("err") is not None:
            raise KafkaException(holder["err"])

    def _handle(self, raw: bytes) -> None:
        """Classify one window, or write a DLQ record after retries are exhausted.

        ``latency_ms`` is recorded inside ``resolve_window`` around the
        successful ``classify`` call only.
        """
        outcome = resolve_window(raw, self._classifier, self._max_attempts)
        record = outcome["record"]
        if outcome["kind"] == "classified":
            self._produce_sync(
                self._output_topic,
                encode_bot_id_key(int(record["bot_id"])),
                record,
            )
            self._produced += 1
            print(
                f"classified bot_id={record['bot_id']} label={record['label']} "
                f"latency_ms={record['latency_ms']}",
                file=self._stderr,
                flush=True,
            )
        else:
            self._produce_sync(self._dlq_topic, _dlq_key(record), record)
            self._dlq += 1
            print(
                f"dlq reason={record['reason']}",
                file=self._stderr,
                flush=True,
            )
        self._consumed += 1

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
                f"dlq={self._dlq} flush_remaining={remaining}",
                file=self._stderr,
                flush=True,
            )
            if remaining > 0:
                exit_code = 1
        return exit_code


def positive_attempts(raw: str | None) -> int:
    """Parse ``CLASSIFIER_MAX_ATTEMPTS``; blank or invalid values use the default."""
    if raw is None or not raw.strip():
        return DEFAULT_MAX_ATTEMPTS
    try:
        parsed = int(raw.strip())
    except ValueError:
        return DEFAULT_MAX_ATTEMPTS
    if parsed <= 0:
        return DEFAULT_MAX_ATTEMPTS
    return parsed


def classifier_from_env(env: Mapping[str, str]) -> ClassifierClient:
    """Select mock or vLLM. Unknown values fail at startup instead of falling back."""
    kind = env.get("CLASSIFIER", "mock").strip().lower() or "mock"
    if kind == "mock":
        return MockClassifier()
    if kind == "vllm":
        base_url = env.get("VLLM_BASE_URL", DEFAULT_VLLM_BASE_URL).strip() or (
            DEFAULT_VLLM_BASE_URL
        )
        model = env.get("VLLM_MODEL", DEFAULT_VLLM_MODEL).strip() or DEFAULT_VLLM_MODEL
        return VllmClassifier(base_url=base_url, model=model)
    raise ValueError(f"unknown CLASSIFIER {kind!r}")


def build_from_env(
    environ: Mapping[str, str] | None = None,
    stderr: IO[str] = sys.stderr,
    classifier: ClassifierClient | None = None,
) -> LlmClientService:
    """Construct the service from env; inject a custom classifier for tests."""
    env = os.environ if environ is None else environ
    return LlmClientService(
        bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP).strip()
        or DEFAULT_BOOTSTRAP,
        input_topic=env.get("KAFKA_INPUT_TOPIC", DEFAULT_INPUT_TOPIC).strip()
        or DEFAULT_INPUT_TOPIC,
        output_topic=env.get("KAFKA_OUTPUT_TOPIC", DEFAULT_OUTPUT_TOPIC).strip()
        or DEFAULT_OUTPUT_TOPIC,
        group_id=env.get("KAFKA_GROUP_ID", DEFAULT_GROUP).strip() or DEFAULT_GROUP,
        classifier=classifier if classifier is not None else classifier_from_env(env),
        dlq_topic=env.get("KAFKA_DLQ_TOPIC", DEFAULT_DLQ_TOPIC).strip() or DEFAULT_DLQ_TOPIC,
        max_attempts=positive_attempts(env.get("CLASSIFIER_MAX_ATTEMPTS")),
        stderr=stderr,
    )


def _dlq_key(record: Mapping[str, object]) -> bytes | None:
    """Use ``bot_id`` as the DLQ key when the failed window still has one."""
    window = record.get("window")
    if not isinstance(window, Mapping) or "bot_id" not in window:
        return None
    return encode_bot_id_key(int(window["bot_id"]))  # type: ignore[arg-type]


def main() -> None:
    """Container entrypoint: SIGINT/SIGTERM → graceful flush and non-zero on errors."""
    service = build_from_env()
    signal.signal(signal.SIGINT, service.request_stop)
    signal.signal(signal.SIGTERM, service.request_stop)
    raise SystemExit(service.run())


if __name__ == "__main__":
    main()
