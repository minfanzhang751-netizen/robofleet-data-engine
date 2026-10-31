"""Kafka sink for RoboFleet telemetry events (Module 2.3)."""

from __future__ import annotations

import json
import os
import sys
from typing import IO, Any, Mapping

from bot_generator import EventSink, StdoutSink, TelemetryEvent

DEFAULT_TOPIC = "robot-telemetry"
DEFAULT_CLIENT_ID = "robofleet-bot-generator"


def encode_bot_id_key(bot_id: int) -> bytes:
    """Encode bot_id as UTF-8 decimal text for the Kafka message key."""
    return str(bot_id).encode("utf-8")


class KafkaTelemetrySink:
    """Produce telemetry events to Kafka with bot_id as the message key."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        stderr: IO[str] = sys.stderr,
        client_id: str = DEFAULT_CLIENT_ID,
        producer: Any | None = None,
    ) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must be a non-empty string")
        if not topic.strip():
            raise ValueError("topic must be a non-empty string")
        self._topic = topic
        self._stderr = stderr
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id.strip() or DEFAULT_CLIENT_ID
        self._delivery_failures = 0
        self._unconfirmed = 0
        if producer is not None:
            self._producer = producer
        else:
            from confluent_kafka import Producer

            self._producer = Producer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "acks": "all",
                    "enable.idempotence": True,
                    "client.id": self._client_id,
                }
            )
        print(
            "Kafka sink starting: "
            f"bootstrap={bootstrap_servers} topic={topic} client.id={self._client_id}",
            file=self._stderr,
            flush=True,
        )

    def _on_delivery(self, err: object, _msg: object) -> None:
        if err is not None:
            self._delivery_failures += 1
            print(f"Kafka delivery failed: {err}", file=self._stderr, flush=True)

    def _produce_once(self, key: bytes, payload: bytes) -> None:
        self._producer.produce(
            self._topic,
            key=key,
            value=payload,
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def emit(self, event: TelemetryEvent) -> None:
        payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
        key = encode_bot_id_key(event["bot_id"])
        try:
            self._produce_once(key, payload)
        except BufferError:
            self._producer.flush(10)
            try:
                self._produce_once(key, payload)
            except BufferError:
                print(
                    "Kafka local queue still full after flush; dropping emit.",
                    file=self._stderr,
                    flush=True,
                )
                raise

    def close(self) -> int:
        remaining = self._producer.flush(30)
        self._unconfirmed = int(remaining)
        print(
            "Kafka sink stopped: "
            f"flush_remaining={self._unconfirmed} "
            f"delivery_failures={self._delivery_failures}",
            file=self._stderr,
            flush=True,
        )
        if self._unconfirmed > 0:
            print(
                f"Kafka flush left {self._unconfirmed} message(s) unconfirmed.",
                file=self._stderr,
                flush=True,
            )
        if self._unconfirmed > 0 or self._delivery_failures > 0:
            return 1
        return 0


def build_event_sink(
    environ: Mapping[str, str] | None = None,
    stdout: IO[str] = sys.stdout,
    stderr: IO[str] = sys.stderr,
) -> EventSink:
    """
    Build stdout or Kafka sink from environment variables.

    When KAFKA_BOOTSTRAP_SERVERS is unset or empty, events go to stdout.
    """
    env = os.environ if environ is None else environ
    bootstrap = env.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap:
        return StdoutSink(stdout=stdout)
    topic = env.get("KAFKA_TOPIC", DEFAULT_TOPIC).strip() or DEFAULT_TOPIC
    client_id = env.get("KAFKA_CLIENT_ID", DEFAULT_CLIENT_ID).strip() or DEFAULT_CLIENT_ID
    return KafkaTelemetrySink(
        bootstrap_servers=bootstrap,
        topic=topic,
        stderr=stderr,
        client_id=client_id,
    )
