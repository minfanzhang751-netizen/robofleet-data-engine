"""Kafka sink for RoboFleet telemetry events (Module 2.3)."""

from __future__ import annotations

import json
import os
import sys
from typing import IO, Mapping

from confluent_kafka import Producer

from bot_generator import EventSink, StdoutSink, TelemetryEvent


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
    ) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must be a non-empty string")
        if not topic.strip():
            raise ValueError("topic must be a non-empty string")
        self._topic = topic
        self._stderr = stderr
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    def emit(self, event: TelemetryEvent) -> None:
        payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
        key = encode_bot_id_key(event["bot_id"])

        def on_delivery(err: object, _msg: object) -> None:
            if err is not None:
                print(f"Kafka delivery failed: {err}", file=self._stderr, flush=True)

        try:
            self._producer.produce(
                self._topic,
                key=key,
                value=payload,
                on_delivery=on_delivery,
            )
            self._producer.poll(0)
        except BufferError:
            self._producer.flush(10)
            self._producer.produce(
                self._topic,
                key=key,
                value=payload,
                on_delivery=on_delivery,
            )
            self._producer.poll(0)

    def close(self) -> None:
        remaining = self._producer.flush(30)
        if remaining > 0:
            print(
                f"Kafka flush left {remaining} message(s) unconfirmed.",
                file=self._stderr,
                flush=True,
            )


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
    topic = env.get("KAFKA_TOPIC", "robot-telemetry").strip() or "robot-telemetry"
    return KafkaTelemetrySink(
        bootstrap_servers=bootstrap,
        topic=topic,
        stderr=stderr,
    )
