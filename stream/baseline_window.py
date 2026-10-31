"""In-cluster handwritten baseline sliding-window consumer (Module 3).

This is the **comparison path**, not the production path. It reads the same
input topic as Spark (``robot-telemetry``) with a **separate consumer group**
so both can process the same messages in parallel for resume metrics.

Pipeline:

1. ``Consumer.poll`` from ``robot-telemetry`` (at-least-once; manual commit).
2. Parse telemetry JSON; feed ``EventTimeWindowEngine`` (shared semantics).
3. When windows close, ``Producer.produce`` to
   ``robot-telemetry-windows-baseline`` with key = UTF-8 decimal ``bot_id``.

State lives only in process memory (no checkpoint PVC). Restart loses open
windows—intentional contrast with Spark's checkpointed state store.

Deployed as ``baseline-window`` in namespace ``robofleet`` via
``scripts/stream-install.sh``. See ``docs/module-3.md``.
"""

from __future__ import annotations

import json
import os
import signal
import sys
from threading import Event
from typing import IO, Mapping

from confluent_kafka import Consumer, KafkaException, Producer

from stream.window_semantics import EventTimeWindowEngine, WindowEvent, parse_event_time

# In-cluster defaults (overridable via env in the Deployment).
DEFAULT_BOOTSTRAP = "robofleet-kafka-bootstrap.kafka.svc:9092"
DEFAULT_INPUT_TOPIC = "robot-telemetry"
DEFAULT_OUTPUT_TOPIC = "robot-telemetry-windows-baseline"
DEFAULT_GROUP = "robofleet-baseline-windows"
PROCESSOR = "baseline"


def encode_bot_id_key(bot_id: int) -> bytes:
    """Encode ``bot_id`` as UTF-8 decimal text (same Kafka key rule as Module 2)."""
    return str(bot_id).encode("utf-8")


def window_to_payload(event: WindowEvent) -> bytes:
    """Serialize a window event to compact UTF-8 JSON for the Kafka value."""
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


class BaselineWindowService:
    """Consume telemetry, aggregate in memory, produce baseline window events.

    Lifecycle:

    - ``run()`` blocks in a poll loop until ``request_stop`` (SIGINT/SIGTERM).
    - On exit, ``flush_remaining`` emits still-open windows, then producer flush.
    - Exit code ``1`` if an exception occurred or flush left unconfirmed messages.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        input_topic: str,
        output_topic: str,
        group_id: str,
        stderr: IO[str] = sys.stderr,
    ) -> None:
        self._input_topic = input_topic
        self._output_topic = output_topic
        self._stderr = stderr
        self._engine = EventTimeWindowEngine(processor=PROCESSOR)
        self._stop = Event()
        self._produced = 0
        self._consumed = 0
        self._dropped_late = 0
        # Manual commit after each handled message → at-least-once delivery.
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
                "client.id": "robofleet-baseline-window",
            }
        )

    def request_stop(self, *_args: object) -> None:
        """Signal handler: ask the poll loop to exit at the next safe point."""
        self._stop.set()

    def _emit(self, window_event: WindowEvent) -> None:
        """Enqueue one closed window to the baseline output topic."""
        self._producer.produce(
            self._output_topic,
            key=encode_bot_id_key(window_event["bot_id"]),
            value=window_to_payload(window_event),
        )
        self._producer.poll(0)
        self._produced += 1

    def _handle_message(self, raw: bytes) -> None:
        """Parse one Kafka value and ingest it into the window engine.

        May emit zero or more closed windows. The first events typically only
        update in-memory aggregates until the watermark advances past a
        ``window_end``.
        """
        payload = json.loads(raw.decode("utf-8"))
        event_time = parse_event_time(str(payload["timestamp"]))
        closed, dropped_late = self._engine.ingest(
            bot_id=int(payload["bot_id"]),
            event_time=event_time,
            metrics=payload["metrics"],
        )
        if dropped_late:
            self._dropped_late += 1
        for window_event in closed:
            self._emit(window_event)
        self._consumed += 1

    def run(self) -> int:
        """Subscribe, poll forever (until stop), then flush and return exit code."""
        self._consumer.subscribe([self._input_topic])
        print(
            "Baseline window starting: "
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
                self._handle_message(msg.value())
                self._consumer.commit(asynchronous=False)
        except Exception as exc:  # noqa: BLE001 - surface in stderr then non-zero
            print(f"Baseline window failed: {exc}", file=self._stderr, flush=True)
            exit_code = 1
        finally:
            for window_event in self._engine.flush_remaining():
                self._emit(window_event)
            remaining = self._producer.flush(30)
            self._consumer.close()
            print(
                "Baseline window stopped: "
                f"consumed={self._consumed} produced={self._produced} "
                f"dropped_late={self._dropped_late} flush_remaining={remaining}",
                file=self._stderr,
                flush=True,
            )
            if remaining > 0:
                exit_code = 1
        return exit_code


def build_service_from_env(
    environ: Mapping[str, str] | None = None,
    stderr: IO[str] = sys.stderr,
) -> BaselineWindowService:
    """Build a service from Twelve-Factor environment variables.

    Recognized keys: ``KAFKA_BOOTSTRAP_SERVERS``, ``KAFKA_INPUT_TOPIC``,
    ``KAFKA_OUTPUT_TOPIC``, ``KAFKA_GROUP_ID``. Empty strings fall back to
    in-cluster defaults.
    """
    env = os.environ if environ is None else environ
    return BaselineWindowService(
        bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP).strip()
        or DEFAULT_BOOTSTRAP,
        input_topic=env.get("KAFKA_INPUT_TOPIC", DEFAULT_INPUT_TOPIC).strip()
        or DEFAULT_INPUT_TOPIC,
        output_topic=env.get("KAFKA_OUTPUT_TOPIC", DEFAULT_OUTPUT_TOPIC).strip()
        or DEFAULT_OUTPUT_TOPIC,
        group_id=env.get("KAFKA_GROUP_ID", DEFAULT_GROUP).strip() or DEFAULT_GROUP,
        stderr=stderr,
    )


def main() -> None:
    """Container / CLI entry: wire signals, run until stop, exit with status."""
    service = build_service_from_env()
    signal.signal(signal.SIGINT, service.request_stop)
    signal.signal(signal.SIGTERM, service.request_stop)
    code = service.run()
    raise SystemExit(code)


if __name__ == "__main__":
    main()
