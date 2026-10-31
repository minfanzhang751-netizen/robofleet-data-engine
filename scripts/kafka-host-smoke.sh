#!/usr/bin/env bash
# Host-side smoke: TCP to Kind-mapped Kafka ports, optional short consume.
# See docs/module-2.md section 2.3.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC="${KAFKA_TOPIC:-robot-telemetry}"
CONSUME="${1:-}"

python3 - <<'PY'
import socket
import sys

for port in (9092, 9093):
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=5)
        s.close()
        print(f"tcp_ok localhost:{port}")
    except OSError as exc:
        print(f"tcp_fail localhost:{port}: {exc}", file=sys.stderr)
        sys.exit(1)
PY

if [[ "$CONSUME" != "--consume" ]]; then
  echo "host_smoke_tcp_ok"
  echo "hint: re-run with --consume after starting bot_generator to fetch one message"
  exit 0
fi

python3 - <<PY
import json, os, time, sys
from confluent_kafka import Consumer, KafkaException

bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "$BOOTSTRAP")
topic = os.environ.get("KAFKA_TOPIC", "$TOPIC")
consumer = Consumer({
    "bootstrap.servers": bootstrap,
    "group.id": f"verify-{int(time.time())}",
    "auto.offset.reset": "latest",
})
consumer.subscribe([topic])
print("waiting for messages...")
deadline = time.time() + 30
while time.time() < deadline:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        raise KafkaException(msg.error())
    print("key=", msg.key(), "value=", msg.value().decode())
    event = json.loads(msg.value())
    assert event["schema_version"] == "1"
    assert msg.key() == str(event["bot_id"]).encode()
    consumer.close()
    print("e2e_ok")
    sys.exit(0)
consumer.close()
raise SystemExit("no message received")
PY
