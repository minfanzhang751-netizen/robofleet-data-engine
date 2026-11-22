# Module 1: Telemetry generator

Host-side fleet simulator. Emits one JSON telemetry event per line on
`stdout` at a configurable EPS; operational logs go to `stderr`. No Kafka
or Kubernetes are required for this module alone.

```text
bot_generator.py
    → stdout  (TelemetryEvent v1, JSON Lines)
    → stderr  (rate warnings / lifecycle)
```

With Module 2 configured, the same process uses `KafkaEventSink` so lines
land on topic `robot-telemetry` instead of (or in addition to) pure stdout
demos. See [module-2.md](module-2.md).

## Prerequisites

- Python 3.11+ (developed on 3.14)
- Repository venv + `pip install -r requirements.txt` (stdlib-only for the
  generator itself; Kafka deps are for the Module 2 sink path)
- Commands from the repository root

## Contract

Frozen fields and diagnostic thresholds live in
[event-schema-v1.md](event-schema-v1.md). Implementation:
[`bot_generator.py`](../bot_generator.py) (`TelemetryEvent`,
`select_diagnostic`, `SCHEMA_VERSION = "1"`).

Threshold hits write a short multi-sentence operator log (what Module 4
reads via window `status_log`). Nominal metrics keep one general status
sentence.

## Run (stdout only)

```bash
source .venv/bin/activate
python bot_generator.py --eps 5 --bots 100
```

| Flag | Meaning | Default |
|------|---------|---------|
| `--eps` | Events per second | 5 |
| `--bots` | Fleet size (`bot_id` from 1..N) | 100 |

Stop with Ctrl+C or `SIGTERM`. Handlers set a stop event; the loop exits on
a safe boundary and the sink `close()`s (important once Kafka is wired).

Sample one line:

```bash
python bot_generator.py --eps 1 --bots 3 | head -n 1 | python -m json.tool
```

## Run into Kafka (Module 2)

```bash
cp -n .env.example .env
set -a && source .env && set +a
python bot_generator.py --eps 5 --bots 100
```

Requires Kind + Strimzi Ready ([module-2.md](module-2.md)). Message key is
UTF-8 decimal `bot_id` ([event-schema-v1.md](event-schema-v1.md)#kafka-key).

## Verify

```bash
python -m unittest tests.test_bot_generator -v
python -m py_compile bot_generator.py
```

## Notes

- `timestamp` is **event time** (UTC ISO8601). Retries must keep the original
  `event_id` and `timestamp`.
- Incompatible field changes require a new `schema_version`; additive fields
  only when old consumers can ignore them.
- Module 3+ consume this contract via Kafka; do not invent a parallel host
  schema for windows or classified records.
