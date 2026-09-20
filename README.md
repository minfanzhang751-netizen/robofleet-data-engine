# RoboFleet Telemetry Engine

Kubernetes-native stream processing and MLOps platform. A simulated fleet of autonomous robots emits telemetry and unstructured logs; events are processed in real time and classified via local GPU LLM inference.

## Current status

- Module 1: hardened telemetry generator (schema v1, SIGTERM, diagnostics)
- Module 2: Kind + Strimzi Kafka + host producer — [runbook](docs/module-2.md)
  (sections 2.1 / 2.2 / 2.3); producer failure/exit contract hardened

Later: stream processing, vLLM on RTX 4090, Prometheus/Grafana.

## Prerequisites

- Python 3.11+ (developed on 3.14)
- Git
- For Module 2.1+: Docker Desktop (WSL2), `kubectl`, `kind` (and `helm` for 2.2+)

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the generator

Events are evenly paced. By default each event is one compact JSON object on
`stdout`. Set `KAFKA_BOOTSTRAP_SERVERS` (see `.env.example`) to publish to
Kafka instead; operational messages still go to `stderr`. Stop with Ctrl+C;
SIGINT and SIGTERM both flush the sink and shut down gracefully. If Kafka
flush leaves unconfirmed messages or delivery failures occurred, the process
exits non-zero after the graceful-stop line on stderr.

```bash
python bot_generator.py --eps 5 --bots 100
```

| Flag | Meaning | Default |
|------|---------|---------|
| `--eps` | Events per second | 5 |
| `--bots` | Fleet size | 100 |

The event-time field, units, diagnostic thresholds, compatibility rules, and
Kafka key encoding are defined in the
[Telemetry Event Contract v1](docs/event-schema-v1.md).

## Test

Install deps from `requirements.txt` (includes `confluent-kafka` for the Kafka
sink tests; stdout-only paths still use the standard library):

```bash
python -m unittest discover -s tests -v
python -m py_compile bot_generator.py kafka_sink.py tests/test_bot_generator.py tests/test_kafka_sink.py
```

## Architecture

See [PROJECT_PLAN.md](PROJECT_PLAN.md).
