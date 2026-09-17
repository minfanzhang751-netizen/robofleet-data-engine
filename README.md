# RoboFleet Telemetry Engine

Kubernetes-native stream processing and MLOps platform. A simulated fleet of autonomous robots emits telemetry and unstructured logs; events are processed in real time and classified via local GPU LLM inference.

## Current status

Phase 1 (data generator) is complete. Later phases: Kafka on local Kubernetes, stream processing, vLLM on RTX 4090, Prometheus/Grafana.

## Prerequisites

- Python 3.11+ (developed on 3.14)
- Git

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the generator

Events are evenly paced and written to `stdout` as one compact JSON object per
line. Operational messages and schedule warnings go to `stderr`, so stdout can
be piped directly into another process. Stop with Ctrl+C; SIGINT and SIGTERM
both trigger graceful shutdown.

```bash
python bot_generator.py --eps 5 --bots 100
```

| Flag | Meaning | Default |
|------|---------|---------|
| `--eps` | Events per second | 5 |
| `--bots` | Fleet size | 100 |

The event-time field, units, diagnostic thresholds, compatibility rules, and
future Kafka key are defined in the
[Telemetry Event Contract v1](docs/event-schema-v1.md).

## Test

The test suite uses only the Python standard library:

```bash
python -m unittest discover -s tests -v
python -m py_compile bot_generator.py tests/test_bot_generator.py
```

## Architecture

See [PROJECT_PLAN.md](PROJECT_PLAN.md).
