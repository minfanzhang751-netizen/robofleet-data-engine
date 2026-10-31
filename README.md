# RoboFleet Telemetry Engine

Kubernetes-native stream processing and MLOps platform. A simulated fleet of autonomous robots emits telemetry and unstructured logs; events are processed in real time and classified via local GPU LLM inference.

## Current status

- Module 1: hardened telemetry generator (schema v1, SIGTERM, diagnostics)
- Module 2: Kind + Strimzi Kafka + host producer — [runbook](docs/module-2.md)
- Module 3: in-cluster PySpark Structured Streaming + baseline comparator +
  mock LLM client — [runbook](docs/module-3.md)

Later: real vLLM on RTX 4090, Prometheus/Grafana.

## Prerequisites

- Python 3.11+ (developed on 3.14)
- Git
- For Module 2+: Docker Desktop (WSL2), `kubectl`, `kind`, `helm`

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the generator (host → Kafka)

```bash
cp -n .env.example .env
set -a && source .env && set +a
python bot_generator.py --eps 5 --bots 100
```

| Flag | Meaning | Default |
|------|---------|---------|
| `--eps` | Events per second | 5 |
| `--bots` | Fleet size | 100 |

Contracts: [telemetry v1](docs/event-schema-v1.md), [windows v1](docs/window-schema-v1.md),
[classified v1](docs/classified-schema-v1.md).

## Module 3 (stream) quick start

```bash
./scripts/kafka-install.sh    # if Kafka not already up
./scripts/stream-install.sh   # topics + images + Deployments
./scripts/bench-module-3.sh   # snapshot lag/resources under load
```

## Test

```bash
python -m unittest discover -s tests -v
python -m py_compile bot_generator.py kafka_sink.py stream/*.py services/llm_client/*.py
```

## Architecture

See [PROJECT_PLAN.md](PROJECT_PLAN.md).
