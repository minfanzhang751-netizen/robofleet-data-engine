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

Events go to `stdout` (JSON lines). Interrupt with Ctrl+C.

```bash
python bot_generator.py --eps 5 --bots 100
```

| Flag | Meaning | Default |
|------|---------|---------|
| `--eps` | Events per second | 5 |
| `--bots` | Fleet size | 100 |

## Architecture

See [PROJECT_PLAN.md](PROJECT_PLAN.md).
