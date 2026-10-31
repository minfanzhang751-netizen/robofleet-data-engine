# Module 3: Stream Processor (PySpark + baseline)

In-cluster event-time sliding windows over `robot-telemetry`, with PySpark
Structured Streaming as the primary path and a handwritten Kafka consumer as
the resume/interview comparison baseline. Window results feed an independent
LLM client (mock classifier today; Module 4 swaps the backend).

## Architecture

```text
bot_generator (host)
    → robot-telemetry
         → spark-window Pod (local[2]) → robot-telemetry-windows
         → baseline-window Pod (memory) → robot-telemetry-windows-baseline
    → llm-client Pod (mock) ← robot-telemetry-windows
         → robot-diagnostics-classified
```

Contracts:

- Input: [event-schema-v1.md](event-schema-v1.md)
- Windows: [window-schema-v1.md](window-schema-v1.md)
- Classified: [classified-schema-v1.md](classified-schema-v1.md)

Semantics (locked): event-time from `timestamp`; group by `bot_id`; sliding
**60s / 10s**; watermark lateness **30s**.

## Prerequisites

- Module 2 Kind + Strimzi Kafka Ready ([module-2.md](module-2.md))
- Docker Desktop; `kubectl`, `kind`, `helm`
- Host generator still uses `localhost:9092` / `.env` for load generation

## Install

```bash
# Kafka stack (if needed)
./scripts/kafka-install.sh

# Topics + images + Deployments
./scripts/stream-install.sh
```

Verify:

```bash
kubectl -n robofleet get pods
kubectl -n kafka get kafkatopic | grep -E 'windows|classified'
```

## Generate load (host)

```bash
source .venv/bin/activate
set -a && source .env && set +a
python bot_generator.py --eps 10 --bots 20
```

## Benchmarks (all six metric classes)

```bash
./scripts/bench-module-3.sh
# writes docs/benchmarks/module-3-results.md
```

| Metric | How to capture |
|--------|----------------|
| Throughput | Generator `--eps` × duration; window emit counts from consumer / logs |
| Latency | `bench-module-3.sh` writes p50/p95 of `emitted_at - window_end` for Spark and baseline windows emitted during the run |
| Lag | Same script writes max partition lag before load and immediately after the generator stops, for Spark prefix groups, baseline, and the LLM client |
| Correctness | `python -m unittest tests.test_window_semantics -v` (gold averages) |
| Resources | `kubectl -n robofleet top pods` (install metrics-server if needed) |
| Comparison | Same EPS/duration: Spark vs baseline lag + CPU/RSS side-by-side |

Template/results: [benchmarks/module-3-results.md](benchmarks/module-3-results.md).

## Interview FAQ

**Why PySpark as primary?** Industrial keyword density for MLE/data roles;
watermark + window API are standard talking points; upgrade path to cluster
mode is documented without rewriting contracts.

**Why keep a handwritten baseline?** Proves poll/commit/event-time understanding;
enables A/B numbers on the resume instead of a single black-box job.

**Why in-cluster only for processors?** Matches the K8s-native narrative; resource
and lag metrics are pod-scoped. Load generator stays on the host for fast
iteration (not on the critical production path story).

**Why a separate LLM client?** Isolates model latency from windowing; Module 4
replaces `MockClassifier` / HTTP backend without changing
`robot-diagnostics-classified` schema.

**Why `local[2]`?** Kind single-node MVP. Extends to Spark Operator / cluster
mode later; contracts and topics stay stable.

**Known limits:** PLAINTEXT, RF=1, single-pod Spark, baseline memory state
(no checkpoint). Honest limits + upgrade path beats a toy that pretends to be HA.

## Tear down stream apps

```bash
kubectl delete -f k8s/stream/llm-client-deployment.yaml --ignore-not-found
kubectl delete -f k8s/stream/spark-window-deployment.yaml --ignore-not-found
kubectl delete -f k8s/stream/baseline-window-deployment.yaml --ignore-not-found
kubectl delete -f k8s/stream/spark-checkpoint-pvc.yaml --ignore-not-found
```
