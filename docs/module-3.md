# Module 3: Stream Processor (PySpark + baseline)

In-cluster event-time sliding windows over `robot-telemetry`, with PySpark
Structured Streaming as the primary path and a handwritten Kafka consumer as
a side-by-side comparison baseline. Window results feed an independent LLM
client (vLLM when `CLASSIFIER=vllm`; mock remains for `CLASSIFIER=mock`).
Module 4's GPU path is [module-4.md](module-4.md).

## Architecture

```text
bot_generator (host)
    → robot-telemetry
         → spark-window Pod (local[2]) → robot-telemetry-windows
         → baseline-window Pod (memory) → robot-telemetry-windows-baseline
    → llm-client Pod ← robot-telemetry-windows
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

## Benchmarks

Formal names and gaps (no end-to-end latency; broker-acked EPS / p99 not
collected): [metrics.md](metrics.md).

```bash
./scripts/bench-module-3.sh
# writes docs/benchmarks/module-3-results.md
```

| Metric | How to capture |
|--------|----------------|
| offered EPS | Generator `--eps` (target rate for the run; not broker-acked) |
| window emission latency | `bench-module-3.sh` writes p50/p95 of `emitted_at - window_end` for Spark and baseline windows whose `emitted_at` falls in the load interval |
| consumer lag | Same script: max partition lag before load and right after the generator stops, for Spark prefix groups, baseline, and the LLM client |
| Correctness | `python -m unittest tests.test_window_semantics -v` (gold averages) |
| Resources | `kubectl -n robofleet top pods` (install metrics-server if needed) |
| Comparison | Same offered EPS / duration: Spark vs baseline **window emission latency** + consumer lag + CPU/RSS side-by-side |

Template/results: [benchmarks/module-3-results.md](benchmarks/module-3-results.md).

## Design FAQ

**Why PySpark as primary?** Common stream-processing stack with a standard
event-time watermark and sliding-window API. Contracts stay stable if the job
later moves from `local[2]` toward cluster mode.

**Why keep a handwritten baseline?** Makes poll/commit and event-time window
semantics explicit, and lets the same load compare Spark vs baseline
consumer lag and window emission latency instead of trusting a single
black-box job.

**Why in-cluster only for processors?** Matches the K8s-native deployment story;
CPU/memory and consumer lag are pod-scoped. The load generator stays on the
host for fast iteration and is not part of the in-cluster processing path.

**Why a separate LLM client?** Isolates **classify latency** from windowing;
Module 4 swaps `MockClassifier` for the vLLM HTTP backend without changing
the `robot-diagnostics-classified` schema.

**Why `local[2]`?** Kind single-node MVP. Extends toward Spark Operator /
cluster mode later; contracts and topics stay stable.

**Known limits:** PLAINTEXT, RF=1, single-pod Spark, baseline memory state
(no checkpoint). Documented limits plus an upgrade path beat pretending the
local stack is HA.

## Tear down stream apps

```bash
kubectl delete -f k8s/stream/llm-client-deployment.yaml --ignore-not-found
kubectl delete -f k8s/stream/spark-window-deployment.yaml --ignore-not-found
kubectl delete -f k8s/stream/baseline-window-deployment.yaml --ignore-not-found
kubectl delete -f k8s/stream/spark-checkpoint-pvc.yaml --ignore-not-found
```
