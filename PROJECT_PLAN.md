# Project Context & Architecture Blueprint: RoboFleet Telemetry Engine

## 1. Role & Objective
You are an expert Cloud Data Infrastructure and MLOps Engineer. You are acting as my pair-programming assistant.
Our objective is to build a highly scalable, Kubernetes-native stream processing and MLOps platform from scratch. We are simulating a fleet of autonomous robots streaming telemetry and unstructured logs, processing them in real-time, and using a local GPU for LLM inference.

## 2. Project Architecture (The Big Picture)
The system consists of 5 core modules, built for a WSL 2 (Ubuntu) local environment, simulating a production-grade cloud deployment:

- **Module 1: Data Generator (Completed)**
  A Python script generating mock robot telemetry (JSON) and unstructured
  status logs at a configurable EPS, outputting to `stdout` (Kafka via Module
  2). See [docs/module-1.md](docs/module-1.md).
- **Module 2: Message Broker (Completed locally)**
  Apache Kafka on Kind via the Strimzi Operator (KRaft single node), topic
  `robot-telemetry`, and a host Python producer (`kafka_sink.py`) publishing
  v1 telemetry with `bot_id` as the message key. See
  [docs/module-2.md](docs/module-2.md).
- **Module 3: Stream Processor (Completed locally)**
  In-cluster PySpark Structured Streaming (primary) plus handwritten baseline
  comparator: event-time sliding windows (60s/10s, 30s watermark) into Kafka
  topics (including a bounded `status_log`), then an LLM client that calls
  in-cluster vLLM (mock remains available when `CLASSIFIER=mock`).
- **Module 4: Edge MLOps Layer (GPU Accelerated)**
  vLLM (`Qwen/Qwen2.5-3B-Instruct`) in the Kind cluster on the RTX 4090 via the
  NVIDIA device plugin (`nvidia.com/gpu: 1`). The client sends the window
  status log plus averages and writes `Safe` / `Warning` / `Critical`. See
  [docs/module-4.md](docs/module-4.md).
- **Module 5: Observability (Completed locally)**
  kube-prometheus-stack (Prometheus, Grafana, Alertmanager), kafka-exporter
  for consumer lag, llm-client and vLLM `/metrics`, DCGM Exporter for GPU,
  PrometheusRules, and a RoboFleet Grafana dashboard. See
  [docs/module-5.md](docs/module-5.md).

## 3. Engineering Standards & Rules (CRITICAL)
Whenever you generate code or configurations for this project, you MUST adhere to the following L5 Senior Engineering standards:
1. **The Twelve-Factor App:** Applications must be stateless. Logs go to `stdout`/`stderr`. No local file storage for data processing.
2. **Type Hints & Clean Code:** All Python code must have strict type hints (`typing`), modular functions, and docstrings.
3. **Idempotency & Timestamps:** Always use UTC ISO8601 for timestamps (`datetime.now(timezone.utc).isoformat()`). Always expect or generate UUIDs for event tracing.
4. **Graceful Shutdowns:** All continuous loops must handle `KeyboardInterrupt` or `SIGTERM` gracefully.
5. **Infrastructure as Code (IaC):** K8s deployments must be written in clean, declarative YAML files.

## 4. Current Status & Next Steps
Modules 1–5 are implemented in-repo for the local Kind + Docker Desktop path.
Runbooks: [module-1](docs/module-1.md), [module-2](docs/module-2.md),
[module-3](docs/module-3.md), [module-4](docs/module-4.md),
[module-5](docs/module-5.md).
