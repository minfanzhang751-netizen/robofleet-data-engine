# Module 5: Observability

Prometheus and Grafana run in the `observability` namespace. They scrape:

- **Kafka consumer lag** via `kafka-exporter` (groups matching `robofleet-.*`)
- **Classify latency** via llm-client `:8080/metrics` (`robofleet_classify_latency_ms`)
- **vLLM engine metrics** via `vllm:8000/metrics`
- **GPU** via DCGM Exporter (`DCGM_FI_DEV_*`)

Alert rules live in `k8s/observability/prometheus-rules.yaml` (lag, classify
p95, GPU memory/util).

```text
Kafka / llm-client / vLLM / GPU
    → ServiceMonitors
         → Prometheus
              → Grafana (RoboFleet Overview dashboard)
              → Alertmanager
```

## Prerequisites

- Kind cluster `robofleet` with Module 2 Kafka Ready (`./scripts/kafka-install.sh`)
- Module 3 stream workloads (`./scripts/stream-install.sh`) so llm-client exists
- Module 4 GPU path for DCGM and meaningful vLLM metrics (`docs/module-4.md`)
- Host tools: `kubectl`, `helm`

Rebuild/reload the llm-client image after pulling these changes so `/metrics`
is in the running Pod (`stream-install.sh` rebuilds it).

## Install

```bash
./scripts/observability-install.sh
```

The script installs `kube-prometheus-stack` (chart `69.8.2` by default), then
applies kafka-exporter, dcgm-exporter, ServiceMonitors, PrometheusRules, and
the Grafana dashboard ConfigMap.

## Open the UIs

NodePorts (Kind extraPortMappings are not required; Docker Desktop publishes
them on localhost):

| UI | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:30300 | `admin` / `robofleet` |
| Prometheus | http://localhost:30090 | none |

Port-forward alternative:

```bash
kubectl -n observability port-forward svc/kube-prometheus-stack-grafana 3000:80
kubectl -n observability port-forward svc/kube-prometheus-stack-prometheus 9090:9090
```

Dashboard: **RoboFleet Overview** (uid `robofleet-overview`).

## Generate traffic so panels move

```bash
source .venv/bin/activate
set -a && source .env && set +a
python bot_generator.py --eps 10 --bots 20
```

Leave it running a few minutes. Expect:

- Kafka lag series for `robofleet-*` consumer groups
- Classify p50/p95 after llm-client handles windows
- DCGM GPU util / memory / temp when vLLM is Loaded
- Alerts appear under Prometheus → Alerts when thresholds are breached

## Spot-check scrapes

```bash
kubectl -n robofleet port-forward svc/llm-client 8080:8080
curl -s localhost:8080/metrics | grep robofleet_classify

kubectl -n robofleet port-forward svc/vllm 8000:8000
curl -s localhost:8000/metrics | head

kubectl -n observability port-forward svc/dcgm-exporter 9400:9400
curl -s localhost:9400/metrics | grep DCGM_FI_DEV_GPU
```

## Notes

- DCGM needs the Module 4 `RuntimeClass` `nvidia` and a visible 4090. If the
  DaemonSet stays `ContainerCreating`, fix GPU injection first (`docs/module-4.md`).
- One-shot lag/latency snapshots remain in `./scripts/bench-module-3.sh`; this
  module is the continuous path.
- Grafana admin password is set in
  `k8s/observability/kube-prometheus-stack-values.yaml` for local demos only.
