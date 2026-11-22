#!/usr/bin/env bash
# Module 5: kube-prometheus-stack, Kafka/DCGM exporters, ServiceMonitors, alerts.
#
# Prerequisites: Kind cluster with Module 2 Kafka Ready. Module 3 stream and
# Module 4 vLLM/GPU are required for classify latency and DCGM panels to fill.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CLUSTER_NAME="${KIND_CLUSTER_NAME:-robofleet}"
RELEASE_NAME="${PROM_RELEASE_NAME:-kube-prometheus-stack}"
CHART_VERSION="${PROM_CHART_VERSION:-69.8.2}"

kubectl config use-context "kind-${CLUSTER_NAME}"

echo "Creating observability namespace..."
kubectl apply -f k8s/observability/namespace.yaml

echo "Adding prometheus-community Helm repo..."
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update prometheus-community

echo "Installing ${RELEASE_NAME} (chart ${CHART_VERSION})..."
helm upgrade --install "${RELEASE_NAME}" prometheus-community/kube-prometheus-stack \
  --namespace observability \
  --version "${CHART_VERSION}" \
  --values k8s/observability/kube-prometheus-stack-values.yaml \
  --wait \
  --timeout 15m

echo "Applying Kafka exporter..."
kubectl apply -f k8s/observability/kafka-exporter.yaml
kubectl -n observability rollout status deployment/kafka-exporter --timeout=180s

echo "Applying DCGM exporter..."
kubectl apply -f k8s/observability/dcgm-exporter.yaml
kubectl -n observability rollout status daemonset/dcgm-exporter --timeout=300s

echo "Applying ServiceMonitors and alert rules..."
kubectl apply -f k8s/observability/llm-client-servicemonitor.yaml
kubectl apply -f k8s/observability/vllm-servicemonitor.yaml
kubectl apply -f k8s/observability/prometheus-rules.yaml

echo "Provisioning Grafana dashboard ConfigMap..."
kubectl -n observability create configmap robofleet-overview-dashboard \
  --from-file=robofleet-overview.json=k8s/observability/dashboards/robofleet-overview.json \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n observability label configmap/robofleet-overview-dashboard \
  grafana_dashboard=1 app=robofleet --overwrite

echo "observability_install_ok"
kubectl -n observability get pods
echo "Grafana NodePort 30300 (admin / robofleet). Prometheus NodePort 30090."
echo "Or: kubectl -n observability port-forward svc/${RELEASE_NAME}-grafana 3000:80"
