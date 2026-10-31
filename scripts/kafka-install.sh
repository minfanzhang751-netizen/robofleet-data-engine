#!/usr/bin/env bash
# Install Strimzi + Kafka + robot-telemetry on Kind context kind-robofleet.
# Single source of truth for Module 2.2 install (see docs/module-2.md).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

kubectl config use-context kind-robofleet

kubectl apply -f k8s/kafka/namespace.yaml

helm upgrade --install strimzi-kafka-operator \
  oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --version 1.2.0 \
  --namespace kafka \
  --values k8s/strimzi/values.yaml \
  --wait

kubectl apply -f k8s/kafka/kafka-cluster.yaml
kubectl -n kafka wait kafka/robofleet --for=condition=Ready --timeout=600s

kubectl apply -f k8s/kafka/topic-robot-telemetry.yaml
kubectl -n kafka wait kafkatopic/robot-telemetry --for=condition=Ready --timeout=120s

echo "kafka_install_ok"
kubectl -n kafka get kafka,kafkanodepool,kafkatopic
kubectl -n kafka get svc | grep -E 'bootstrap|external|dual-role-0' || true
