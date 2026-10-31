#!/usr/bin/env bash
# Module 3 one-shot installer: topics → docker images → kind load → Deployments.
#
# Prerequisites: Kind cluster ``robofleet`` and Module 2 Kafka already Ready
# (``./scripts/kafka-install.sh``). This script does not start bot_generator;
# after success the three Pods idle-poll until telemetry arrives.
#
# Steps:
#   1. Apply Strimzi KafkaTopic CRs for windows / baseline / classified; wait Ready.
#   2. Build three local images (baseline, llm-client, spark).
#   3. kind load into the Kind node (images are not on a registry).
#   4. Apply namespace, Spark checkpoint PVC, three Deployments; wait rollout.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CLUSTER_NAME="${KIND_CLUSTER_NAME:-robofleet}"

kubectl config use-context "kind-${CLUSTER_NAME}"

echo "Applying Module 3 Kafka topics..."
kubectl apply -f k8s/kafka/topic-robot-telemetry-windows.yaml
kubectl apply -f k8s/kafka/topic-robot-telemetry-windows-baseline.yaml
kubectl apply -f k8s/kafka/topic-robot-diagnostics-classified.yaml
# apply accepts the CR; wait until Topic Operator has created partitions.
kubectl -n kafka wait kafkatopic/robot-telemetry-windows --for=condition=Ready --timeout=120s
kubectl -n kafka wait kafkatopic/robot-telemetry-windows-baseline --for=condition=Ready --timeout=120s
kubectl -n kafka wait kafkatopic/robot-diagnostics-classified --for=condition=Ready --timeout=120s

echo "Building images..."
# Context is repo root (``.’’) so Dockerfiles can COPY stream/ and services/.
docker build -t robofleet-baseline:local -f stream/Dockerfile.baseline .
docker build -t robofleet-llm-client:local -f services/llm_client/Dockerfile .
docker build -t robofleet-spark:local -f stream/Dockerfile.spark .

echo "Loading images into Kind (${CLUSTER_NAME})..."
# Kind nodes cannot see host Docker tags unless loaded (or pushed to a registry).
kind load docker-image robofleet-baseline:local --name "${CLUSTER_NAME}"
kind load docker-image robofleet-llm-client:local --name "${CLUSTER_NAME}"
kind load docker-image robofleet-spark:local --name "${CLUSTER_NAME}"

echo "Deploying stream workloads..."
# Order matters: namespace → PVC (Spark checkpoint) → Deployments.
kubectl apply -f k8s/stream/namespace.yaml
kubectl apply -f k8s/stream/spark-checkpoint-pvc.yaml
kubectl apply -f k8s/stream/baseline-window-deployment.yaml
kubectl apply -f k8s/stream/spark-window-deployment.yaml
kubectl apply -f k8s/stream/llm-client-deployment.yaml

# Spark gets a longer timeout (JVM + connector warmup).
kubectl -n robofleet rollout status deployment/baseline-window --timeout=180s
kubectl -n robofleet rollout status deployment/spark-window --timeout=300s
kubectl -n robofleet rollout status deployment/llm-client --timeout=180s

echo "stream_install_ok"
kubectl -n robofleet get pods
# grep may find nothing on a partial install; do not fail the script for display.
kubectl -n kafka get kafkatopic | grep -E 'windows|classified|robot-telemetry' || true
