#!/usr/bin/env bash
# Module 4: device plugin, vLLM Deployment, and the classification DLQ topic.
#
# Does not delete or recreate the Kind cluster. GPU passthrough requires the
# host checks, node image, and cluster recreate in docs/module-4.md first.
# This script registers the RuntimeClass and device plugin that advertise
# nvidia.com/gpu, then rolls out vLLM.
#
# The vLLM image is large and multi-arch. kind load fails on its attestation
# digest. Import it before this script:
#   docker save vllm/vllm-openai:v0.11.0 | docker exec -i robofleet-control-plane \
#     ctr --namespace=k8s.io images import --snapshotter=overlayfs -
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CLUSTER_NAME="${KIND_CLUSTER_NAME:-robofleet}"

kubectl config use-context "kind-${CLUSTER_NAME}"

echo "Applying classification DLQ topic..."
kubectl apply -f k8s/kafka/topic-robot-diagnostics-dlq.yaml
kubectl -n kafka wait kafkatopic/robot-diagnostics-dlq --for=condition=Ready --timeout=120s

echo "Applying NVIDIA RuntimeClass..."
kubectl apply -f k8s/gpu/runtime-class.yaml

echo "Applying NVIDIA device plugin..."
kubectl apply -f k8s/gpu/nvidia-device-plugin.yaml
kubectl -n kube-system rollout status daemonset/nvidia-device-plugin-daemonset --timeout=180s

echo "Deploying vLLM..."
kubectl apply -f k8s/stream/namespace.yaml
kubectl apply -f k8s/gpu/vllm-model-cache-pvc.yaml
kubectl apply -f k8s/gpu/vllm.yaml
# First model download can take many minutes. Matches progressDeadlineSeconds.
kubectl -n robofleet rollout status deployment/vllm --timeout=1800s

echo "gpu_install_ok"
kubectl -n robofleet get pods -l app=vllm
echo "Confirm the 4090: kubectl -n robofleet exec deploy/vllm -- nvidia-smi"
