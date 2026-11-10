#!/usr/bin/env bash
# Build the Kind node image that carries NVIDIA_VISIBLE_DEVICES and
# nvidia-container-runtime. Tag matches k8s/kind/cluster-config.yaml.
# Does not create or delete a cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker build \
  -t kindest/node:v1.37.0-gpu \
  -f "${ROOT}/k8s/kind/Dockerfile.node" \
  "${ROOT}/k8s/kind"
