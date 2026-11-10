# Module 4: In-cluster vLLM on the RTX 4090

The LLM client keeps reading Spark windows from `robot-telemetry-windows` and
writing `robot-diagnostics-classified`. Classification runs in a separate vLLM
pod that holds the GPU. The client pod stays on CPU at 256Mi.

```text
bot_generator (host)
    → robot-telemetry  (diagnostics status log on each event)
         → spark-window → robot-telemetry-windows  (status_log, ≤8 lines)
              → llm-client (CPU) → POST /v1/chat/completions
                    → vllm pod (nvidia.com/gpu: 1, Qwen2.5-3B-Instruct)
              → robot-diagnostics-classified
              → robot-diagnostics-dlq   (no legal label after 3 attempts)
```

Baseline windows are still not consumed. `label` stays `Safe`, `Warning`, or
`Critical`. `latency_ms` measures only `classify()`. `model` is the served
model id (`Qwen/Qwen2.5-3B-Instruct` unless `VLLM_MODEL` overrides it).

## 1. Prove the Docker engine can see the 4090

Kind uses whichever Docker daemon `docker` talks to. On Docker Desktop that
daemon is not `systemctl` inside the Ubuntu distro.

```bash
docker run --rm --gpus all nvidia/cuda:12.6.0-base-ubuntu22.04 nvidia-smi
```

The table must list an RTX 4090. If it does not, fix the Windows NVIDIA driver
and Docker Desktop's WSL2 GPU support before changing the cluster.

Do not install `nvidia-ctk` inside Ubuntu. That binary is not on this distro,
and Ubuntu's Docker config is not the engine Kind uses.

Docker Desktop's engine must use the NVIDIA runtime by default, or the Kind
node image's `NVIDIA_VISIBLE_DEVICES` is ignored. In Docker Desktop, open
Settings, then Docker Engine, and add `"default-runtime": "nvidia"` beside the
existing `builder` and `experimental` keys. Do not delete those keys. Apply
and restart. Confirm with:

```bash
docker info --format 'default={{.DefaultRuntime}}'
```

The line must be `default=nvidia`.

## 2. Build the node image and recreate the Kind cluster

`./scripts/build-kind-gpu-node.sh` builds `kindest/node:v1.37.0-gpu` from
`k8s/kind/Dockerfile.node`. The image sets `NVIDIA_VISIBLE_DEVICES=all` so
Docker Desktop injects the 4090 into the Kind node, and it installs
`nvidia-container-runtime` for pods. `k8s/kind/cluster-config.yaml` points the
control-plane at that image and registers a containerd runtime named `nvidia`.
The `/dev/null` mount is still there; it is not what makes a pod see the GPU.

Applying that file to an existing cluster does nothing. Recreate, then
reinstall Kafka and the stream workloads. Topics, the imported vLLM image, and
PVCs (including the model cache) come back empty.

```bash
./scripts/build-kind-gpu-node.sh
kind delete cluster --name robofleet
kind create cluster --name robofleet --config k8s/kind/cluster-config.yaml
docker exec robofleet-control-plane nvidia-smi
./scripts/kafka-install.sh
./scripts/stream-install.sh
```

`nvidia-smi` inside the node must print the RTX 4090 before the install
scripts. `stream-install.sh` also creates `robot-diagnostics-dlq` and rolls
out `llm-client` with `CLASSIFIER=vllm`. Until vLLM is Ready, failed calls
land on the DLQ after 3 attempts. They are not relabeled by the mock rules.

## 3. Device plugin and vLLM

Pull the pinned image on the host (about 10GB). `kind load` fails on this
multi-arch image because it imports an attestation digest that `docker save`
does not contain. Import with `ctr` instead:

```bash
docker pull vllm/vllm-openai:v0.11.0
docker save vllm/vllm-openai:v0.11.0 | docker exec -i robofleet-control-plane ctr --namespace=k8s.io images import --snapshotter=overlayfs -
./scripts/gpu-install.sh
```

`gpu-install.sh` applies:

- `k8s/gpu/runtime-class.yaml` (handler `nvidia`, the containerd runtime from the node image)
- `k8s/gpu/nvidia-device-plugin.yaml` (host driver stays on the machine; the plugin pod uses that RuntimeClass)
- `k8s/gpu/vllm-model-cache-pvc.yaml` (Hugging Face cache, 20Gi)
- `k8s/gpu/vllm.yaml` (`limits` and `requests` include `nvidia.com/gpu: 1`; one replica, Recreate)

The vLLM Service is named `vllm`. The Deployment sets `enableServiceLinks: false` so Kubernetes does not inject `VLLM_PORT` as a URI. The device plugin sets `NVIDIA_VISIBLE_DEVICES` when it allocates the card.

The first start downloads `Qwen/Qwen2.5-3B-Instruct` into the PVC. `kubectl logs` does not show the Hugging Face progress bar. `kubectl -n robofleet exec deploy/vllm -- du -sh /root/.cache/huggingface` grows until the weights are on disk (about 12Gi with the cache layout), then the process loads them. Later restarts read that cache. Weights are not baked into an image and are not pushed anywhere. The rollout wait is 30 minutes.

## 4. Acceptance

```bash
kubectl describe node | grep nvidia.com/gpu
kubectl -n robofleet describe pod -l app=vllm | grep -A2 'Limits:'
kubectl -n robofleet exec deploy/vllm -- nvidia-smi
```

Expect allocatable `nvidia.com/gpu: 1`, the vLLM pod limit `nvidia.com/gpu: 1`,
and `nvidia-smi` printing the RTX 4090. A client that calls a process on the
host port is not this setup.

Generate a little traffic and read one classified record:

```bash
source .venv/bin/activate
set -a && source .env && set +a
python bot_generator.py --eps 5 --bots 5
```

In another shell, after a window closes (watermark needs event time to move
about 30s past the window end):

```bash
kubectl -n kafka exec robofleet-dual-role-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic robot-diagnostics-classified \
  --from-beginning --max-messages 1
```

`model` is `Qwen/Qwen2.5-3B-Instruct`. `label` is one of the three enum values.
`latency_ms` is the client-side classify call.

## DLQ record

`robot-diagnostics-dlq` is not a classified event. Each value is JSON:

| Field | Meaning |
|---|---|
| `schema_version` | `"1"` |
| `reason` | Why this attempt failed |
| `failed_at` | UTC ISO8601 |
| `window_raw` | Original Kafka value text |
| `window` | Parsed object when the JSON was an object |

Invalid window JSON is written here without calling vLLM. A reachable vLLM
that returns a non-enum label is retried up to `CLASSIFIER_MAX_ATTEMPTS`
(default 3). The offset is committed only after the classified produce or the
DLQ produce is acknowledged.

## Swap the model

Qwen2.5-7B-Instruct fits the same 4090. Change `VLLM_MODEL` on the client and
the `--model` arg on the vLLM Deployment together, then roll both. The
classified field names stay the same.

To run the Module 3 mock again, remove `CLASSIFIER` (or set it to `mock`) on
the llm-client Deployment. `CLASSIFIER=vllm` never calls `MockClassifier`.
