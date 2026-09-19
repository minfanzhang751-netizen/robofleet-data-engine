# Module 2: Kind cluster, Kafka, and host producer

Local Kubernetes “bed”, in-cluster Strimzi Kafka, then host-side Python
produce/consume. Follow the sections in order (2.1 → 2.2 → 2.3). The
checked-in Kind config already includes host port maps used in 2.3; changing
those maps requires a full Kind rebuild, then reinstalling 2.2.

| Step | Goal | Success when |
|------|------|--------------|
| [2.1](#21-kind-development-cluster) | Kind cluster `robofleet` | Node Ready; empty cluster is OK |
| [2.2](#22-in-cluster-kafka-strimzi) | Strimzi + Kafka + topic | In-cluster console produce/consume works |
| [2.3](#23-host-access--python-producer) | Host + `bot_generator` → Kafka | Host TCP + Python E2E; SIGTERM flushes |

## Shared prerequisites

- Docker Desktop running (WSL2 backend; Ubuntu integration enabled)
- `kubectl`, `kind`, and (from 2.2 onward) `helm`
- Commands from the repository root in Ubuntu

---

## 2.1 Kind development cluster

Creates the local Kubernetes bed only. Strimzi/Kafka/Python are not required
for this step to count as done.

### Create

```bash
kind create cluster --name robofleet --config k8s/kind/cluster-config.yaml
```

Kind sets your kubectl context to `kind-robofleet`.

The config maps host Kafka ports for [2.3](#23-host-access--python-producer)
(`9092`/`9093`). Changing `extraPortMappings` requires delete + create, then
reinstall from [2.2](#22-in-cluster-kafka-strimzi).

### Verify

```bash
kubectl config current-context
kubectl get nodes
kind get clusters
```

Expected:

- Current context: `kind-robofleet`
- One node named like `robofleet-control-plane`
- `STATUS=Ready`, `ROLES=control-plane`
- `kind get clusters` lists `robofleet`

### Keep running / delete / rebuild

Leave `robofleet` running for 2.2. Tear down only when intentional:

```bash
kind delete cluster --name robofleet
```

Rebuild:

```bash
kind delete cluster --name robofleet
kind create cluster --name robofleet --config k8s/kind/cluster-config.yaml
kubectl get nodes
```

### Common issues (2.1)

| Symptom | Likely cause | What to try |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop stopped or WSL integration off | Start Desktop; confirm Ubuntu integration; `wsl --shutdown` and reopen Ubuntu |
| `permission denied ... docker.sock` | User not in `docker` group | `sudo usermod -aG docker $USER`, then new shell / `newgrp docker` |
| Wrong cluster in kubectl | Context points elsewhere | `kubectl config use-context kind-robofleet` |
| `Create: ... already exists` | Cluster name still present | `kind get clusters`; delete or pick another name |
| Node stuck `NotReady` | Resource or CNI still starting | Wait a minute; `kubectl get nodes`; check Docker memory |

---

## 2.2 In-cluster Kafka (Strimzi)

Installs Strimzi Operator, a single-node KRaft Kafka cluster, and the
`robot-telemetry` topic on the Kind cluster from 2.1. Verification here is
**in-cluster only**. Host `localhost` access and the Python producer are
[2.3](#23-host-access--python-producer).

### What each step is doing

1. **Namespace `kafka`** — isolates broker and operator resources.
2. **Helm install Strimzi** — deploys the Cluster Operator Pod. It watches
   `Kafka` / `KafkaTopic` CRs and creates real Pods/Services for you.
3. **Apply `Kafka` + `KafkaNodePool`** — one KRaft node as controller and
   broker (internal listener on 9092; external NodePort for host use in 2.3).
4. **Apply `KafkaTopic`** — declares `robot-telemetry` (6 partitions, RF=1).
5. **Console produce/consume inside the broker Pod** — proves the post office
   works before relying on the host or Python.

### Install (from repository root)

```bash
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
```

Pinned versions:

| Component | Version |
|---|---|
| Strimzi Helm chart | `1.2.0` (`oci://quay.io/strimzi-helm/strimzi-kafka-operator`) |
| Kafka (in CR) | `4.3.1` / metadata `4.3-IV0` |

### Verify resources

```bash
kubectl -n kafka get pods
kubectl -n kafka get kafka,kafkanodepool,kafkatopic
kubectl -n kafka get svc robofleet-kafka-bootstrap
```

Expected:

- Operator, `robofleet-dual-role-0`, and entity-operator Pods Ready
- `kafka/robofleet` Ready=`True`
- `kafkatopic/robot-telemetry` Ready=`True`, 6 partitions, RF=1
- Bootstrap Service: `robofleet-kafka-bootstrap` port `9092`
- In-cluster DNS: `robofleet-kafka-bootstrap.kafka.svc:9092`

### In-cluster produce / consume probe

Produce one keyed message (key `42`):

```bash
printf '42:{"schema_version":"1","event_id":"module-2-2-probe","bot_id":42}\n' | \
  kubectl -n kafka exec -i robofleet-dual-role-0 -- \
    /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic robot-telemetry \
      --reader-property parse.key=true \
      --reader-property key.separator=:
```

Consume one message from the beginning:

```bash
kubectl -n kafka exec robofleet-dual-role-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic robot-telemetry \
    --from-beginning \
    --formatter-property print.key=true \
    --formatter-property key.separator=: \
    --timeout-ms 10000 \
    --max-messages 1
```

You should see the key and JSON value. Connecting from the Ubuntu host to
`localhost:9092` is covered in [2.3](#23-host-access--python-producer)
(requires the Kind port maps and external listener already in the checked-in
manifests).

### Tear down Kafka only (keep Kind)

```bash
kubectl delete -f k8s/kafka/topic-robot-telemetry.yaml --ignore-not-found
kubectl delete -f k8s/kafka/kafka-cluster.yaml --ignore-not-found
helm uninstall strimzi-kafka-operator --namespace kafka
# Optional: remove leftover PVCs after broker delete
kubectl -n kafka delete pvc -l strimzi.io/cluster=robofleet --ignore-not-found
```

### Common issues (2.2)

| Symptom | Likely cause | What to try |
|---|---|---|
| Helm hangs / image pull errors | Network / registry | Retry; check Docker Desktop network |
| Kafka never Ready | PVC Pending or image pull | `kubectl -n kafka get pvc,pods,events` |
| Topic not Ready | Entity operator / cluster not Ready | Wait for Kafka Ready first; check topic Operator logs |
| Wrong context | Not on Kind cluster | `kubectl config use-context kind-robofleet` |

If you recreate the Kind cluster (required when changing port mappings),
rerun the install section above afterward.

---

## 2.3 Host access + Python producer

Connects the Ubuntu host (and `bot_generator.py`) to in-cluster Kafka via Kind
port mappings and the Strimzi external listener.

### What each step is doing

1. **Kind `extraPortMappings`** — opens host ports into the Kind node:
   - `localhost:9092` → NodePort `30092` (external bootstrap Service)
   - `localhost:9093` → NodePort `30093` (broker 0; advertised to clients)
2. **Rebuild Kind** — port mappings apply only at cluster create time (skip if
   you already created with the current `cluster-config.yaml`).
3. **Reinstall [2.2](#22-in-cluster-kafka-strimzi)** — namespace, Strimzi Helm,
   Kafka CR, topic.
4. **External listener** — Kafka advertises `localhost:9093` so host clients
   are not redirected to cluster-internal DNS names.
5. **`kafka_sink.py` + env** — when `KAFKA_BOOTSTRAP_SERVERS` is set, events
   go to Kafka with key=`bot_id`; otherwise stdout behaves as Module 1.

### Rebuild Kind (required once for port maps)

```bash
cd ~/projects/robofleet-data-engine
kind delete cluster --name robofleet
kind create cluster --name robofleet --config k8s/kind/cluster-config.yaml
kubectl wait --for=condition=Ready node --all --timeout=120s
```

Confirm Docker published the ports:

```bash
docker ps --format '{{.Names}} {{.Ports}}' | grep robofleet
# expect: 0.0.0.0:9092->30092/tcp and 0.0.0.0:9093->30093/tcp
```

### Reinstall Kafka stack after rebuild

Follow [2.2 install](#install-from-repository-root), or:

```bash
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
```

External Services should look like:

```bash
kubectl -n kafka get svc | grep -E 'external|dual-role-0'
# robofleet-kafka-external-bootstrap  NodePort  ... 9094:30092/TCP
# robofleet-dual-role-0               NodePort  ... 9094:30093/TCP
```

### Host network smoke

```bash
python3 - <<'PY'
import socket
for port in (9092, 9093):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.close()
    print(f"tcp_ok localhost:{port}")
PY
```

### Python producer setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Requires a wheel for your Python; 3.14 works with confluent-kafka==2.15.1.
cp .env.example .env
set -a && source .env && set +a
```

Run (low rate for a short demo):

```bash
python bot_generator.py --eps 2 --bots 10
# Ctrl+C / SIGTERM should print graceful stop on stderr after flush
```

Without `KAFKA_BOOTSTRAP_SERVERS`, output stays on stdout (Module 1 behavior).

### Consume from the host to verify

```bash
python - <<'PY'
import json, os, time
from confluent_kafka import Consumer, KafkaException

consumer = Consumer({
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": f"verify-{int(time.time())}",
    "auto.offset.reset": "latest",
})
consumer.subscribe([os.environ.get("KAFKA_TOPIC", "robot-telemetry")])
print("waiting for messages...")
deadline = time.time() + 30
while time.time() < deadline:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        raise KafkaException(msg.error())
    print("key=", msg.key(), "value=", msg.value().decode())
    event = json.loads(msg.value())
    assert event["schema_version"] == "1"
    assert msg.key() == str(event["bot_id"]).encode()
    break
else:
    raise SystemExit("no message received")
consumer.close()
print("e2e_ok")
PY
```

### Why two host ports?

Kafka clients connect to the **bootstrap** address first (`localhost:9092`),
then the broker returns **advertised** addresses for produce/fetch. For a
single Kind node we advertise `localhost:9093` so the second hop also works
from the host. Mapping only bootstrap is not enough.

### Common issues (2.3)

| Symptom | Likely cause | What to try |
|---|---|---|
| `localhost:9092` connection refused | Kind created without port maps / Desktop down | Recreate with current `cluster-config.yaml`; check `docker ps` ports |
| Metadata shows `*.svc` hosts | advertisedHost not set | Confirm external listener block in `kafka-cluster.yaml` |
| `pip install confluent-kafka` builds from source and fails | No wheel / no gcc | Pin `confluent-kafka==2.15.1` (has cp314 wheels) or install `build-essential` + `librdkafka-dev` |
| Idempotence PID warnings on first produce | Broker still loading | Retry; first messages usually succeed after a short wait |

### Files touched in this module

- `k8s/kind/cluster-config.yaml` — host port maps
- `k8s/kafka/kafka-cluster.yaml` — external listener
- `kafka_sink.py` — confluent-kafka producer wrapper
- `bot_generator.py` — pluggable sink + graceful `close()`
- `requirements.txt`, `.env.example`
