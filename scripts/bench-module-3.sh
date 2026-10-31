#!/usr/bin/env bash
# Snapshot Module 3 resume metrics under host generator load.
#
# Captures (into docs/benchmarks/):
#   - Pod list and optional kubectl top (resources)
#   - Max consumer-group lag before load and immediately after the generator stops
#   - p50/p95 of emitted_at - window_end for Spark and baseline windows from this run
#
# Correctness remains the offline unittest noted at the end of the snapshot.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DURATION_SEC="${BENCH_DURATION_SEC:-60}"
EPS="${BENCH_EPS:-10}"
DRAIN_SEC="${BENCH_DRAIN_SEC:-40}"
OUT_DIR="${BENCH_OUT_DIR:-docs/benchmarks}"
mkdir -p "${OUT_DIR}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="${OUT_DIR}/module-3-results-${STAMP}.md"
SPARK_GROUP_PREFIX="robofleet-spark-windows"
BASELINE_GROUP="robofleet-baseline-windows"
LLM_GROUP="robofleet-llm-client"

if [[ -x .venv/bin/python ]]; then
  PYTHON=(.venv/bin/python)
else
  PYTHON=(python3)
fi

echo "Bench starting: duration=${DURATION_SEC}s eps=${EPS} drain=${DRAIN_SEC}s"

kubectl config use-context kind-robofleet

kafka_groups() {
  kubectl -n kafka exec robofleet-dual-role-0 -- \
    /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 \
      "$@"
}

append_lag_section() {
  local title="$1"
  {
    echo
    echo "## ${title}"
    echo
    echo "| Group | Lag (max) |"
    echo "|-------|-----------|"
  } >>"${OUT_FILE}"

  local listed spark_groups=() group
  listed="$(kafka_groups --list 2>/dev/null || true)"
  while IFS= read -r group; do
    [[ -n "${group}" ]] && spark_groups+=("${group}")
  done < <(printf '%s\n' "${listed}" | grep "^${SPARK_GROUP_PREFIX}" || true)

  if [[ ${#spark_groups[@]} -eq 0 ]]; then
    echo "| \`${SPARK_GROUP_PREFIX}*\` | group not found |" >>"${OUT_FILE}"
  else
    for group in "${spark_groups[@]}"; do
      append_lag_row "${group}"
    done
  fi
  append_lag_row "${BASELINE_GROUP}"
  append_lag_row "${LLM_GROUP}"
}

append_lag_row() {
  local group="$1"
  local described lag
  described="$(kafka_groups --describe --group "${group}" 2>&1 || true)"
  lag="$(printf '%s\n' "${described}" | "${PYTHON[@]}" scripts/bench_latency.py max-lag)"
  echo "| \`${group}\` | ${lag} |" >>"${OUT_FILE}"
}

{
  echo "# Module 3 benchmark snapshot (${STAMP})"
  echo
  echo "## Environment"
  echo
  echo "- Kind context: \`kind-robofleet\`"
  echo "- Generator EPS: \`${EPS}\`"
  echo "- Duration (s): \`${DURATION_SEC}\`"
  echo "- Drain wait (s): \`${DRAIN_SEC}\`"
  echo "- Git commit: \`$(git rev-parse --short HEAD 2>/dev/null || echo unknown)\`"
  echo
  echo "## Pods"
  echo
  echo '```'
  kubectl -n robofleet get pods -o wide || true
  echo '```'
  echo
  echo "## Resource snapshot (kubectl top, if metrics-server installed)"
  echo
  echo '```'
  kubectl -n robofleet top pods 2>&1 || echo "metrics-server not available"
  echo '```'
} >"${OUT_FILE}"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-robot-telemetry}"

BENCH_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "- Load start (UTC): \`${BENCH_START}\`" >>"${OUT_FILE}"

echo "Recording lag before load..."
append_lag_section "Lag before load"

echo "Load generator for ${DURATION_SEC}s (host)..."
python bot_generator.py --eps "${EPS}" --bots 20 &
GEN_PID=$!
sleep "${DURATION_SEC}"
kill -TERM "${GEN_PID}" 2>/dev/null || true
wait "${GEN_PID}" 2>/dev/null || true

echo "Recording lag after load..."
append_lag_section "Lag after load"

echo "Waiting ${DRAIN_SEC}s for watermarked windows to emit..."
sleep "${DRAIN_SEC}"
SAMPLE_END="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "Sampling window latency..."
{
  echo
  echo "- Latency sample end (UTC): \`${SAMPLE_END}\`"
  echo
  "${PYTHON[@]}" scripts/bench_latency.py sample \
    --bootstrap "${KAFKA_BOOTSTRAP_SERVERS}" \
    --since "${BENCH_START}" \
    --until "${SAMPLE_END}" \
    --group "bench-latency-${STAMP}"
  echo
  echo "## Notes"
  echo
  echo "- Fill correctness from \`python -m unittest tests.test_window_semantics\`."
  echo "- Compare Spark vs baseline using lag (max) and the latency table above."
} >>"${OUT_FILE}"

cp "${OUT_FILE}" "${OUT_DIR}/module-3-results.md"
echo "bench_ok wrote ${OUT_FILE}"
