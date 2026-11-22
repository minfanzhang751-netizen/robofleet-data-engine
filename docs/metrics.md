# Metrics glossary

Every published number should answer: **from where to where**, and **which
component measured it**. Prefer the names in this file over bare “latency”
or “throughput”.

## Explicitly not measured: end-to-end latency

This project does **not** define or collect end-to-end latency.

There is no timer from telemetry `timestamp` (or host `produce`) to
`classified_at`. Do **not** describe **window emission latency** or
**classify latency** as end-to-end.

## Metric catalog

| Name | Definition (from → to) | Where measured | How reported | Status |
|------|------------------------|----------------|--------------|--------|
| **offered EPS** | Generator *target* send rate: `--eps` (schedule goal, not broker-confirmed rate) | Host [`bot_generator.py`](../bot_generator.py); behind-schedule warnings on stderr | CLI flag / run notes | Collected (config + logs) |
| **broker-acked EPS** | Events per second successfully produced and broker-acked (`acks=all`) | Would be host [`kafka_sink.py`](../kafka_sink.py) | — | **Gap**: producer uses `acks=all` but does not export a success count or rate |
| **window emission latency** | `emitted_at − window_end` (processing time minus event-time window end) | Window JSON on `robot-telemetry-windows` / `…-baseline`; [`scripts/bench_latency.py`](../scripts/bench_latency.py) via `./scripts/bench-module-3.sh` | Bench table p50 / p95 (seconds) | Collected |
| **classify latency** | Wall time of a successful `classify()` call only (ms) | llm-client: field `latency_ms` and Prometheus `robofleet_classify_latency_ms` | Grafana p50 / p95; alert on p95 | Collected |
| **consumer lag** | Messages the consumer group is behind the log end | Bench: `kafka-consumer-groups --describe` max LAG; continuous: `kafka_consumergroup_lag` (kafka-exporter) | Bench before/after tables; Grafana lag panel | Collected |

## Percentiles: two different populations

Do not compare bench window percentiles to Grafana classify percentiles as if
they were the same statistic.

### Window emission latency (bench)

- **Observation:** one delay per sampled window message:
  `delay_seconds = emitted_at - window_end`.
- **Sample scope:** messages whose `emitted_at` falls in the load interval
  `[sample_start, sample_end]`. Spark and baseline are summarized **separately**.
- **Algorithm:** nearest-rank percentile
  ([`nearest_rank_percentile`](../scripts/bench_latency.py)).
- **Reported:** **p50**, **p95**.
- **p99:** **not reported** (bench code does not compute it).

### Classify latency (Prometheus)

- **Observation:** each successful classified path observes
  `robofleet_classify_latency_ms` (same value as `latency_ms` on the
  classified record).
- **Sample scope:** Grafana / alerts use `histogram_quantile` over
  `rate(..._bucket[5m])` — a **sliding scrape window**, not one bench batch.
- **Reported:** dashboard **p50** / **p95**; alert rule on classify **p95**.
- **p99:** **not published** as a project metric (PromQL could compute it; the
  RoboFleet Overview dashboard does not).

## Related runbooks

- Module 3 bench driver: [module-3.md](module-3.md)
- Continuous scrapes: [module-5.md](module-5.md)
- Window fields: [window-schema-v1.md](window-schema-v1.md)
- Classified `latency_ms`: [classified-schema-v1.md](classified-schema-v1.md)
