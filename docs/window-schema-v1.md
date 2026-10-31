# Window Aggregate Contract v1

Each message is one UTF-8 JSON object. Kafka message key is the base-10
`bot_id` encoded as UTF-8 (same convention as telemetry v1).

Produced by Module 3 stream processors:

- `robot-telemetry-windows` — PySpark Structured Streaming (`processor=spark`)
- `robot-telemetry-windows-baseline` — handwritten baseline (`processor=baseline`)

| Field | JSON type | Meaning and constraints |
|---|---|---|
| `schema_version` | string | Always `"1"` for this contract. |
| `window_start` | string | Inclusive event-time window start (UTC ISO8601 with offset). |
| `window_end` | string | Exclusive event-time window end (UTC ISO8601 with offset). |
| `bot_id` | integer | Robot identifier >= 1. |
| `metrics_avg.battery_level_pct` | number | Mean battery level (%) in the window. |
| `metrics_avg.battery_temp_c` | number | Mean battery temperature (°C) in the window. |
| `metrics_avg.motor_torque_nm` | number | Mean motor torque (N·m) in the window. |
| `event_count` | integer | Number of telemetry events aggregated (>= 1). |
| `processor` | string | `"spark"` or `"baseline"`. |
| `emitted_at` | string | Processing-time emit instant (UTC ISO8601); for latency metrics. |

## Window semantics

- Event time comes from telemetry `timestamp`, not Kafka ingestion time.
- Sliding window: size **60s**, slide **10s**, aligned to Unix epoch boundaries
  (same default alignment as Spark `window(col, "60 seconds", "10 seconds")`).
- Watermark / lateness: events older than **max observed event-time − 30s**
  (stream watermark delay) are dropped and do not update aggregates.
- A window result is emitted once the watermark reaches `window_end` (no further
  on-time events can arrive for that window).

## Compatibility

Additive optional fields only when old consumers can ignore them. Incompatible
changes require a new `schema_version`.
