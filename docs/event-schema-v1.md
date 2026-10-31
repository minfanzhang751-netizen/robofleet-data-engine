# Telemetry Event Contract v1

Each message is one UTF-8 JSON object on a single line. All fields are required.

| Field | JSON type | Meaning and constraints |
|---|---|---|
| `schema_version` | string | Always `"1"` for this contract. |
| `event_id` | string | UUID identifying this logical event. Preserve it when retrying the event. |
| `timestamp` | string | Event generation time as UTC ISO8601 with an explicit UTC offset. Preserve it when retrying. |
| `bot_id` | integer | Robot identifier greater than or equal to 1. |
| `metrics.battery_level_pct` | number | Battery level in percent, from 0 through 100. |
| `metrics.battery_temp_c` | number | Battery temperature in degrees Celsius, from 30 through 60. |
| `metrics.motor_torque_nm` | number | Motor torque in newton-metres, from 0 through 300. |
| `diagnostics` | string | Human-readable status text consistent with the metric thresholds below. |

## Time semantics

`timestamp` is event time: when the generator created the telemetry event. Consumers must use it, rather than Kafka ingestion time, for event-time windows. A producer retry must retain both the original `event_id` and `timestamp`.

## Kafka key

The Module 2 host producer (`kafka_sink.py`) sets the Kafka message key to the
base-10 `bot_id` encoded as UTF-8. For example, `bot_id: 42` uses the bytes for
`42`. This keeps events from one robot in the same partition and preserves
their per-robot order.

## Diagnostic thresholds

Metric-based diagnostics use this priority:

1. `battery_level_pct <= 15` produces `Battery voltage low.`
2. Otherwise, `battery_temp_c >= 55` produces `Motor temperature high.`
3. Otherwise, `motor_torque_nm >= 270` produces `Overcurrent protection active.`
4. Otherwise, the generator selects a general operational or non-metric diagnostic.

## Compatibility

Version 1 fields may be added only when old consumers can safely ignore them. Do not change an existing field's type, unit, meaning, or required status in place. Any incompatible change requires a new `schema_version`.
