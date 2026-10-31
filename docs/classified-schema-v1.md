# Diagnostics Classification Contract v1

Each message is one UTF-8 JSON object. Kafka message key is the base-10
`bot_id` encoded as UTF-8.

Produced by the Module 3 LLM client (mock today; Module 4 swaps the model
backend without changing this contract) on topic `robot-diagnostics-classified`.

| Field | JSON type | Meaning and constraints |
|---|---|---|
| `schema_version` | string | Always `"1"`. |
| `bot_id` | integer | Robot identifier >= 1. |
| `window_start` | string | Copied from the window event (UTC ISO8601). |
| `window_end` | string | Copied from the window event (UTC ISO8601). |
| `metrics_avg` | object | Copied window averages (`battery_level_pct`, `battery_temp_c`, `motor_torque_nm`). |
| `event_count` | integer | Copied from the window event. |
| `label` | string | One of `Safe`, `Warning`, `Critical`. |
| `model` | string | `"mock"` for Module 3; Module 4 sets the real model id. |
| `latency_ms` | number | Client-measured classifier call latency in milliseconds. |
| `classified_at` | string | UTC ISO8601 when classification completed. |

## Mock decision rules (Module 3)

Applied to window averages (priority order):

1. `battery_level_pct <= 15` or `battery_temp_c >= 55` or `motor_torque_nm >= 270` → `Critical`
2. Else `battery_level_pct <= 30` or `battery_temp_c >= 50` or `motor_torque_nm >= 200` → `Warning`
3. Else → `Safe`

## Compatibility

Module 4 must keep field names and `label` enum stable. Only `model` and
internal scoring may change.
