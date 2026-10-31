# Module 3 benchmark snapshot (20261024T194208Z)

## Environment

- Kind context: `kind-robofleet`
- Generator EPS: `100`
- Duration (s): `120`
- Drain wait (s): `40`
- Git commit: `d15a114`

## Pods

```
NAME                               READY   STATUS    RESTARTS   AGE    IP            NODE                      NOMINATED NODE   READINESS GATES
baseline-window-5f9899c796-cbc2w   1/1     Running   0          170m   10.244.0.8    robofleet-control-plane   <none>           <none>
llm-client-78bcd6bb46-r22mm        1/1     Running   0          170m   10.244.0.10   robofleet-control-plane   <none>           <none>
spark-window-5658bd98-w6khn        1/1     Running   0          124m   10.244.0.13   robofleet-control-plane   <none>           <none>
```

## Resource snapshot (kubectl top, if metrics-server installed)

```
error: Metrics API not available
metrics-server not available
```
- Load start (UTC): `2026-10-24T19:42:08Z`

## Lag before load

| Group | Lag (max) |
|-------|-----------|
| `robofleet-spark-windows*` | group not found |
| `robofleet-baseline-windows` | 0 |
| `robofleet-llm-client` | 0 |

## Lag after load

| Group | Lag (max) |
|-------|-----------|
| `robofleet-spark-windows*` | group not found |
| `robofleet-baseline-windows` | 0 |
| `robofleet-llm-client` | 0 |

- Latency sample end (UTC): `2026-10-24T19:44:56Z`

## Latency (`emitted_at - window_end`)

| Path | p50 (s) | p95 (s) | Sample size |
|------|---------|---------|-------------|
| Spark | 30.695 | 5653.706 | 207 |
| Baseline | 30.011 | 5653.064 | 207 |

## Notes

- Fill correctness from `python -m unittest tests.test_window_semantics`.
- Compare Spark vs baseline using lag (max) and the latency table above.
