"""Latency sample and consumer-lag helpers for the Module 3 benchmark.

The shell driver lists Kafka groups and passes ``--describe`` text here.
Window latency is ``emitted_at - window_end`` in seconds. Sampling uses a
throwaway consumer group and does not commit offsets.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping, Sequence


def parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 instant, accepting a trailing ``Z``."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def nearest_rank_percentile(values: Sequence[float], percentile: float) -> float:
    """Return the nearest-rank percentile of ``values`` (``percentile`` in 0–100)."""
    if not values:
        raise ValueError("values must not be empty")
    if percentile <= 0 or percentile > 100:
        raise ValueError("percentile must be in (0, 100]")
    ordered = sorted(values)
    rank = math.ceil(percentile / 100.0 * len(ordered))
    index = min(max(rank, 1), len(ordered)) - 1
    return ordered[index]


def in_sample_window(emitted_at: datetime, start: datetime, end: datetime) -> bool:
    """Return whether ``emitted_at`` falls in the inclusive bench interval."""
    return start <= emitted_at <= end


def delay_seconds(window_end: datetime, emitted_at: datetime) -> float:
    """Return processing delay in seconds (``emitted_at - window_end``)."""
    return (emitted_at - window_end).total_seconds()


def summarize_delays(delays: Sequence[float]) -> dict[str, str]:
    """Format p50, p95, and sample size. Empty input is ``no samples``."""
    if not delays:
        return {"p50": "no samples", "p95": "no samples", "n": "0"}
    return {
        "p50": f"{nearest_rank_percentile(delays, 50):.3f}",
        "p95": f"{nearest_rank_percentile(delays, 95):.3f}",
        "n": str(len(delays)),
    }


def max_lag_from_describe(text: str) -> str:
    """Return the max numeric LAG from ``kafka-consumer-groups --describe`` text."""
    lines = text.splitlines()
    header_index: int | None = None
    lag_index = -1
    for index, line in enumerate(lines):
        columns = line.split()
        if "LAG" in columns and "PARTITION" in columns:
            header_index = index
            lag_index = columns.index("LAG")
            break
    if header_index is None:
        return "group not found"
    values: list[int] = []
    for line in lines[header_index + 1 :]:
        columns = line.split()
        if len(columns) <= lag_index:
            continue
        cell = columns[lag_index]
        if cell == "-":
            continue
        try:
            values.append(int(cell))
        except ValueError:
            continue
    if not values:
        return "group not found"
    return str(max(values))


def latency_markdown(rows: Iterable[tuple[str, Mapping[str, str]]]) -> str:
    """Render the latency table written into the benchmark snapshot."""
    lines = [
        "## Latency (`emitted_at - window_end`)",
        "",
        "| Path | p50 (s) | p95 (s) | Sample size |",
        "|------|---------|---------|-------------|",
    ]
    for name, summary in rows:
        lines.append(
            f"| {name} | {summary['p50']} | {summary['p95']} | {summary['n']} |"
        )
    return "\n".join(lines) + "\n"


def delays_for_records(
    records: Iterable[Mapping[str, object]],
    start: datetime,
    end: datetime,
) -> list[float]:
    """Collect delays for records whose ``emitted_at`` is inside the interval."""
    delays: list[float] = []
    for record in records:
        emitted_raw = record.get("emitted_at")
        window_end_raw = record.get("window_end")
        if not isinstance(emitted_raw, str) or not isinstance(window_end_raw, str):
            continue
        emitted_at = parse_iso(emitted_raw)
        if not in_sample_window(emitted_at, start, end):
            continue
        delays.append(delay_seconds(parse_iso(window_end_raw), emitted_at))
    return delays


def _assign_from_time(consumer: object, topic: str, timestamp_ms: int) -> None:
    """Assign topic partitions starting near ``timestamp_ms`` (epoch milliseconds)."""
    from confluent_kafka import OFFSET_END, TopicPartition

    metadata = consumer.list_topics(topic, timeout=10)  # type: ignore[attr-defined]
    topic_meta = metadata.topics[topic]
    if topic_meta.error is not None:
        raise RuntimeError(f"topic metadata failed for {topic}: {topic_meta.error}")
    query = [
        TopicPartition(topic, partition_id, timestamp_ms)
        for partition_id in topic_meta.partitions
    ]
    offsets = consumer.offsets_for_times(query, timeout=10)  # type: ignore[attr-defined]
    assigned = []
    for partition in offsets:
        if partition.offset is None or partition.offset < 0:
            partition.offset = OFFSET_END
        assigned.append(partition)
    consumer.assign(assigned)  # type: ignore[attr-defined]


def sample_topic_delays(
    consumer: object,
    topic: str,
    start: datetime,
    end: datetime,
    lookback: timedelta,
    poll_timeout_sec: float = 20.0,
) -> list[float]:
    """Read ``topic`` without committing and return in-window delays."""
    seek_at = start - lookback
    timestamp_ms = int(seek_at.timestamp() * 1000)
    _assign_from_time(consumer, topic, timestamp_ms)
    records: list[dict[str, object]] = []
    idle_rounds = 0
    deadline = time.monotonic() + poll_timeout_sec
    while time.monotonic() < deadline and idle_rounds < 5:
        message = consumer.poll(1.0)  # type: ignore[attr-defined]
        if message is None:
            idle_rounds += 1
            continue
        if message.error() is not None:
            continue
        idle_rounds = 0
        try:
            payload = json.loads(message.value().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return delays_for_records(records, start, end)


def sample_latency(
    bootstrap: str,
    group_id: str,
    start: datetime,
    end: datetime,
    spark_topic: str,
    baseline_topic: str,
    lookback: timedelta = timedelta(seconds=90),
) -> str:
    """Sample both window topics and return the markdown latency table."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )
    try:
        spark_delays = sample_topic_delays(
            consumer, spark_topic, start, end, lookback
        )
        baseline_delays = sample_topic_delays(
            consumer, baseline_topic, start, end, lookback
        )
    finally:
        consumer.close()
    return latency_markdown(
        (
            ("Spark", summarize_delays(spark_delays)),
            ("Baseline", summarize_delays(baseline_delays)),
        )
    )


def main(argv: Sequence[str] | None = None) -> None:
    """CLI: ``max-lag`` reads describe text on stdin; ``sample`` prints a table."""
    parser = argparse.ArgumentParser(description="Module 3 benchmark metrics.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("max-lag", help="Print max LAG from describe text on stdin.")

    sample = subparsers.add_parser("sample", help="Print a p50/p95 markdown table.")
    sample.add_argument("--bootstrap", required=True)
    sample.add_argument("--since", required=True, help="Inclusive UTC start.")
    sample.add_argument("--until", required=True, help="Inclusive UTC end.")
    sample.add_argument("--group", required=True)
    sample.add_argument("--spark-topic", default="robot-telemetry-windows")
    sample.add_argument(
        "--baseline-topic",
        default="robot-telemetry-windows-baseline",
    )

    args = parser.parse_args(argv)
    if args.command == "max-lag":
        print(max_lag_from_describe(sys.stdin.read()))
        return
    print(
        sample_latency(
            bootstrap=args.bootstrap,
            group_id=args.group,
            start=parse_iso(args.since),
            end=parse_iso(args.until),
            spark_topic=args.spark_topic,
            baseline_topic=args.baseline_topic,
        ),
        end="",
    )


if __name__ == "__main__":
    main()
