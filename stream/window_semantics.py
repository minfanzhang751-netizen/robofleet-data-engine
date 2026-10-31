"""Shared event-time sliding-window semantics for Module 3.

This module is the **readable reference implementation** of the window
contract documented in ``docs/window-schema-v1.md``. The handwritten baseline
consumer uses it directly. The Spark job implements the same rules via
Structured Streaming APIs; treat Spark's JVM state store as a black box, but
keep these concepts semi-transparent:

- **Event time**: telemetry ``timestamp``, not Kafka ingestion time.
- **Sliding window**: size 60s, slide 10s, Unix-epoch aligned
  (same as Spark ``window(col, "60 seconds", "10 seconds")``).
- **Watermark / lateness**: drop events older than
  ``max_observed_event_time - 30 seconds``.
- **Emit**: when watermark reaches ``window_end``, close that aggregate and
  emit a ``WindowEvent`` (``emitted_at`` is processing time for latency).

State model (maps to Spark state conceptually):

- Key: ``(bot_id, window_start, window_end)``
- Value: running sums / count in ``_Agg``
- Windows are created lazily when an on-time event lands in them; they are not
  pre-instantiated on a wall-clock timer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator, Mapping, MutableMapping, Sequence, TypedDict

# Locked Module 3 window parameters (must stay aligned with Spark job options).
WINDOW_SIZE = timedelta(seconds=60)
WINDOW_SLIDE = timedelta(seconds=10)
WATERMARK_DELAY = timedelta(seconds=30)
SCHEMA_VERSION = "1"


class MetricsAvg(TypedDict):
    """Mean metrics for one closed window (window-schema-v1 ``metrics_avg``)."""

    battery_level_pct: float
    battery_temp_c: float
    motor_torque_nm: float


class WindowEvent(TypedDict):
    """One Kafka window-aggregate message body (window-schema-v1).

    ``window_start`` / ``window_end`` are event-time bounds ``[start, end)``.
    ``emitted_at`` is processing time when the aggregate was materialized.
    ``processor`` is ``\"baseline\"`` or ``\"spark\"`` depending on who produced it.
    """

    schema_version: str
    window_start: str
    window_end: str
    bot_id: int
    metrics_avg: MetricsAvg
    event_count: int
    processor: str
    emitted_at: str


def parse_event_time(timestamp: str) -> datetime:
    """Parse a telemetry ISO8601 ``timestamp`` into timezone-aware UTC.

    Naive timestamps are assumed UTC. Aware timestamps are converted to UTC so
    window alignment does not depend on the producer's local offset string.
    """
    parsed = datetime.fromisoformat(timestamp)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def to_iso(dt: datetime) -> str:
    """Format a datetime as UTC ISO8601 with an explicit offset (contract field)."""
    return dt.astimezone(timezone.utc).isoformat()


def sliding_window_bounds(event_time: datetime) -> list[tuple[datetime, datetime]]:
    """Return all epoch-aligned sliding windows that contain ``event_time``.

    Each window is the half-open interval ``[start, end)`` with
    ``end - start == WINDOW_SIZE`` and starts spaced by ``WINDOW_SLIDE``.

    For size=60s and slide=10s, a typical event falls into exactly six windows
    (60 / 10). Starts are aligned so ``start`` is a multiple of 10 seconds from
    the Unix epoch—matching Spark's default window alignment.
    """
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    elapsed = (event_time - epoch).total_seconds()
    slide = WINDOW_SLIDE.total_seconds()
    size = WINDOW_SIZE.total_seconds()
    max_start_offset = (elapsed // slide) * slide
    bounds: list[tuple[datetime, datetime]] = []
    start_offset = max_start_offset
    while start_offset + size > elapsed and start_offset <= elapsed:
        start = epoch + timedelta(seconds=start_offset)
        end = start + WINDOW_SIZE
        if start <= event_time < end:
            bounds.append((start, end))
        start_offset -= slide
        if start_offset < elapsed - size:
            break
    bounds.sort(key=lambda pair: pair[0])
    return bounds


@dataclass
class _Agg:
    """Mutable running aggregate for one ``(bot_id, window)`` key.

    Stores sums (not yet averaged). ``averages()`` divides by ``count`` when
    the window closes. Internal to the engine; not a Kafka payload.
    """

    count: int = 0
    battery_level_pct: float = 0.0
    battery_temp_c: float = 0.0
    motor_torque_nm: float = 0.0

    def add(self, metrics: Mapping[str, float]) -> None:
        """Fold one telemetry ``metrics`` object into this aggregate."""
        self.count += 1
        self.battery_level_pct += float(metrics["battery_level_pct"])
        self.battery_temp_c += float(metrics["battery_temp_c"])
        self.motor_torque_nm += float(metrics["motor_torque_nm"])

    def averages(self) -> MetricsAvg:
        """Return rounded means for the window-schema ``metrics_avg`` object."""
        if self.count <= 0:
            raise ValueError("cannot average empty aggregate")
        n = float(self.count)
        return MetricsAvg(
            battery_level_pct=round(self.battery_level_pct / n, 4),
            battery_temp_c=round(self.battery_temp_c / n, 4),
            motor_torque_nm=round(self.motor_torque_nm / n, 4),
        )


@dataclass
class EventTimeWindowEngine:
    """In-memory event-time sliding windows with watermark lateness drop.

    Used by the baseline Kafka service. Conceptually the same state a Spark
    Structured Streaming window aggregation keeps in its state store:

    - advance ``_max_event_time`` on each event
    - ``watermark = max_event_time - WATERMARK_DELAY``
    - late events (``event_time < watermark``) do not update aggregates
    - when ``window_end <= watermark``, emit and delete that key

    ``processor`` is copied into every emitted ``WindowEvent`` (``\"baseline\"``
    for the handwritten path).
    """

    processor: str
    _max_event_time: datetime | None = None
    _aggs: MutableMapping[tuple[int, datetime, datetime], _Agg] = field(
        default_factory=dict
    )

    @property
    def watermark(self) -> datetime | None:
        """Current event-time watermark, or ``None`` before any event arrives."""
        if self._max_event_time is None:
            return None
        return self._max_event_time - WATERMARK_DELAY

    def ingest(
        self,
        bot_id: int,
        event_time: datetime,
        metrics: Mapping[str, float],
    ) -> tuple[list[WindowEvent], bool]:
        """Ingest one on-stream telemetry event.

        Returns:
            A pair ``(closed_windows, dropped_as_late)``.

            - ``closed_windows``: aggregates whose ``window_end`` is now at or
              before the watermark (may be empty on early events).
            - ``dropped_as_late``: ``True`` if this event was older than the
              watermark and therefore did not update any aggregates.
        """
        if self._max_event_time is None or event_time > self._max_event_time:
            self._max_event_time = event_time

        wm = self.watermark
        if wm is not None and event_time < wm:
            return self._emit_closed(wm), True

        for start, end in sliding_window_bounds(event_time):
            if wm is not None and end <= wm:
                continue
            key = (bot_id, start, end)
            agg = self._aggs.get(key)
            if agg is None:
                agg = _Agg()
                self._aggs[key] = agg
            agg.add(metrics)

        closed = self._emit_closed(wm) if wm is not None else []
        return closed, False

    def _emit_closed(self, watermark: datetime) -> list[WindowEvent]:
        """Materialize and remove every aggregate with ``end <= watermark``."""
        emitted_at = datetime.now(timezone.utc)
        closed: list[tuple[tuple[int, datetime, datetime], _Agg]] = []
        for key, agg in list(self._aggs.items()):
            _bot_id, _start, end = key
            if end <= watermark:
                closed.append((key, agg))
                del self._aggs[key]
        closed.sort(key=lambda item: (item[0][0], item[0][1]))
        results: list[WindowEvent] = []
        for (bot_id, start, end), agg in closed:
            results.append(
                WindowEvent(
                    schema_version=SCHEMA_VERSION,
                    window_start=to_iso(start),
                    window_end=to_iso(end),
                    bot_id=bot_id,
                    metrics_avg=agg.averages(),
                    event_count=agg.count,
                    processor=self.processor,
                    emitted_at=to_iso(emitted_at),
                )
            )
        return results

    def flush_remaining(self) -> list[WindowEvent]:
        """Emit all still-open windows without waiting for watermark.

        Used on graceful shutdown / tests so buffered state is not silently
        discarded. Production Spark relies on watermark + checkpoint instead;
        baseline uses this only when the process stops.
        """
        emitted_at = datetime.now(timezone.utc)
        items = sorted(self._aggs.items(), key=lambda item: (item[0][0], item[0][1]))
        self._aggs.clear()
        results: list[WindowEvent] = []
        for (bot_id, start, end), agg in items:
            results.append(
                WindowEvent(
                    schema_version=SCHEMA_VERSION,
                    window_start=to_iso(start),
                    window_end=to_iso(end),
                    bot_id=bot_id,
                    metrics_avg=agg.averages(),
                    event_count=agg.count,
                    processor=self.processor,
                    emitted_at=to_iso(emitted_at),
                )
            )
        return results


def gold_window_averages(
    events: Sequence[Mapping[str, object]],
    bot_id: int,
    window_start: datetime,
    window_end: datetime,
) -> MetricsAvg | None:
    """Compute offline ground-truth averages for correctness tests.

    Ignores watermark/lateness: every event with matching ``bot_id`` and
    ``timestamp`` in ``[window_start, window_end)`` is included. Used to verify
    that the engine's closed-window means match a simple replay.
    """
    total_level = 0.0
    total_temp = 0.0
    total_torque = 0.0
    count = 0
    for event in events:
        if int(event["bot_id"]) != bot_id:  # type: ignore[arg-type]
            continue
        event_time = parse_event_time(str(event["timestamp"]))
        if not (window_start <= event_time < window_end):
            continue
        metrics = event["metrics"]  # type: ignore[index]
        assert isinstance(metrics, Mapping)
        total_level += float(metrics["battery_level_pct"])
        total_temp += float(metrics["battery_temp_c"])
        total_torque += float(metrics["motor_torque_nm"])
        count += 1
    if count == 0:
        return None
    return MetricsAvg(
        battery_level_pct=round(total_level / count, 4),
        battery_temp_c=round(total_temp / count, 4),
        motor_torque_nm=round(total_torque / count, 4),
    )


def iter_closed_windows_offline(
    events: Iterable[Mapping[str, object]],
    processor: str = "baseline",
) -> Iterator[WindowEvent]:
    """Replay a finite event list through the engine in event-time order.

    Yields window events as they close under watermark rules. Does not call
    ``flush_remaining``; trailing open windows are left unemitted unless the
    replay itself advances the watermark far enough.
    """
    engine = EventTimeWindowEngine(processor=processor)
    ordered = sorted(events, key=lambda e: parse_event_time(str(e["timestamp"])))
    for event in ordered:
        metrics = event["metrics"]  # type: ignore[index]
        assert isinstance(metrics, Mapping)
        closed, _dropped = engine.ingest(
            bot_id=int(event["bot_id"]),  # type: ignore[arg-type]
            event_time=parse_event_time(str(event["timestamp"])),
            metrics=metrics,  # type: ignore[arg-type]
        )
        yield from closed
