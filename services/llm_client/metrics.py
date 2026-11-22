"""Prometheus metrics for the llm-client process (Module 5).

Exposes ``/metrics`` on a background HTTP server. Histograms and counters use
the process-wide default registry unless a custom ``CollectorRegistry`` is
passed (tests).
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from prometheus_client import CollectorRegistry, Counter, Histogram, start_http_server

if TYPE_CHECKING:
    pass

# Buckets cover typical Kind classify latencies (ms) through slow GPU cold starts.
LATENCY_BUCKETS_MS = (
    5.0,
    10.0,
    25.0,
    50.0,
    100.0,
    250.0,
    500.0,
    1000.0,
    2500.0,
    5000.0,
    10000.0,
)


class ClientMetrics:
    """Classify latency histogram plus classified / DLQ counters."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        kwargs: dict[str, object] = {}
        if registry is not None:
            kwargs["registry"] = registry
        self.classify_latency_ms = Histogram(
            "robofleet_classify_latency_ms",
            "Successful classify() wall time in milliseconds (same as latency_ms).",
            buckets=LATENCY_BUCKETS_MS,
            **kwargs,
        )
        self.classified_total = Counter(
            "robofleet_classified_total",
            "Windows written to robot-diagnostics-classified.",
            **kwargs,
        )
        self.dlq_total = Counter(
            "robofleet_dlq_total",
            "Windows written to robot-diagnostics-dlq.",
            **kwargs,
        )

    def observe_classified(self, latency_ms: float) -> None:
        """Record one successful classification."""
        self.classify_latency_ms.observe(float(latency_ms))
        self.classified_total.inc()

    def observe_dlq(self) -> None:
        """Record one DLQ produce path."""
        self.dlq_total.inc()


_DEFAULT = ClientMetrics()
_metrics_server_started = False
_metrics_lock = threading.Lock()


def default_metrics() -> ClientMetrics:
    """Process-wide metrics used by ``LlmClientService``."""
    return _DEFAULT


def start_metrics_server(port: int, addr: str = "0.0.0.0") -> None:
    """Start the Prometheus HTTP exposition server once per process."""
    global _metrics_server_started
    with _metrics_lock:
        if _metrics_server_started:
            return
        start_http_server(port, addr=addr)
        _metrics_server_started = True
