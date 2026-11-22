"""Unit tests for llm-client Prometheus metrics (no Kafka, no HTTP scrape)."""

from __future__ import annotations

import unittest

from prometheus_client import CollectorRegistry, generate_latest

from services.llm_client.metrics import ClientMetrics


class ClientMetricsTests(unittest.TestCase):
    """Counters and histogram observations land in an isolated registry."""

    def setUp(self) -> None:
        self.registry = CollectorRegistry()
        self.metrics = ClientMetrics(registry=self.registry)

    def test_observe_classified_increments_and_records_latency(self) -> None:
        self.metrics.observe_classified(42.5)
        self.metrics.observe_classified(100.0)
        text = generate_latest(self.registry).decode("utf-8")
        self.assertIn("robofleet_classified_total", text)
        self.assertIn("robofleet_classify_latency_ms_bucket", text)
        self.assertIn('robofleet_classified_total 2.0', text)

    def test_observe_dlq_increments(self) -> None:
        self.metrics.observe_dlq()
        text = generate_latest(self.registry).decode("utf-8")
        self.assertIn("robofleet_dlq_total", text)
        self.assertIn("robofleet_dlq_total 1.0", text)


if __name__ == "__main__":
    unittest.main()
