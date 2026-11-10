"""LLM client service package (Module 3 mock, Module 4 vLLM).

Consumes window aggregates from ``robot-telemetry-windows``, applies a
``ClassifierClient``, and produces ``robot-diagnostics-classified``.
Unclassified windows go to ``robot-diagnostics-dlq``.

Public pieces:

- ``classifier``: ``ClassifierClient``, ``MockClassifier``, ``VllmClassifier``
- ``main``: Kafka consume/classify/produce loop for the Deployment
"""
