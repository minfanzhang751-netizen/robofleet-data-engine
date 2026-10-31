"""LLM client service package (Module 3).

Consumes window aggregates from ``robot-telemetry-windows``, applies a
``ClassifierClient`` (mock rules today), and produces
``robot-diagnostics-classified``.

Public pieces:

- ``classifier``: ``ClassifierClient`` protocol + ``MockClassifier``
- ``main``: Kafka consume/classify/produce loop for the Deployment
"""
