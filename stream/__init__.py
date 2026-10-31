"""Module 3 stream processing package.

Contains:

- ``window_semantics``: shared event-time sliding-window math (baseline gold
  semantics; mirrors Spark ``window`` + watermark defaults).
- ``baseline_window``: in-cluster handwritten Kafka consumer/producer used as
  the resume/interview comparison path.
- ``spark_window_job``: primary PySpark Structured Streaming job.

Deployed by ``scripts/stream-install.sh`` into the ``robofleet`` namespace.
See ``docs/module-3.md`` and ``docs/window-schema-v1.md``.
"""
