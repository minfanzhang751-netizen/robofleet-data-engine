"""PySpark Structured Streaming window job (Module 3 primary path).

This file is the **production** stream processor. It is submitted with
``spark-submit`` from ``entrypoint-spark.sh`` inside the ``spark-window`` Pod.

What to treat as semi-transparent vs black-box:

- Semi-transparent: Kafka source/sink options, event-time column, watermark,
  ``window(60s, 10s)``, ``outputMode(\"append\")``, checkpoint location.
- Black-box: how Spark stores window state in the JVM / RocksDB, codegen.

Semantic alignment with ``stream.window_semantics.EventTimeWindowEngine``:

- Same 60s / 10s sliding windows and 30s watermark delay.
- Group by ``bot_id`` (Kafka key convention preserved on sink).
- Emit window rows only after watermark passes ``window.end`` (append mode).

Output topic: ``robot-telemetry-windows`` (see ``docs/window-schema-v1.md``).
Checkpoint directory is mounted from a PVC in the Deployment.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

DEFAULT_BOOTSTRAP = "robofleet-kafka-bootstrap.kafka.svc:9092"
DEFAULT_INPUT_TOPIC = "robot-telemetry"
DEFAULT_OUTPUT_TOPIC = "robot-telemetry-windows"
DEFAULT_CHECKPOINT = "/tmp/spark-checkpoints/robot-telemetry-windows"
PROCESSOR = "spark"

# Must match telemetry event-schema-v1 so from_json does not drop fields.
TELEMETRY_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("bot_id", IntegerType(), False),
        StructField(
            "metrics",
            StructType(
                [
                    StructField("battery_level_pct", DoubleType(), False),
                    StructField("battery_temp_c", DoubleType(), False),
                    StructField("motor_torque_nm", DoubleType(), False),
                ]
            ),
            False,
        ),
        StructField("diagnostics", StringType(), False),
    ]
)


def build_spark(app_name: str = "robofleet-spark-windows") -> SparkSession:
    """Create or reuse a ``SparkSession`` for this streaming job.

    When launched via ``spark-submit --master ...``, the master is already set
    by the submitter. ``SPARK_MASTER`` is applied only when present so local
    debugging can override without fighting spark-submit.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.session.timeZone", "UTC")
    )
    # Prefer spark-submit --master; allow override when running the module directly.
    master = os.environ.get("SPARK_MASTER", "").strip()
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def run_job(
    spark: SparkSession,
    bootstrap: str,
    input_topic: str,
    output_topic: str,
    checkpoint: str,
    starting_offsets: str = "earliest",
) -> None:
    """Define and start the continuous Structured Streaming query.

    Steps (DataFrame lineage until ``start()``):

    1. ``readStream`` Kafka → ``raw`` binary key/value rows.
    2. Parse JSON value with ``TELEMETRY_SCHEMA``; derive ``event_time``.
    3. ``withWatermark(event_time, 30 seconds)`` for lateness.
    4. ``groupBy(bot_id, window(60s, 10s)).agg(avg, count)``.
    5. Shape columns to window-schema-v1; JSON-encode for Kafka value.
    6. ``writeStream`` to Kafka with checkpoint + append mode; block on
       ``awaitTermination``.

    The live job object is the returned ``StreamingQuery`` (not a custom class).
    """
    # --- Source: subscribe to Module 2 output topic ---
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", input_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .option("groupIdPrefix", "robofleet-spark-windows")
        .load()
    )

    # --- Parse + event-time watermark (semi-transparent contract) ---
    parsed = (
        raw.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json("json", TELEMETRY_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withWatermark("event_time", "30 seconds")
    )

    # --- Sliding-window aggregation (aligns with window_semantics constants) ---
    aggregated = (
        parsed.groupBy(
            F.col("bot_id"),
            F.window(F.col("event_time"), "60 seconds", "10 seconds"),
        )
        .agg(
            F.avg("metrics.battery_level_pct").alias("battery_level_pct"),
            F.avg("metrics.battery_temp_c").alias("battery_temp_c"),
            F.avg("metrics.motor_torque_nm").alias("motor_torque_nm"),
            F.count(F.lit(1)).alias("event_count"),
        )
        .select(
            F.lit("1").alias("schema_version"),
            F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias(
                "window_start"
            ),
            F.date_format(F.col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias(
                "window_end"
            ),
            F.col("bot_id"),
            F.struct(
                F.round("battery_level_pct", 4).alias("battery_level_pct"),
                F.round("battery_temp_c", 4).alias("battery_temp_c"),
                F.round("motor_torque_nm", 4).alias("motor_torque_nm"),
            ).alias("metrics_avg"),
            F.col("event_count").cast("int").alias("event_count"),
            F.lit(PROCESSOR).alias("processor"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias(
                "emitted_at"
            ),
        )
    )

    # Kafka sink expects string key + JSON string value columns.
    payload = aggregated.select(
        F.col("bot_id").cast("string").alias("key"),
        F.to_json(F.struct(*aggregated.columns)).alias("value"),
    )

    # --- Sink: append-only closed windows; checkpoint for recovery ---
    query = (
        payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", output_topic)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )
    print(
        "Spark window job started: "
        f"input={input_topic} output={output_topic} checkpoint={checkpoint}",
        file=sys.stderr,
        flush=True,
    )
    query.awaitTermination()


def main() -> None:
    """Read env, build session, run streaming job until the process is stopped."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP).strip() or (
        DEFAULT_BOOTSTRAP
    )
    input_topic = os.environ.get("KAFKA_INPUT_TOPIC", DEFAULT_INPUT_TOPIC).strip() or (
        DEFAULT_INPUT_TOPIC
    )
    output_topic = os.environ.get("KAFKA_OUTPUT_TOPIC", DEFAULT_OUTPUT_TOPIC).strip() or (
        DEFAULT_OUTPUT_TOPIC
    )
    checkpoint = os.environ.get("SPARK_CHECKPOINT_DIR", DEFAULT_CHECKPOINT).strip() or (
        DEFAULT_CHECKPOINT
    )
    spark = build_spark()
    try:
        run_job(spark, bootstrap, input_topic, output_topic, checkpoint)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
