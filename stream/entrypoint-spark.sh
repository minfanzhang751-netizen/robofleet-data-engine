#!/usr/bin/env bash
# Container ENTRYPOINT for the spark-window Pod (Module 3 primary path).
#
# Invoked by the container runtime when the Deployment starts a Pod using
# image robofleet-spark:local (see stream/Dockerfile.spark ENTRYPOINT).
# This script does not process Kafka data itself; it only launches spark-submit
# which runs /opt/spark-job/spark_window_job.py as a Structured Streaming job.
#
# Env (usually from the Deployment): SPARK_MASTER, SPARK_KAFKA_PACKAGE,
# SPARK_IVY_DIR, plus Kafka_* / SPARK_CHECKPOINT_DIR read by the Python job.
set -euo pipefail

# Maven coordinates for the Spark Kafka connector (jars cached in the image ivy dir).
PACKAGES="${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3}"
# Local ivy cache populated at docker build time so the Pod need not hit Maven.
IVY_DIR="${SPARK_IVY_DIR:-/opt/bitnami/spark/ivy}"

# The image runs as uid 1001, which has no /etc/passwd name unless Bitnami's
# entrypoint sets up nss_wrapper. Hadoop's UnixLoginModule exits when that
# lookup returns null, so give this uid a name before spark-submit.
NSS_LIB="${LIBNSS_WRAPPER_PATH:-/opt/bitnami/common/lib/libnss_wrapper.so}"
NSS_PASSWD="/tmp/nss_passwd"
NSS_GROUP="/tmp/nss_group"
echo "spark:x:$(id -u):$(id -g):spark:/opt/bitnami/spark:/usr/sbin/nologin" > "${NSS_PASSWD}"
echo "spark:x:$(id -g):" > "${NSS_GROUP}"
export NSS_WRAPPER_PASSWD="${NSS_PASSWD}"
export NSS_WRAPPER_GROUP="${NSS_GROUP}"
export LD_PRELOAD="${NSS_LIB}${LD_PRELOAD:+:${LD_PRELOAD}}"

# exec replaces this shell as PID 1 so SIGTERM from Kubernetes reaches Spark.
exec /opt/bitnami/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-local[2]}" \
  --packages "${PACKAGES}" \
  --conf "spark.jars.ivy=${IVY_DIR}" \
  --conf spark.sql.shuffle.partitions=6 \
  --conf spark.sql.session.timeZone=UTC \
  /opt/spark-job/spark_window_job.py
