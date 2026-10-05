#!/usr/bin/env bash
# Linux/Mac version of scripts/spark-submit.ps1 -- see that file for details.
#   ./scripts/spark-submit.sh spark_jobs/prepare_data.py
set -euo pipefail

SCRIPT="$1"; shift
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,io.delta:delta-spark_2.12:3.3.2"

docker compose exec spark spark-submit \
  --master "local[*]" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
  --packages "$PACKAGES" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  "/app/$SCRIPT" "$@"
