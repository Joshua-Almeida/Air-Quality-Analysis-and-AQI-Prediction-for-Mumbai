"""
Step 5b - Does the streaming result match a batch computation?

The streaming job produced rolling 24-hour averages incrementally, one
micro-batch at a time, from Kafka messages. Here we recompute exactly the same
windows from the cleaned Parquet file in one go and compare them row by row.
If the pipeline lost, duplicated or mis-timed any readings, the averages differ.

How the comparison range is chosen:
  The first reading the stream saw (T0) falls into 24 sliding windows, the
  earliest of which starts 23 hours before T0. So T0 = earliest window_start + 23h.
  The batch side only uses readings from T0 up to the last finished window,
  which is exactly the data the stream had seen for those windows.

Run (after the streaming job has written some windows):
    .\scripts\spark-submit.ps1 spark_jobs/validate_stream_vs_batch.py
"""
import os
import sys
from datetime import timedelta

import pandas as pd
from pyspark.sql import functions as F

from spark_jobs.common import BATCH_DIR, CITY_ROLLING_TABLE, CLEAN_PARQUET, get_spark

# Both sides round to 2 decimals, and Spark may add the readings in a different order,
# so a value sitting on a rounding edge can differ by one step (0.01) plus float noise.
TOLERANCE = 0.01 + 1e-6


def main():
    spark = get_spark("validate_stream_vs_batch")

    if not os.path.exists(os.path.join(CITY_ROLLING_TABLE, "_delta_log")):
        sys.exit("No streaming output yet - run the streaming job and the producer first.")

    stream = spark.read.format("delta").load(CITY_ROLLING_TABLE)
    bounds = stream.agg(F.min("window_start").alias("first_start"), F.max("window_end").alias("last_end")).first()
    t0 = bounds.first_start + timedelta(hours=23)
    t_end = bounds.last_end
    print(f"Stream covered readings from {t0} to {t_end}")

    batch = (
        spark.read.parquet(CLEAN_PARQUET)
        .filter((F.col("datetime") >= F.lit(t0)) & (F.col("datetime") < F.lit(t_end)))
        .groupBy(F.window("datetime", "24 hours", "1 hour"), "city")
        .agg(
            F.round(F.avg("pm25"), 2).alias("batch_avg_pm25"),
            F.count("*").alias("batch_readings"),
        )
        .select(F.col("window.start").alias("window_start"), "city", "batch_avg_pm25", "batch_readings")
    )

    compared = (
        stream.select("window_start", "window_end", "city", "avg_pm25", "readings")
        .join(batch, on=["window_start", "city"], how="left")
        .withColumn("abs_diff", F.abs(F.col("avg_pm25") - F.col("batch_avg_pm25")))
        .withColumn("complete_window", F.col("window_start") >= F.lit(t0))
    ).cache()

    summary = compared.agg(
        F.count("*").alias("windows"),
        F.sum(F.col("complete_window").cast("int")).alias("complete_windows"),
        F.sum(F.col("batch_avg_pm25").isNull().cast("int")).alias("missing_in_batch"),
        F.sum((F.col("readings") != F.col("batch_readings")).cast("int")).alias("count_mismatches"),
        F.max("abs_diff").alias("max_abs_diff"),
        F.round(F.avg("abs_diff"), 6).alias("mean_abs_diff"),
    ).first()

    duplicates = compared.groupBy("window_start", "city").count().filter("count > 1").count()

    print("\n================ STREAM vs BATCH ================")
    print(f"windows compared          : {summary.windows:,}")
    print(f"  of which full 24h       : {summary.complete_windows:,}")
    print(f"missing on batch side     : {summary.missing_in_batch}")
    print(f"duplicate stream windows  : {duplicates}")
    print(f"reading-count mismatches  : {summary.count_mismatches}")
    print(f"max |avg_pm25 difference| : {summary.max_abs_diff}")
    print(f"mean |difference|         : {summary.mean_abs_diff}")

    passed = (
        summary.windows > 0
        and summary.missing_in_batch == 0
        and duplicates == 0
        and summary.count_mismatches == 0
        and (summary.max_abs_diff or 0) <= TOLERANCE
    )
    print(f"RESULT                    : {'PASS' if passed else 'FAIL'}")
    print("=================================================")

    os.makedirs(BATCH_DIR, exist_ok=True)
    report = {
        "stream_from": str(t0), "stream_to": str(t_end),
        "windows": summary.windows, "complete_windows": summary.complete_windows,
        "missing_in_batch": summary.missing_in_batch, "duplicate_windows": duplicates,
        "count_mismatches": summary.count_mismatches,
        "max_abs_diff": summary.max_abs_diff, "mean_abs_diff": summary.mean_abs_diff,
        "result": "PASS" if passed else "FAIL",
    }
    pd.DataFrame([report]).to_csv(os.path.join(BATCH_DIR, "validation_report.csv"), index=False)
    # Every compared window, for the stream-vs-batch scatter plot in the report
    compared.select("window_start", "city", "avg_pm25", "batch_avg_pm25", "readings", "batch_readings") \
        .toPandas().to_csv(os.path.join(BATCH_DIR, "validation_pairs.csv"), index=False)

    if not passed:
        print("\nLargest differences:")
        compared.orderBy(F.desc_nulls_first("abs_diff")).show(10, truncate=False)
    spark.stop()
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
