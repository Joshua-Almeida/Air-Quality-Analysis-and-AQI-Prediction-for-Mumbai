"""
Step 2 - Data preparation.

Input : data/raw/station_hour.csv  (hourly readings per monitoring station)
        data/raw/stations.csv      (which city/state each station is in)
Output: data/processed/air_quality_clean.csv      (one sorted CSV, used by the producer)
        data/processed/air_quality_clean.parquet  (same data, fast for batch jobs)

What it does:
  1. joins every reading to its station to get City and State
  2. drops readings that have no AQI value (nothing to rank or alert on)
  3. fills missing pollutant values with the city's average for that month;
     if a city never measured that pollutant in that month, it falls back to
     the all-India average for that month
  4. sorts everything by timestamp so it can be replayed like a live feed

Run:  .\scripts\spark-submit.ps1 spark_jobs/prepare_data.py
"""
import glob
import os
import shutil
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from spark_jobs.common import (
    AQI_BUCKETS,
    CLEAN_CSV,
    CLEAN_PARQUET,
    POLLUTANT_RENAMES,
    POLLUTANTS,
    PROCESSED_DIR,
    STATION_HOUR_CSV,
    STATIONS_CSV,
    get_spark,
)

EXPECTED_READING_COLS = ["StationId", "Datetime", *POLLUTANT_RENAMES, "AQI", "AQI_Bucket"]
EXPECTED_STATION_COLS = ["StationId", "StationName", "City", "State"]


def check_columns(df, expected, file_name):
    """Stop early with a clear message if the CSV is not what we expect."""
    missing = [c for c in expected if c not in df.columns]
    if missing:
        sys.exit(f"{file_name} is missing columns {missing}. Found: {df.columns}")


def bucket_from_aqi(aqi_col):
    """Build a CASE WHEN expression that turns an AQI number into its CPCB category."""
    expr = None
    for upper, name in AQI_BUCKETS:
        cond = aqi_col <= upper
        expr = F.when(cond, name) if expr is None else expr.when(cond, name)
    return expr


def main():
    for path in (STATION_HOUR_CSV, STATIONS_CSV):
        if not os.path.exists(path):
            sys.exit(f"Missing {path}. Download the Kaggle dataset into data/raw/ first.")

    spark = get_spark("prepare_data")

    # ---- 1. Read both CSVs (everything as string first, then cast) ----------
    readings = spark.read.csv(STATION_HOUR_CSV, header=True)
    stations = spark.read.csv(STATIONS_CSV, header=True)
    check_columns(readings, EXPECTED_READING_COLS, "station_hour.csv")
    check_columns(stations, EXPECTED_STATION_COLS, "stations.csv")

    readings = readings.select(
        F.col("StationId").alias("station_id"),
        F.to_timestamp("Datetime", "yyyy-MM-dd HH:mm:ss").alias("datetime"),
        *[F.col(f"`{raw}`").cast("double").alias(new) for raw, new in POLLUTANT_RENAMES.items()],
        F.col("AQI").cast("double").alias("aqi"),
        F.col("AQI_Bucket").alias("aqi_bucket"),
    )
    stations = stations.select(
        F.col("StationId").alias("station_id"),
        F.col("StationName").alias("station_name"),
        F.col("City").alias("city"),
        F.col("State").alias("state"),
    )
    n_raw = readings.count()
    print(f"[prep] raw readings              : {n_raw:,}")

    # ---- 2. Join station -> city -------------------------------------------
    df = readings.join(stations, on="station_id", how="inner")
    df = df.filter(F.col("city").isNotNull() & F.col("datetime").isNotNull())
    print(f"[prep] after join with stations  : {df.count():,}")

    # ---- 3. Drop rows with no AQI ------------------------------------------
    df = df.filter(F.col("aqi").isNotNull())
    print(f"[prep] after dropping null AQI   : {df.count():,}")

    # Some rows have AQI but no bucket label -> derive it from the AQI number
    df = df.withColumn("aqi_bucket", F.coalesce(F.col("aqi_bucket"), bucket_from_aqi(F.col("aqi"))))

    df = (
        df.withColumn("year", F.year("datetime"))
        .withColumn("month", F.month("datetime"))
        .withColumn("hour", F.hour("datetime"))
    )

    # ---- 4. Fill missing pollutants ----------------------------------------
    # avg() ignores nulls, so avg over (city, year, month) is the monthly city
    # average of the values that DO exist.
    city_month = Window.partitionBy("city", "year", "month")
    india_month = Window.partitionBy("year", "month")
    missing_before = df.select([F.sum(F.col(p).isNull().cast("int")).alias(p) for p in POLLUTANTS]).first()
    for p in POLLUTANTS:
        df = df.withColumn(
            p,
            F.round(
                F.coalesce(F.col(p), F.avg(p).over(city_month), F.avg(p).over(india_month)),
                2,
            ),
        )
    print("[prep] missing values filled per pollutant:")
    for p in POLLUTANTS:
        print(f"         {p:<8} {missing_before[p]:>10,}")

    df = df.dropna(subset=POLLUTANTS)
    df = df.select(
        "station_id", "station_name", "city", "state", "datetime",
        *POLLUTANTS, "aqi", "aqi_bucket", "year", "month", "hour",
    ).persist()
    n_clean = df.count()
    print(f"[prep] clean rows                : {n_clean:,}  ({n_clean / n_raw:.1%} of raw)")
    print(f"[prep] cities                    : {df.select('city').distinct().count()}")

    # ---- 5. Parquet copy for the batch jobs --------------------------------
    df.write.mode("overwrite").parquet(CLEAN_PARQUET)
    print(f"[prep] wrote {CLEAN_PARQUET}")

    # ---- 6. One CSV sorted by time for the producer ------------------------
    # repartition(1) + sortWithinPartitions guarantees a single, fully sorted file.
    tmp_dir = os.path.join(PROCESSED_DIR, "_csv_tmp")
    (
        df.drop("year", "month", "hour")
        .repartition(1)
        .sortWithinPartitions("datetime", "station_id")
        .write.mode("overwrite")
        .option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(tmp_dir)
    )
    part_file = glob.glob(os.path.join(tmp_dir, "part-*.csv"))[0]
    if os.path.exists(CLEAN_CSV):
        os.remove(CLEAN_CSV)
    shutil.move(part_file, CLEAN_CSV)
    shutil.rmtree(tmp_dir)
    print(f"[prep] wrote {CLEAN_CSV}")

    spark.stop()


if __name__ == "__main__":
    main()
