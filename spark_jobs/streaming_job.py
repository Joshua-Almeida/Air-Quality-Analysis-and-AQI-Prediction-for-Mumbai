"""
Step 4 - Spark Structured Streaming job.

Reads station readings from Kafka and keeps four Delta tables up to date:

  city_hourly       one row per city per hour (average / max AQI, PM2.5, PM10)
  city_rolling_24h  (a) rolling 24-hour average PM2.5 and AQI per city
  top10_ranking     (b) the 10 most polluted cities in the latest 24-hour window
  alerts            (c) every station reading whose AQI is above the threshold

Event time vs processing time
-----------------------------
All windows use the reading's own timestamp (`event_time`), not the time Spark
happened to receive it. The watermark ("2 hours") tells Spark how long to wait
for late readings: once it has seen data up to 15:00, it treats every window
ending at or before 13:00 as finished, emits it once, and forgets its state.
Because we write in *append* mode, every row in the output tables is final.

Run (Kafka and the producer can be started before or after):
    .\scripts\spark-submit.ps1 spark_jobs/streaming_job.py
    .\scripts\spark-submit.ps1 spark_jobs/streaming_job.py --alert-threshold 400
Stop with Ctrl+C. Restarting continues from the checkpoints (the /checkpoints
Docker volume); scripts/reset_stream.ps1 wipes them for a fresh replay.
"""
import argparse
import os

from pyspark.sql import Window
from pyspark.sql import functions as F

from spark_jobs.common import (
    ALERTS_TABLE,
    CHECKPOINT_DIR,
    CITY_HOURLY_TABLE,
    CITY_ROLLING_TABLE,
    KAFKA_BOOTSTRAP,
    READING_SCHEMA,
    TOP10_TABLE,
    TOPIC,
    get_spark,
)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--alert-threshold", type=float, default=300,
                   help="AQI above this raises an alert (300 = 'Very Poor' or worse)")
    p.add_argument("--watermark", default="2 hours", help="how late a reading may arrive")
    p.add_argument("--trigger", default="5 seconds", help="how often a micro-batch runs")
    p.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    return p.parse_args()


def read_readings(spark, starting_offsets):
    """Kafka -> DataFrame of parsed readings with a proper event_time column."""
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")   # tolerate a re-created topic during demos
        .load()
    )
    # Kafka gives us bytes; the message body is JSON matching READING_SCHEMA
    return (
        kafka_df.select(
            F.from_json(F.col("value").cast("string"), READING_SCHEMA).alias("r"),
            F.col("timestamp").alias("received_at"),
        )
        .select("r.*", "received_at")
        .withColumn("event_time", F.to_timestamp("datetime", "yyyy-MM-dd HH:mm:ss"))
        .filter(F.col("event_time").isNotNull() & F.col("city").isNotNull())
    )


def write_rolling_and_ranking(batch_df, batch_id):
    """foreachBatch handler for the 24-hour windows.

    Each micro-batch contains the 24-hour windows that have just been finalised.
    We (1) append them to city_rolling_24h and (2) rebuild the top-10 ranking
    from the newest window. txnAppId/txnVersion make the append idempotent:
    if Spark retries a batch after a crash, Delta skips the duplicate write.
    """
    if batch_df.isEmpty():
        return
    batch_df.persist()

    (
        batch_df.write.format("delta").mode("append")
        .option("txnAppId", "city_rolling_24h")
        .option("txnVersion", batch_id)
        .save(CITY_ROLLING_TABLE)
    )

    latest_end = batch_df.agg(F.max("window_end")).first()[0]
    by_aqi = Window.orderBy(F.desc("avg_aqi"))
    top10 = (
        batch_df.filter(F.col("window_end") == latest_end)
        .withColumn("rank", F.row_number().over(by_aqi))
        .filter(F.col("rank") <= 10)
        .withColumn("updated_at", F.current_timestamp())
    )
    top10.write.format("delta").mode("overwrite").save(TOP10_TABLE)

    print(f"[batch {batch_id}] {batch_df.count()} new 24h windows, ranking updated for window ending {latest_end}")
    batch_df.unpersist()


def main():
    args = parse_args()
    # Few partitions = few state-store files per micro-batch; ~25 cities need no more.
    spark = get_spark("air_quality_streaming", shuffle_partitions=4)
    readings = read_readings(spark, args.starting_offsets)

    # Late-data rule shared by both windowed aggregations
    watermarked = readings.withWatermark("event_time", args.watermark)

    # ---- (a) + (b): rolling 24-hour average per city, sliding every hour ----
    # window("24 hours", "1 hour") puts each reading into 24 overlapping windows,
    # so the window ending at 14:00 is "the average of the last 24 hours at 14:00".
    rolling = (
        watermarked.groupBy(F.window("event_time", "24 hours", "1 hour"), "city")
        .agg(
            F.round(F.avg("pm25"), 2).alias("avg_pm25"),
            F.round(F.avg("aqi"), 1).alias("avg_aqi"),
            F.max("aqi").alias("max_aqi"),
            F.count("*").alias("readings"),
            F.min("event_time").alias("first_reading"),
            F.max("event_time").alias("last_reading"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city", "avg_pm25", "avg_aqi", "max_aqi", "readings",
            "first_reading", "last_reading",
        )
    )
    rolling_query = (
        rolling.writeStream.queryName("city_rolling_24h")
        .outputMode("append")
        .foreachBatch(write_rolling_and_ranking)
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "city_rolling_24h"))
        .trigger(processingTime=args.trigger)
        .start()
    )

    # ---- latest AQI per city: 1-hour tumbling windows ----------------------
    hourly = (
        watermarked.groupBy(F.window("event_time", "1 hour"), "city", "state")
        .agg(
            F.round(F.avg("aqi"), 1).alias("avg_aqi"),
            F.max("aqi").alias("max_aqi"),
            F.round(F.avg("pm25"), 2).alias("avg_pm25"),
            F.round(F.avg("pm10"), 2).alias("avg_pm10"),
            F.count("*").alias("stations"),
        )
        .select(
            F.col("window.start").alias("hour"),
            "city", "state", "avg_aqi", "max_aqi", "avg_pm25", "avg_pm10", "stations",
        )
    )
    hourly_query = (
        hourly.writeStream.queryName("city_hourly")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "city_hourly"))
        .trigger(processingTime=args.trigger)
        .start(CITY_HOURLY_TABLE)
    )

    # ---- (c) alerts: no aggregation needed, just a filter ------------------
    alerts = readings.filter(F.col("aqi") > args.alert_threshold).select(
        "event_time", "station_id", "station_name", "city", "state",
        "aqi", "aqi_bucket", "pm25", "pm10", "received_at",
    )
    alerts_query = (
        alerts.writeStream.queryName("alerts")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "alerts"))
        .trigger(processingTime=args.trigger)
        .start(ALERTS_TABLE)
    )

    print("Streaming queries running:")
    for q in (rolling_query, hourly_query, alerts_query):
        print(f"  - {q.name}")
    print(f"Reading topic '{TOPIC}' from {KAFKA_BOOTSTRAP}. Alert threshold AQI > {args.alert_threshold}. Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
