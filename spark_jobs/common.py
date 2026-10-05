"""
Shared settings for every Spark job: file paths, the Kafka topic, the message
schema and a helper that creates a SparkSession with Delta Lake enabled.

All jobs run inside the `spark` Docker container, where the project folder is
mounted at /app.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# ---------------------------------------------------------------- paths -----
PROJECT_DIR = os.environ.get("PROJECT_DIR", "/app")

RAW_DIR = os.path.join(PROJECT_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(PROJECT_DIR, "data", "processed")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")

STATION_HOUR_CSV = os.path.join(RAW_DIR, "station_hour.csv")
STATIONS_CSV = os.path.join(RAW_DIR, "stations.csv")

CLEAN_CSV = os.path.join(PROCESSED_DIR, "air_quality_clean.csv")
CLEAN_PARQUET = os.path.join(PROCESSED_DIR, "air_quality_clean.parquet")

DELTA_DIR = os.path.join(OUTPUT_DIR, "delta")            # streaming results
# Streaming progress (Kafka offsets + window state). docker-compose points this
# at a Docker volume, because checkpoints write many tiny files and the Windows
# bind mount is slow at that.
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", os.path.join(OUTPUT_DIR, "checkpoints"))
BATCH_DIR = os.path.join(OUTPUT_DIR, "batch")            # batch analysis CSVs
ML_DIR = os.path.join(OUTPUT_DIR, "ml")                  # model metrics

# One Delta table per streaming result
CITY_HOURLY_TABLE = os.path.join(DELTA_DIR, "city_hourly")
CITY_ROLLING_TABLE = os.path.join(DELTA_DIR, "city_rolling_24h")
TOP10_TABLE = os.path.join(DELTA_DIR, "top10_ranking")
ALERTS_TABLE = os.path.join(DELTA_DIR, "alerts")

# ---------------------------------------------------------------- kafka -----
# Inside Docker, Spark reaches the broker by its service name.
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = "air_quality_readings"

# ------------------------------------------------------------- columns ------
# The raw CSV uses names like "PM2.5"; a dot inside a column name is awkward in
# Spark (it means "field of a struct"), so we rename everything to snake_case.
POLLUTANT_RENAMES = {
    "PM2.5": "pm25",
    "PM10": "pm10",
    "NO": "no",
    "NO2": "no2",
    "NOx": "nox",
    "NH3": "nh3",
    "CO": "co",
    "SO2": "so2",
    "O3": "o3",
    "Benzene": "benzene",
    "Toluene": "toluene",
    "Xylene": "xylene",
}
POLLUTANTS = list(POLLUTANT_RENAMES.values())

# Official CPCB AQI categories (upper bound of each band)
AQI_BUCKETS = [
    (50, "Good"),
    (100, "Satisfactory"),
    (200, "Moderate"),
    (300, "Poor"),
    (400, "Very Poor"),
    (float("inf"), "Severe"),
]
BUCKET_ORDER = [name for _, name in AQI_BUCKETS]

# The JSON message the producer sends for every station reading.
# `datetime` stays a string here and is converted to a timestamp in Spark.
READING_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("station_name", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("datetime", StringType()),
    ]
    + [StructField(p, DoubleType()) for p in POLLUTANTS]
    + [
        StructField("aqi", DoubleType()),
        StructField("aqi_bucket", StringType()),
    ]
)


def get_spark(app_name, shuffle_partitions=8, extra_conf=None):
    """Create (or reuse) a SparkSession with Delta Lake enabled.

    The Delta and Kafka jars themselves come from `spark-submit --packages ...`
    (see scripts/spark-submit.ps1); here we only switch the features on.
    """
    builder = SparkSession.builder
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = (
        builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # The CPCB timestamps are Indian local time without a zone. Using UTC
        # as the session zone means Spark never shifts them: "13:00" stays "13:00".
        .config("spark.sql.session.timeZone", "UTC")
        # The default of 200 shuffle partitions is far too many for one laptop
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
