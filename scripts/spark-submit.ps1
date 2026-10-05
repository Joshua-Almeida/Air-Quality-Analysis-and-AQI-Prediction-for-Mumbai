# Runs a PySpark job inside the `spark` container with the Kafka connector
# and Delta Lake jars. Usage (from the project root):
#
#   .\scripts\spark-submit.ps1 spark_jobs/prepare_data.py
#   .\scripts\spark-submit.ps1 spark_jobs/streaming_job.py --alert-threshold 300
#
# The connector versions MUST match the Spark version (3.5.9, Scala 2.12):
#   org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9   -> Kafka source for Structured Streaming
#   io.delta:delta-spark_2.12:3.3.2                    -> Delta Lake 3.3 (built for Spark 3.5)
#
# Set $env:SPARK_DRIVER_MEMORY = "3g" before running to give a job more memory.

param(
    [Parameter(Mandatory = $true)][string]$Script,
    [Parameter(ValueFromRemainingArguments = $true)]$JobArgs
)

$packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,io.delta:delta-spark_2.12:3.3.2"
$memory = if ($env:SPARK_DRIVER_MEMORY) { $env:SPARK_DRIVER_MEMORY } else { "2g" }

docker compose exec spark spark-submit `
    --master "local[*]" `
    --driver-memory $memory `
    --packages $packages `
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
    "/app/$Script" @JobArgs
