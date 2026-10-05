# Start the live demo from a clean slate:
#   1. stops any Spark job running in the container (restarts the container)
#   2. deletes the streaming Delta tables in output/delta/
#   3. deletes the streaming checkpoints (the /checkpoints Docker volume)
#   4. deletes and re-creates the Kafka topic, so old messages are gone
#
# Batch results (output/batch, output/ml) and the cleaned data are kept.
# Usage (from the project root):   .\scripts\reset_stream.ps1

# Docker prints progress on stderr; Windows PowerShell would treat that as an error.
$ErrorActionPreference = "Continue"
$topic = "air_quality_readings"
$kafkaTopics = "/opt/kafka/bin/kafka-topics.sh"

Write-Host "Restarting the spark container (stops running jobs)..."
docker compose up -d spark 2>&1 | Out-Null
docker compose restart spark 2>&1 | Out-Null

Write-Host "Removing streaming Delta tables and checkpoints..."
if (Test-Path "output/delta") { Remove-Item -Recurse -Force "output/delta" }
if (Test-Path "output/checkpoints") { Remove-Item -Recurse -Force "output/checkpoints" }
docker compose exec spark bash -c "rm -rf /checkpoints/*"

Write-Host "Re-creating Kafka topic '$topic'..."
docker compose exec kafka $kafkaTopics --bootstrap-server localhost:9092 --delete --topic $topic --if-exists 2>&1 | Out-Null
Start-Sleep -Seconds 3
docker compose exec kafka $kafkaTopics --bootstrap-server localhost:9092 --create --topic $topic --partitions 3 --replication-factor 1 2>&1 |
    Where-Object { "$_" -notmatch "WARNING: Due to limitations" }
docker compose exec kafka $kafkaTopics --bootstrap-server localhost:9092 --describe --topic $topic

Write-Host "Done. Start the streaming job, then the producer."
