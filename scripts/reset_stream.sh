#!/usr/bin/env bash
# Linux/Mac version of scripts/reset_stream.ps1 -- see that file for details.
set -euo pipefail
TOPIC=air_quality_readings
KT=/opt/kafka/bin/kafka-topics.sh

docker compose up -d spark >/dev/null
docker compose restart spark >/dev/null
rm -rf output/delta output/checkpoints
docker compose exec spark bash -c "rm -rf /checkpoints/*"
docker compose exec kafka $KT --bootstrap-server localhost:9092 --delete --topic $TOPIC --if-exists 2>/dev/null || true
sleep 3
docker compose exec kafka $KT --bootstrap-server localhost:9092 --create --topic $TOPIC --partitions 3 --replication-factor 1
echo "Done. Start the streaming job, then the producer."
