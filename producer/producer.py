"""
Step 3 - Kafka producer: replays the cleaned CPCB data as if it were live.

The cleaned CSV is sorted by time, so all stations' readings for one hour sit
next to each other. We read the file row by row, collect one hour's worth of
readings, send each reading to Kafka as a JSON message, then wait `--speed`
seconds before sending the next hour. With the default of 2 seconds, one real
minute replays 30 hours of air quality data.

Runs on your laptop (not in Docker):
    python producer/producer.py                          # from 2019-11-01, one hour every 2 s
    python producer/producer.py --start 2020-03-01 --speed 0.5
    python producer/producer.py --max-hours 72           # stop after 3 simulated days

Example message (key = city, value = JSON):
    {"station_id": "MH005", "station_name": "Bandra, Mumbai - MPCB", "city": "Mumbai",
     "state": "Maharashtra", "datetime": "2019-11-01 13:00:00", "pm25": 41.2, ...,
     "aqi": 112.0, "aqi_bucket": "Moderate"}
"""
import argparse
import csv
import json
import os
import sys
import time

from confluent_kafka import Producer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CSV = os.path.join(ROOT, "data", "processed", "air_quality_clean.csv")

TEXT_FIELDS = {"station_id", "station_name", "city", "state", "datetime", "aqi_bucket"}


def parse_args():
    p = argparse.ArgumentParser(description="Replay CPCB hourly data into Kafka")
    p.add_argument("--csv", default=DEFAULT_CSV, help="cleaned, time-sorted CSV")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka broker address")
    p.add_argument("--topic", default="air_quality_readings")
    p.add_argument("--speed", type=float, default=2.0,
                   help="real seconds per simulated hour (0 = as fast as possible)")
    p.add_argument("--start", default="2019-11-01",
                   help="first timestamp to send, e.g. 2019-11-01 or '2019-11-01 06:00:00'")
    p.add_argument("--end", default=None, help="stop before this timestamp")
    p.add_argument("--max-hours", type=int, default=None, help="stop after this many simulated hours")
    return p.parse_args()


def hours_from_csv(path, start, end):
    """Yield (timestamp, [rows]) one simulated hour at a time.

    Timestamps look like '2019-11-01 13:00:00', so plain string comparison
    gives the right chronological order.
    """
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        current_ts, batch = None, []
        for row in reader:
            ts = row["datetime"]
            if start and ts < start:
                continue
            if end and ts >= end:
                break
            if ts != current_ts and batch:
                yield current_ts, batch
                batch = []
            current_ts = ts
            batch.append(row)
        if batch:
            yield current_ts, batch


def to_message(row):
    """CSV values are all strings; turn the numeric ones into numbers."""
    msg = {}
    for key, value in row.items():
        if key in TEXT_FIELDS:
            msg[key] = value
        else:
            msg[key] = float(value) if value != "" else None
    return msg


def main():
    args = parse_args()
    if not os.path.exists(args.csv):
        sys.exit(f"{args.csv} not found - run spark_jobs/prepare_data.py first.")

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 50,              # group small messages into larger network batches
        "compression.type": "lz4",
    })

    failures = []

    def on_delivery(err, _msg):
        if err is not None:
            failures.append(err)

    print(f"Replaying {args.csv}")
    print(f"  -> topic '{args.topic}' on {args.bootstrap}, one hour every {args.speed}s, from {args.start}")
    print("  (skipping rows before the start date can take a few seconds)")

    sent_hours = sent_msgs = 0
    try:
        for ts, rows in hours_from_csv(args.csv, args.start, args.end):
            t0 = time.time()
            for row in rows:
                msg = to_message(row)
                producer.produce(
                    args.topic,
                    key=msg["city"].encode("utf-8"),
                    value=json.dumps(msg).encode("utf-8"),
                    on_delivery=on_delivery,
                )
                producer.poll(0)      # serve delivery callbacks
            producer.flush()          # make sure the whole hour is in Kafka

            sent_hours += 1
            sent_msgs += len(rows)
            cities = len({r["city"] for r in rows})
            print(f"[{ts}] sent {len(rows):>3} readings from {cities:>2} cities "
                  f"(total {sent_msgs:,} msgs, {sent_hours} hours)", flush=True)
            if failures:
                sys.exit(f"Kafka delivery failed: {failures[0]}")

            if args.max_hours and sent_hours >= args.max_hours:
                break
            time.sleep(max(0.0, args.speed - (time.time() - t0)))
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()
    print(f"Done: {sent_msgs:,} messages, {sent_hours} simulated hours.")


if __name__ == "__main__":
    main()
