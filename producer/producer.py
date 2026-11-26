#!/usr/bin/env python3

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

LOG = logging.getLogger("hospital_producer")

TIME_FMT = "%Y-%m-%d %H:%M:%S"
TIME_COLS = [
    "event_time", "event_end_time", "admittime", "dischtime", "edregtime",
    "edouttime", "transfer_intime", "transfer_outtime", "icu_intime", "icu_outtime",
]
INT_COLS = ["subject_id", "hadm_id", "transfer_id", "stay_id", "anchor_age", "anchor_year"]
FLOAT_COLS = ["icu_los_days"]
REQUIRED_COLUMNS = {"event_source", "event_type", "event_time", "subject_id"}

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "data" / "hospital_rebased.csv"


def convert(function, text):
    """function(text), or None if the cell is empty / not convertible."""
    try:
        return function(text) if text else None
    except ValueError:
        return None


def load_rows(path):
    """
    CSV -> list of dicts with real types (empty cell -> None).
    A row without a valid event_time or subject_id is skipped with a warning:
    event_time drives the replay, subject_id is the Kafka key.
    """
    rows, skipped = [], 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            sys.exit(f"[error] missing columns in {path}: {sorted(missing)}")

        for line, raw in enumerate(reader, start=2):          # line 1 is the header
            row = {k: (v if v and v.strip() else None) for k, v in raw.items()}
            for c in TIME_COLS:
                row[c] = convert(lambda t: datetime.strptime(t, TIME_FMT), row.get(c))
            for c in INT_COLS:
                row[c] = convert(lambda t: int(float(t)), row.get(c))    # "25563031.0" -> 25563031
            for c in FLOAT_COLS:
                row[c] = convert(float, row.get(c))

            if row["event_time"] is None or row["subject_id"] is None:
                skipped += 1
                LOG.warning("CSV line %d skipped: missing/invalid event_time or subject_id", line)
                continue
            rows.append(row)

    if skipped:
        LOG.warning("%d row(s) skipped in total", skipped)
    return rows

# Building the JSON message

def to_message(row, seq):
    msg = {k: (v.strftime(TIME_FMT) if isinstance(v, datetime) else v) for k, v in row.items()}
    if not msg.get("original_event_time"):          # file was not rebased: original = event time
        msg["original_event_time"] = msg["event_time"]
    msg["producer_seq"] = seq                                        # 1..N
    msg["ingest_time"] = datetime.now().strftime(TIME_FMT)           # real send time
    return msg


# Kafka helpers

def make_producer(bootstrap):
    from confluent_kafka import Producer            # imported here so --dry-run works without it
    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "hospital-raw-producer",
        "acks": "all",                              # wait until the broker confirms
        "enable.idempotence": True,                 # no duplicates / no reordering on retry
        "linger.ms": 20,
        "message.timeout.ms": 120000,               # give up on a message after 2 minutes
    })


def ensure_topic(bootstrap, topic, partitions):
    """Create the topic if missing (same as the command in kafka/kafka-topic.txt)."""
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for name, future in admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=1)]).items():
        try:
            future.result()
            LOG.info("Created topic '%s' (%d partitions)", name, partitions)
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise
            LOG.info("Topic '%s' already exists", name)


def send(producer, topic, key, value, on_delivery):
    """produce() raises BufferError when the local queue is full: wait a bit and retry."""
    while True:
        try:
            producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
            return
        except BufferError:
            producer.poll(0.5)

# Main

def main():
    ap = argparse.ArgumentParser(description="Replay the rebased hospital CSV into Kafka as a live stream.")
    ap.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="rebased CSV (default: data/hospital_rebased.csv)")
    ap.add_argument("--bootstrap", default="localhost:9092", help="Kafka address seen from the host")
    ap.add_argument("--topic", default="hospital_raw")
    ap.add_argument("--partitions", type=int, default=3, help="used only if the topic must be created")
    ap.add_argument("--speed", type=float, default=3600,
                    help="simulated seconds per real second (default pacing)")
    ap.add_argument("--max-sleep", type=float, default=3.0,
                    help="with --speed: never wait more than this many real seconds between two messages")
    ap.add_argument("--rate", type=float, default=0,
                    help="fixed messages per second; if set, replaces --speed pacing")
    ap.add_argument("--limit", type=int, default=0, help="send only the first N messages (0 = all)")
    ap.add_argument("--log-every", type=int, default=100,
                    help="log partition/offset once per this many delivered messages")
    ap.add_argument("--dry-run", action="store_true", help="print messages instead of sending to Kafka")
    args = ap.parse_args()

    if args.speed <= 0:
        ap.error("--speed must be greater than 0")
    if args.rate < 0 or args.max_sleep < 0 or args.limit < 0:
        ap.error("--rate, --max-sleep and --limit cannot be negative")
    if args.partitions < 1 or args.log_every < 1:
        ap.error("--partitions and --log-every must be at least 1")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")

    if not args.csv.is_file():
        sys.exit(f"[error] file not found: {args.csv}\n"
                 f"        it must be the REBASED csv (data/hospital_rebased.csv); use --csv to give another path")

    rows = load_rows(args.csv)
    if not rows:
        sys.exit("[error] no usable rows in the CSV - nothing to send")
    rows.sort(key=lambda r: r["event_time"])        # stable sort: equal times keep the file order
    if args.limit:
        rows = rows[: args.limit]
    LOG.info("%d messages ready for topic '%s'", len(rows), args.topic)

    producer = None
    if not args.dry_run:
        ensure_topic(args.bootstrap, args.topic, args.partitions)
        producer = make_producer(args.bootstrap)

    counts = {"queued": 0, "delivered": 0, "failed": 0}

    def on_delivery(error, message):                # Kafka calls this once per message
        if error:
            counts["failed"] += 1
            LOG.error("Delivery failed: %s", error)
            return
        counts["delivered"] += 1
        if counts["delivered"] % args.log_every == 0:
            LOG.info("Delivered %d | partition %d | offset %d",
                     counts["delivered"], message.partition(), message.offset())

    started = time.monotonic()
    due = started                                    # real-clock time the current message is due
    previous_time = rows[0]["event_time"]

    try:
        for n, row in enumerate(rows, start=1):
            # --- pacing: how long after the previous message should this one go out? ---
            if args.rate:
                delay = 1 / args.rate
            else:
                gap = (row["event_time"] - previous_time).total_seconds() / args.speed
                delay = min(gap, args.max_sleep)
            previous_time = row["event_time"]
            # due = real-clock moment this message should go out. Adding the delay to the previous
            # due time (not to "now") avoids drift; max(..., now) means a late message is sent at once.
            due = max(due + delay, time.monotonic())
            if due > time.monotonic():
                time.sleep(due - time.monotonic())

            # --- send ---
            msg = to_message(row, n)
            payload = json.dumps(msg, ensure_ascii=False, separators=(",", ":"))
            if args.dry_run:
                print(payload)
                counts["queued"] += 1
                counts["delivered"] += 1
            else:
                # key = patient id -> one patient's events stay in order inside one partition.
                # Kafka does NOT order across partitions, so Spark must use event_time (+ watermark).
                send(producer, args.topic, str(msg["subject_id"]), payload, on_delivery)
                counts["queued"] += 1
                producer.poll(0)                     # serve delivery callbacks
                LOG.info("[%d/%d] %s | %-9s | patient %s | %s", n, len(rows),
                         msg["event_time"], msg["event_type"], msg["subject_id"], msg["careunit"] or "-")
    except KeyboardInterrupt:
        LOG.warning("Stopped by user (Ctrl+C)")

    remaining = 0
    if producer:
        LOG.info("Flushing pending messages...")
        remaining = producer.flush(30)               # number of messages STILL not delivered
        if remaining:
            LOG.error("%d message(s) remain undelivered", remaining)

    LOG.info("Finished | queued=%d delivered=%d failed=%d undelivered=%d | %.1fs",
             counts["queued"], counts["delivered"], counts["failed"], remaining, time.monotonic() - started)
    return 1 if (counts["failed"] or remaining) else 0


if __name__ == "__main__":
    sys.exit(main())