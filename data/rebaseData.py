#!/usr/bin/env python3
"""Create a separate CSV with hospital timestamps shifted onto a shared timeline."""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

TIME_FMT = "%Y-%m-%d %H:%M:%S"
TIME_COLS = [
    "event_time", "event_end_time", "admittime", "dischtime", "edregtime",
    "edouttime", "transfer_intime", "transfer_outtime", "icu_intime", "icu_outtime",
]


def rebase_times(rows, mean_arrival_gap_min, seed):
    rng = random.Random(seed)
    groups = {}
    for i, row in enumerate(rows):
        key = ("adm", row["hadm_id"]) if row["hadm_id"] else ("event", i)
        groups.setdefault(key, []).append(row)

    ordered = sorted(groups.values(), key=lambda g: min(r["event_time"] for r in g))
    sim_start = datetime.now().replace(microsecond=0)
    new_start = sim_start

    for group in ordered:
        earliest = min(r["event_time"] for r in group)
        new_start += timedelta(seconds=rng.expovariate(1 / (mean_arrival_gap_min * 60)))
        offset = new_start - earliest
        for row in group:
            row["original_event_time"] = row["event_time"]
            for col in TIME_COLS:
                if row.get(col):
                    row[col] += offset

    rows.sort(key=lambda r: r["event_time"])
    return sim_start


def main():
    parser = argparse.ArgumentParser(description="Rebase hospital CSV timestamps")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--arrival-gap-min", type=float, default=60)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.arrival_gap_min <= 0:
        parser.error("--arrival-gap-min must be greater than 0")

    output = args.output or args.input.with_name(args.input.stem + "_rebased" + args.input.suffix)
    with args.input.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames, rows = reader.fieldnames, list(reader)

    for row in rows:
        for col in TIME_COLS:
            value = row.get(col)
            row[col] = datetime.strptime(value, TIME_FMT) if value else None
        if row["event_time"] is None:
            raise ValueError("Every row must have a valid event_time")

    start = rebase_times(rows, args.arrival_gap_min, args.seed)
    for row in rows:
        for col in TIME_COLS + ["original_event_time"]:
            if row.get(col):
                row[col] = row[col].strftime(TIME_FMT)

    with output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames + ["original_event_time"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Rebased {len(rows)} rows | start={start} | output={output}")


if __name__ == "__main__":
    main()