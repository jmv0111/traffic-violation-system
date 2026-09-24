"""Ingestion: replay camera telemetry into Apache Kafka.

Each dataset row becomes one JSON message on the telemetry topic, keyed by plate
number so every reading of a vehicle lands in the same partition, in order.

Pacing follows the dataset's own event times. ``--speedup 10`` replays ten times
faster, and ``--no-pacing`` sends as fast as the broker accepts (stress test).
By default, timestamps are shifted so the replay starts "now"; ``--loop`` replays
the dataset forever, shifting each pass forward in time.

    python -m ncap.producer --speedup 5 --loop
"""
from __future__ import annotations

import argparse
import json
import signal
import time
from datetime import datetime, timedelta
from pathlib import Path

from ncap import config
from ncap.dataset.loader import MANILA_TZ, load_events
from ncap.schemas import TELEMETRY_FIELDS

LOOP_GAP = timedelta(seconds=10)  # event-time gap between replay passes
REPORT_EVERY_SECONDS = 5


def to_message(event: dict, time_offset: timedelta) -> dict:
    """Build the Kafka payload: telemetry fields only, event time shifted by ``time_offset``."""
    message = {field: event.get(field) for field in TELEMETRY_FIELDS}
    message["event_time"] = (event["event_time"] + time_offset).isoformat(timespec="milliseconds")
    return message


def create_producer():
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "ncap-camera-simulator",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "batch.num.messages": 10000,
    })


class Stats:
    def __init__(self):
        self.sent = 0
        self.failed = 0
        self.started = time.monotonic()
        self.last_report = self.started

    def on_delivery(self, err, _msg):
        if err is not None:
            self.failed += 1
            print(f"Delivery failed: {err}", flush=True)

    def maybe_report(self, force: bool = False):
        now = time.monotonic()
        if force or now - self.last_report >= REPORT_EVERY_SECONDS:
            rate = self.sent / max(now - self.started, 1e-9)
            print(f"sent={self.sent} failed={self.failed} rate={rate:,.0f} events/s", flush=True)
            self.last_report = now


def stream(events: list[dict], topic: str, speedup: float, pacing: bool, loop: bool,
           keep_timestamps: bool, limit: int | None) -> None:
    producer = create_producer()
    stats = Stats()
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    dataset_start = events[0]["event_time"]
    pass_span = events[-1]["event_time"] - dataset_start + LOOP_GAP
    base_offset = timedelta(0) if keep_timestamps else datetime.now(MANILA_TZ) - dataset_start
    wall_start = time.monotonic()
    replay = 0

    print(f"Streaming {len(events)} events per pass to '{topic}' on {config.KAFKA_BOOTSTRAP_SERVERS} "
          f"(speedup={speedup}, pacing={pacing}, loop={loop})", flush=True)

    while not stopping:
        for event in events:
            if stopping or (limit is not None and stats.sent >= limit):
                stopping = True
                break

            if pacing:
                simulated = replay * pass_span + (event["event_time"] - dataset_start)
                delay = simulated.total_seconds() / speedup - (time.monotonic() - wall_start)
                if delay > 0:
                    time.sleep(delay)

            message = to_message(event, base_offset + replay * pass_span)
            payload = json.dumps(message).encode("utf-8")
            while True:
                try:
                    producer.produce(topic, key=event["plate_number"].encode("utf-8"),
                                     value=payload, on_delivery=stats.on_delivery)
                    break
                except BufferError:  # local queue full: let it drain
                    producer.poll(0.5)
            stats.sent += 1
            producer.poll(0)
            stats.maybe_report()

        replay += 1
        if not loop:
            break

    producer.flush(30)
    stats.maybe_report(force=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay traffic camera telemetry into Kafka.")
    parser.add_argument("--dataset", type=Path, default=config.DATASET_FILE)
    parser.add_argument("--topic", default=config.KAFKA_TOPIC)
    parser.add_argument("--speedup", type=float, default=1.0, help="replay speed multiplier")
    parser.add_argument("--no-pacing", action="store_true", help="send as fast as possible")
    parser.add_argument("--loop", action="store_true", help="replay the dataset forever")
    parser.add_argument("--keep-timestamps", action="store_true",
                        help="keep the dataset's original event times instead of starting now")
    parser.add_argument("--limit", type=int, help="stop after this many events")
    args = parser.parse_args()

    if args.speedup <= 0:
        parser.error("--speedup must be positive")

    if not args.dataset.exists():
        if args.dataset.resolve() != config.DATASET_FILE.resolve():
            parser.error(f"dataset not found: {args.dataset}")
        from ncap.dataset.generate_placeholder import generate_dataset

        print(f"{args.dataset} not found; generating the placeholder dataset", flush=True)
        generate_dataset(args.dataset)

    events = load_events(args.dataset)
    if not events:
        parser.error(f"dataset is empty: {args.dataset}")
    stream(events, args.topic, args.speedup, not args.no_pacing, args.loop,
           args.keep_timestamps, args.limit)


if __name__ == "__main__":
    main()
