"""Serving: HTTP API and live dashboard for stored violations.

Reads finalized violations from Cassandra and serves them to the web dashboard
(``web/index.html``) together with the health of each pipeline stage:

    GET /                    the dashboard
    GET /api/cameras         camera registry
    GET /api/violations      today's violations, newest first
    GET /api/stream          Server-Sent Events: one message per new violation
    GET /api/plate/{plate}   all violations of one vehicle (any day)
    GET /api/stats           headline numbers for today
    GET /api/summary         printable summary of today
    GET /api/health          Kafka, Spark, and Cassandra status

Today's violations are kept in memory. A background task polls Cassandra every
``API_POLL_SECONDS`` and pushes anything new to every open ``/api/stream``.

    uvicorn ncap.api:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from urllib.request import urlopen

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse

from ncap import config
from ncap.dataset.loader import MANILA_TZ, load_cameras

VIOLATION_TYPES = ("BEATING_RED_LIGHT", "SPEEDING", "ILLEGAL_PARKING")
HEALTH_POLL_SECONDS = 5
PER_MINUTE_SPAN = timedelta(minutes=5)  # "per minute" is averaged over this span
KEEPALIVE_SECONDS = 15

COLUMNS = ("violation_id, violation_time, violation_type, plate_number, vehicle_type, camera_id, "
           "location_name, recorded_speed_kph, speed_limit_kph, evidence, ticket_status, detected_at")
DAY_QUERY = (f"SELECT {COLUMNS} FROM violations_by_type "
             "WHERE violation_type = ? AND violation_date = ?")
RECENT_QUERY = DAY_QUERY + " AND violation_time >= ?"
PLATE_QUERY = f"SELECT {COLUMNS} FROM violations_by_plate WHERE plate_number = ? LIMIT ?"


def _iso(ts: datetime | None) -> str | None:
    if ts is None:
        return None
    if ts.tzinfo is None:  # the driver returns naive UTC timestamps
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(MANILA_TZ).isoformat(timespec="milliseconds")


def to_record(row) -> dict:
    """Convert a Cassandra violation row into the JSON record the dashboard uses."""
    return {
        "id": row.violation_id,
        "time": _iso(row.violation_time),
        "type": row.violation_type,
        "plate": row.plate_number,
        "vehicle": row.vehicle_type,
        "camera": row.camera_id,
        "location": row.location_name,
        "speed": row.recorded_speed_kph,
        "limit": row.speed_limit_kph,
        "evidence": row.evidence,
        "status": row.ticket_status,
        "detected": _iso(row.detected_at),
    }


def normalize_plate(plate: str) -> str:
    return " ".join(plate.upper().split())


def newest_first(records) -> list[dict]:
    # Every timestamp carries the same +08:00 offset, so ISO strings sort chronologically.
    return sorted(records, key=lambda r: r["time"] or "", reverse=True)


def _avg_speed(records: list[dict]) -> float:
    speeds = [r["speed"] for r in records if r["speed"] is not None]
    return round(sum(speeds) / len(speeds), 2) if speeds else 0.0


def compute_stats(records: list[dict], now: datetime) -> dict:
    by_camera = Counter(r["camera"] for r in records)
    locations = {r["camera"]: r["location"] for r in records}
    busiest = by_camera.most_common(1)
    since = now - PER_MINUTE_SPAN
    recent = sum(1 for r in records if r["detected"] and datetime.fromisoformat(r["detected"]) >= since)
    return {
        "total": len(records),
        "per_minute": round(recent / (PER_MINUTE_SPAN.total_seconds() / 60), 1),
        "avg_speed": _avg_speed(records),
        "busiest": locations[busiest[0][0]] if busiest else None,
        "by_camera": dict(by_camera),
    }


def compute_summary(records: list[dict], day: date) -> dict:
    times = sorted(r["time"] for r in records if r["time"])
    top_plate = Counter(r["plate"] for r in records).most_common(1)
    return {
        "date": day.isoformat(),
        "first": times[0] if times else None,
        "last": times[-1] if times else None,
        "total": len(records),
        "avg_speed": _avg_speed(records),
        "by_type": dict(Counter(r["type"] for r in records)),
        "by_location": dict(Counter(r["location"] for r in records).most_common()),
        "top_plate": list(top_plate[0]) if top_plate else None,
    }


class ViolationStore:
    """Today's violations, kept in memory and refreshed from Cassandra.

    Cassandra calls run in worker threads (``fetch``, ``plate_history``); the
    in-memory records are only changed on the event loop (``apply``).
    """

    def __init__(self):
        self.session = None
        self.statements = {}
        self.day: date | None = None
        self.records: dict[str, dict] = {}
        self.ok = False
        self.latency_ms = 0.0
        self.last_full_reload = 0.0

    def _connect(self):
        if self.session is None:
            from cassandra.cluster import Cluster

            cluster = Cluster([config.CASSANDRA_HOST], port=config.CASSANDRA_PORT)
            session = cluster.connect(config.CASSANDRA_KEYSPACE)
            self.statements = {name: session.prepare(cql) for name, cql in
                               [("day", DAY_QUERY), ("recent", RECENT_QUERY), ("plate", PLATE_QUERY)]}
            self.session = session
        return self.session

    def fetch(self, day: date, full: bool) -> list[dict]:
        """Read the whole day (``full``) or only the lookback window, for every type."""
        session = self._connect()
        started = time.perf_counter()
        if full:
            rows = [row for t in VIOLATION_TYPES
                    for row in session.execute(self.statements["day"], (t, day))]
        else:
            since = datetime.now(timezone.utc) - timedelta(minutes=config.API_LOOKBACK_MINUTES)
            rows = [row for t in VIOLATION_TYPES
                    for row in session.execute(self.statements["recent"], (t, day, since))]
        self.latency_ms = (time.perf_counter() - started) * 1000 / len(VIOLATION_TYPES)
        return [to_record(row) for row in rows]

    def apply(self, day: date, records: list[dict], full: bool) -> list[dict]:
        """Merge fetched records; return the ones not seen before, oldest first."""
        initial = self.day is None
        if day != self.day:
            self.day, self.records = day, {}
        if full:
            self.last_full_reload = time.monotonic()
        fresh = [r for r in records if r["id"] not in self.records]
        self.records.update((r["id"], r) for r in records)
        # The first load is history, not news; the dashboard fetches it with /api/violations.
        return [] if initial else sorted(fresh, key=lambda r: r["time"] or "")

    async def refresh(self) -> list[dict]:
        day = datetime.now(MANILA_TZ).date()
        full = (day != self.day
                or time.monotonic() - self.last_full_reload > config.API_FULL_RELOAD_SECONDS)
        try:
            records = await asyncio.to_thread(self.fetch, day, full)
        except Exception as exc:  # Cassandra down or schema not loaded yet: retry next poll
            if self.ok:
                print(f"Cassandra query failed: {exc}", flush=True)
            self.ok = False
            return []
        self.ok = True
        return self.apply(day, records, full)

    def plate_history(self, plate: str, limit: int) -> list[dict]:
        session = self._connect()
        return [to_record(row) for row in session.execute(self.statements["plate"], (plate, limit))]

    def latest_detection_age(self) -> float | None:
        detected = [r["detected"] for r in self.records.values() if r["detected"]]
        if not detected:
            return None
        newest = max(datetime.fromisoformat(d) for d in detected)
        return max(0.0, (datetime.now(timezone.utc) - newest).total_seconds())


class KafkaMonitor:
    """Message count and rate of the telemetry topic, from its partition end offsets."""

    def __init__(self):
        self.consumer = None
        self.ok = False
        self.total = 0
        self.events_per_sec = 0.0
        self._last: tuple[float, int] | None = None

    def refresh(self) -> None:
        try:
            from confluent_kafka import KafkaException, TopicPartition

            if self.consumer is None:
                from confluent_kafka import Consumer

                self.consumer = Consumer({
                    "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                    "group.id": "ncap-api-monitor",
                    "enable.auto.commit": False,
                })
            topic = self.consumer.list_topics(config.KAFKA_TOPIC, timeout=5).topics[config.KAFKA_TOPIC]
            if topic.error is not None:
                raise KafkaException(topic.error)
            total = sum(
                self.consumer.get_watermark_offsets(TopicPartition(config.KAFKA_TOPIC, p), timeout=5)[1]
                for p in topic.partitions
            )
        except Exception:
            self.ok, self.events_per_sec, self._last = False, 0.0, None
            return
        now = time.monotonic()
        if self._last is not None and now > self._last[0]:
            self.events_per_sec = max(0.0, (total - self._last[1]) / (now - self._last[0]))
        self._last = (now, total)
        self.ok, self.total = True, total


class SparkMonitor:
    """Liveness and completed tasks of the streaming job, from the Spark UI REST API."""

    def __init__(self):
        self.ok = False
        self.tasks_completed = 0

    def refresh(self) -> None:
        base = f"{config.SPARK_UI_URL.rstrip('/')}/api/v1/applications"
        try:
            with urlopen(base, timeout=3) as resp:
                app_id = json.load(resp)[0]["id"]
            with urlopen(f"{base}/{app_id}/executors", timeout=3) as resp:
                executors = json.load(resp)
        except Exception:
            self.ok = False
            return
        self.ok = True
        self.tasks_completed = sum(e.get("completedTasks", 0) for e in executors)


store = ViolationStore()
kafka = KafkaMonitor()
spark = SparkMonitor()
subscribers: set[asyncio.Queue] = set()


def broadcast(record: dict) -> None:
    for queue in list(subscribers):
        try:
            queue.put_nowait(record)
        except asyncio.QueueFull:  # a stalled client misses messages rather than blocking others
            pass


async def poll_violations() -> None:
    while True:
        for record in await store.refresh():
            broadcast(record)
        await asyncio.sleep(config.API_POLL_SECONDS)


async def poll_health() -> None:
    while True:
        await asyncio.gather(asyncio.to_thread(kafka.refresh), asyncio.to_thread(spark.refresh))
        await asyncio.sleep(HEALTH_POLL_SECONDS)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    tasks = [asyncio.create_task(poll_violations()), asyncio.create_task(poll_health())]
    yield
    for task in tasks:
        task.cancel()


app = FastAPI(title="NCAP Live Violations", lifespan=lifespan)
# Lets the dashboard also work when opened straight from disk (file://).
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"])


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(config.WEB_DIR / "index.html")


@app.get("/api/cameras")
def cameras():
    return [
        {"id": c["camera_id"], "location": c["location_name"], "road": c["road_name"],
         "type": c["camera_type"], "limit": c["speed_limit_kph"],
         "lat": c["latitude"], "lon": c["longitude"]}
        for c in load_cameras(config.CAMERAS_FILE)
    ]


# Endpoints that read store.records are async so they run on the event loop,
# where apply() changes it, rather than in FastAPI's thread pool.
@app.get("/api/violations")
async def violations(limit: int = Query(300, ge=1, le=2000)):
    return newest_first(store.records.values())[:limit]


@app.get("/api/plate/{plate}")
async def plate(plate: str, limit: int = Query(100, ge=1, le=1000)):
    try:
        history = await asyncio.to_thread(store.plate_history, normalize_plate(plate), limit)
    except Exception:
        raise HTTPException(503, "Cassandra is not reachable")
    return history


@app.get("/api/stats")
async def stats():
    return compute_stats(list(store.records.values()), datetime.now(timezone.utc))


@app.get("/api/summary")
async def summary():
    return compute_summary(list(store.records.values()), store.day or datetime.now(MANILA_TZ).date())


@app.get("/api/health")
async def health():
    return {
        "kafka": {"ok": kafka.ok, "events_per_sec": round(kafka.events_per_sec, 2),
                  "total": kafka.total},
        "spark": {"ok": spark.ok, "tasks_completed": spark.tasks_completed,
                  "latest_violation_age": store.latest_detection_age()},
        "cassandra": {"ok": store.ok, "latency_ms": round(store.latency_ms, 1),
                      "violations_today": len(store.records)},
    }


@app.get("/api/stream")
async def stream(request: Request):
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    subscribers.add(queue)

    async def events():
        try:
            yield "retry: 3000\n\n"
            while not await request.is_disconnected():
                try:
                    record = await asyncio.wait_for(queue.get(), timeout=KEEPALIVE_SECONDS)
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                yield f"data: {json.dumps(record)}\n\n"
        finally:
            subscribers.discard(queue)

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
