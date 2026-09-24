# Real-time NCAP Analytics: High-Velocity Traffic Violation Detection System via Kafka, Spark, and Cassandra

CSS182-02 / AM3, Mapúa University, School of Information Technology.

This project models a real-time streaming pipeline for the No Contact Apprehension
Policy (NCAP). It simulates traffic cameras on Metro Manila roads (EDSA,
Commonwealth, Quezon Ave, Roxas Blvd, España, Aurora), detects violations as the
data streams in, and stores finalized violation records for ticketing.

```
 traffic cameras            INGESTION              TRANSFORMATION                        PERSISTENCE
 (dataset replay)  ──►  Apache Kafka topic  ──►  Spark Structured Streaming  ──►  Apache Cassandra
 ncap/producer.py       ncap-traffic-telemetry   ncap/stream_processor.py         cassandra/schema.cql
                                                   ├ SPEEDING (sliding window avg)   violations_by_plate
                                                   ├ BEATING_RED_LIGHT               violations_by_camera
                                                   ├ ILLEGAL_PARKING (session window) violations_by_type
                                                   └ camera speed stats (sliding)    camera_speed_stats
```

## How the code maps to the proposal's objectives

| Objective | Where |
|---|---|
| Design a system scope and functional architecture that integrates Kafka, Spark, and Cassandra | `docker-compose.yml`, `ncap/config.py` |
| Simulate high-velocity data ingestion of Metro Manila traffic telemetry | `ncap/dataset/generate_placeholder.py`, `ncap/producer.py` |
| Windowed transformations in Spark to filter and identify violations | `ncap/stream_processor.py` |
| A persistent, highly available Cassandra data model to store and retrieve violations | `cassandra/schema.cql`, `ncap/query_violations.py` |

## ⚠ The dataset is a placeholder

We have not picked the real dataset yet. Until then, `ncap/dataset/generate_placeholder.py`
generates synthetic telemetry into `data/placeholder/traffic_events.csv`, and plants
each violation type so the pipeline has something to find. The producer generates
the file automatically if it's missing.

Each telemetry record (see `ncap/schemas.py`) has:
`event_id, camera_id, plate_number, vehicle_type, latitude, longitude, speed_kph,
traffic_light_state, stop_line_crossed, event_time`.

The placeholder CSV also includes a `simulated_scenario` column (`NORMAL`,
`SPEEDING`, `RED_LIGHT`, `ILLEGAL_PARKING`, `SHORT_STOP`) that shows what was planted.
It is **not** sent to Kafka. Use it to check detections against expected results.

`data/cameras.csv` is the camera registry: location, road, speed limit, and camera
type (`SPEED`, `INTERSECTION`, `NO_PARKING`). Coordinates are approximate.

### Switching to the real dataset

1. Put the file in `data/` and set `DATASET_FILE` (env var) or change the default in `ncap/config.py`.
2. Edit `COLUMN_MAPPING` in `ncap/dataset/loader.py` so each telemetry field names the
   matching column in the real file. Only `camera_id, plate_number, latitude,
   longitude, speed_kph, event_time` are required.
3. Update `data/cameras.csv` so it lists the cameras that appear in the dataset.
   Events from cameras that aren't in the registry are dropped.

## Running it (Docker, recommended)

Requires Docker Desktop. It's the easiest option on Windows, because Spark
checkpoints don't work on Windows without extra Hadoop binaries.

```bash
docker compose up --build
```

This starts Kafka (KRaft mode, no ZooKeeper), creates the topic, starts Cassandra,
loads the schema, starts the Spark job, and starts the producer replaying the
dataset in a loop. The first start takes a few minutes: Cassandra has to boot, and
Spark downloads the Kafka/Cassandra connector jars.

Violations appear after about a window length plus the watermark (≈1.5 to 2
minutes), because Spark only writes a window once it's final. Look for lines like
`[speeding_violations] batch 7: wrote 3 rows ...` in the `ncap-spark` logs. The
Spark UI is at http://localhost:4040.

### Querying violations

```bash
docker compose run --rm producer python -m ncap.query_violations type SPEEDING
docker compose run --rm producer python -m ncap.query_violations type BEATING_RED_LIGHT
docker compose run --rm producer python -m ncap.query_violations type ILLEGAL_PARKING
docker compose run --rm producer python -m ncap.query_violations camera CAM-EDSA-SHAW
docker compose run --rm producer python -m ncap.query_violations plate "ABC 1234"
docker compose run --rm producer python -m ncap.query_violations stats CAM-EDSA-GUA
```

`--date YYYY-MM-DD` (Manila time) defaults to today. You can also use cqlsh:
`docker exec -it ncap-cassandra cqlsh -e "SELECT * FROM ncap.violations_by_type WHERE violation_type='SPEEDING' AND violation_date='2026-09-24';"`

### High-velocity simulation

Change the producer `command` in `docker-compose.yml`, or run it separately:

```bash
# replay 20x faster than real time
docker compose run --rm producer python -m ncap.producer --loop --speedup 20
# send as fast as the broker accepts (stress test)
docker compose run --rm producer python -m ncap.producer --loop --no-pacing
# a denser dataset: 120 vehicles per minute per camera over 60 simulated minutes
docker compose run --rm producer python -m ncap.dataset.generate_placeholder --minutes 60 --vehicles-per-minute 120
```

The producer reports throughput (`events/s`) every 5 seconds.

### Reset everything

```bash
docker compose down -v   # also deletes Cassandra data and Spark checkpoints
```

## Detection rules

All rules use **event time** with a 30-second watermark, so late readings still count.
Thresholds live in `ncap/config.py`, and each one can be overridden with an env var.

| Violation | Camera type | Rule |
|---|---|---|
| `SPEEDING` | `SPEED` | Average speed of a vehicle over a **1-minute window sliding every 20 s** is above the camera's speed limit (+ `SPEED_TOLERANCE_KPH`), with at least 2 readings |
| `BEATING_RED_LIGHT` | `INTERSECTION` | Stop line crossed while the light is `RED` at ≥ 5 kph |
| `ILLEGAL_PARKING` | `NO_PARKING` | Vehicle stationary (≤ 3 kph) for ≥ 180 s. A **session window** (2-minute gap) turns one continuous stop into one violation |
| camera stats | all | Per-camera average/max speed, vehicle count, and share of readings over the limit, on a 1-minute window sliding every 30 s |

Every violation is stored with `ticket_status = 'PENDING_REVIEW'` for the ticketing workflow.

**Overlapping windows:** sliding windows overlap, so the same pass can fall into
more than one window. Each violation ID is a hash of type, plate, camera, and first
reading time, so windows that contain the whole pass write the *same* row (Cassandra
upsert). If a window boundary cuts a pass, a second record can still appear. The
review step before a notice is issued should resolve those.

## Cassandra data model

Cassandra tables are built around their queries, so each violation is written to
three tables (`cassandra/schema.cql`):

| Table | Partition key | Answers |
|---|---|---|
| `violations_by_plate` | `plate_number` | "All violations of this vehicle", newest first (for notices) |
| `violations_by_camera` | `camera_id, violation_date` | "What happened at this camera today" |
| `violations_by_type` | `violation_type, violation_date` | "All red-light violations today" |
| `camera_speed_stats` | `camera_id, stat_date` | Traffic speed trends per camera |

Date-bucketed partitions stay bounded in size. The dev cluster runs a single node
with replication factor 1. For **high availability**, run 3+ nodes, change the keyspace to
`NetworkTopologyStrategy` with RF 3, and keep writes at `LOCAL_QUORUM` (already the
default), so one node can go down without losing records or blocking writes.

## Running without Docker (optional)

Requires Python 3.10 or 3.11, Java 17 (Spark 3.5 doesn't support Java 21 well),
and Kafka and Cassandra running (e.g. `docker compose up kafka kafka-init cassandra cassandra-init`).

```bash
pip install -r requirements-spark.txt
python -m ncap.dataset.generate_placeholder
python -m ncap.stream_processor --debug      # terminal 1
python -m ncap.producer --loop --speedup 5   # terminal 2
python -m ncap.query_violations type SPEEDING
pytest                                       # dataset, loader, and producer tests
```

## Project layout

```
cassandra/schema.cql                 Cassandra keyspace and tables
data/cameras.csv                     camera registry (reference data)
data/placeholder/traffic_events.csv  PLACEHOLDER telemetry (generated)
ncap/config.py                       all settings (env-var overridable)
ncap/schemas.py                      telemetry / camera record layouts
ncap/dataset/loader.py               CSV loading + column mapping for the real dataset
ncap/dataset/generate_placeholder.py placeholder dataset generator
ncap/producer.py                     Kafka producer (ingestion)
ncap/stream_processor.py             Spark Structured Streaming job (transformation)
ncap/query_violations.py             Cassandra query CLI (retrieval)
tests/                               unit tests
```
