"""Central configuration for the NCAP traffic violation pipeline.

Every setting can be overridden with an environment variable of the same name,
so the same code runs on a laptop (``localhost``) and inside Docker Compose
(service hostnames such as ``kafka`` and ``cassandra``).
"""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"


def _str(name: str, default: str) -> str:
    return os.getenv(name, default)


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


TIMEZONE = _str("TIMEZONE", "Asia/Manila")

# --- Datasets -----------------------------------------------------------------
# Camera registry: static reference data (location, road, speed limit, type).
CAMERAS_FILE = Path(_str("CAMERAS_FILE", str(DATA_DIR / "cameras.csv")))
# Telemetry dataset replayed into Kafka. PLACEHOLDER until the real dataset is chosen.
DATASET_FILE = Path(_str("DATASET_FILE", str(DATA_DIR / "placeholder" / "traffic_events.csv")))

# --- Ingestion: Apache Kafka --------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = _str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = _str("KAFKA_TOPIC", "ncap-traffic-telemetry")
KAFKA_STARTING_OFFSETS = _str("KAFKA_STARTING_OFFSETS", "earliest")

# --- Transformation: Spark Structured Streaming -------------------------------
SPARK_PACKAGES = _str(
    "SPARK_PACKAGES",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1",
)
SPARK_CHECKPOINT_DIR = _str("SPARK_CHECKPOINT_DIR", str(PROJECT_ROOT / "checkpoints"))
SPARK_SHUFFLE_PARTITIONS = _str("SPARK_SHUFFLE_PARTITIONS", "4")
TRIGGER_INTERVAL = _str("TRIGGER_INTERVAL", "10 seconds")
# How late an event may arrive and still be counted in its window.
WATERMARK_DELAY = _str("WATERMARK_DELAY", "30 seconds")

# Speeding: average speed per vehicle over a sliding window.
SPEED_WINDOW_DURATION = _str("SPEED_WINDOW_DURATION", "1 minute")
SPEED_WINDOW_SLIDE = _str("SPEED_WINDOW_SLIDE", "20 seconds")
SPEED_TOLERANCE_KPH = _float("SPEED_TOLERANCE_KPH", 0.0)
MIN_SPEED_READINGS = _int("MIN_SPEED_READINGS", 2)

# Beating the red light: minimum speed to count as actually crossing on red.
RED_LIGHT_MIN_SPEED_KPH = _float("RED_LIGHT_MIN_SPEED_KPH", 5.0)

# Illegal parking: stationary in a no-parking zone longer than the grace period.
PARKING_MAX_SPEED_KPH = _float("PARKING_MAX_SPEED_KPH", 3.0)
PARKING_SESSION_GAP = _str("PARKING_SESSION_GAP", "2 minutes")
PARKING_GRACE_SECONDS = _int("PARKING_GRACE_SECONDS", 180)

# Per-camera traffic statistics (sliding window).
STATS_WINDOW_DURATION = _str("STATS_WINDOW_DURATION", "1 minute")
STATS_WINDOW_SLIDE = _str("STATS_WINDOW_SLIDE", "30 seconds")

# --- Persistence: Apache Cassandra --------------------------------------------
CASSANDRA_HOST = _str("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = _int("CASSANDRA_PORT", 9042)
CASSANDRA_KEYSPACE = _str("CASSANDRA_KEYSPACE", "ncap")
CASSANDRA_WRITE_CONSISTENCY = _str("CASSANDRA_WRITE_CONSISTENCY", "LOCAL_QUORUM")
