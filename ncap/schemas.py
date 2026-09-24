"""Shared record layouts for camera telemetry and the camera registry."""
from __future__ import annotations

# Fields each traffic camera emits per vehicle reading (the message sent to Kafka).
TELEMETRY_FIELDS = [
    "event_id",
    "camera_id",
    "plate_number",
    "vehicle_type",
    "latitude",
    "longitude",
    "speed_kph",
    "traffic_light_state",  # RED / YELLOW / GREEN, intersection cameras only
    "stop_line_crossed",  # intersection cameras only
    "event_time",  # ISO-8601 with UTC offset, e.g. 2026-09-01T07:00:00.000+08:00
]

CAMERA_FIELDS = [
    "camera_id",
    "location_name",
    "road_name",
    "city",
    "latitude",
    "longitude",
    "speed_limit_kph",
    "camera_type",
]

# What each camera enforces.
CAMERA_TYPES = ("SPEED", "INTERSECTION", "NO_PARKING")
LIGHT_STATES = ("RED", "YELLOW", "GREEN")


def telemetry_spark_schema():
    from pyspark.sql.types import (BooleanType, DoubleType, StringType, StructField,
                                   StructType)

    return StructType([
        StructField("event_id", StringType()),
        StructField("camera_id", StringType()),
        StructField("plate_number", StringType()),
        StructField("vehicle_type", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("speed_kph", DoubleType()),
        StructField("traffic_light_state", StringType()),
        StructField("stop_line_crossed", BooleanType()),
        StructField("event_time", StringType()),
    ])


def camera_spark_schema():
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    return StructType([
        StructField("camera_id", StringType(), False),
        StructField("location_name", StringType()),
        StructField("road_name", StringType()),
        StructField("camera_type", StringType()),
        StructField("speed_limit_kph", IntegerType()),
    ])
