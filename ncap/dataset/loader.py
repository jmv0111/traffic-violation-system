"""Load the camera registry and telemetry datasets from CSV.

The telemetry dataset is currently a PLACEHOLDER (see ``generate_placeholder.py``).
When the real dataset is chosen, point ``DATASET_FILE`` at it and edit
``COLUMN_MAPPING`` so each telemetry field names the matching column in that file.
Nothing else in the pipeline needs to change.
"""
from __future__ import annotations

import csv
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

MANILA_TZ = timezone(timedelta(hours=8))

# telemetry field -> column name in the source dataset
COLUMN_MAPPING = {
    "event_id": "event_id",
    "camera_id": "camera_id",
    "plate_number": "plate_number",
    "vehicle_type": "vehicle_type",
    "latitude": "latitude",
    "longitude": "longitude",
    "speed_kph": "speed_kph",
    "traffic_light_state": "traffic_light_state",
    "stop_line_crossed": "stop_line_crossed",
    "event_time": "event_time",
}

REQUIRED_FIELDS = ("camera_id", "plate_number", "latitude", "longitude", "speed_kph", "event_time")

_TRUE = {"true", "1", "yes", "y", "t"}


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp; naive values are assumed to be Manila time."""
    ts = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=MANILA_TZ)


def _optional(row: dict, column: str | None) -> str | None:
    if not column:
        return None
    value = (row.get(column) or "").strip()
    return value or None


def normalize_event(row: dict, mapping: dict = COLUMN_MAPPING) -> dict:
    """Convert one raw CSV row into a typed telemetry event."""
    light = _optional(row, mapping.get("traffic_light_state"))
    crossed = _optional(row, mapping.get("stop_line_crossed"))
    return {
        "event_id": _optional(row, mapping.get("event_id")) or str(uuid.uuid4()),
        "camera_id": row[mapping["camera_id"]].strip(),
        "plate_number": row[mapping["plate_number"]].strip().upper(),
        "vehicle_type": _optional(row, mapping.get("vehicle_type")),
        "latitude": float(row[mapping["latitude"]]),
        "longitude": float(row[mapping["longitude"]]),
        "speed_kph": float(row[mapping["speed_kph"]]),
        "traffic_light_state": light.upper() if light else None,
        "stop_line_crossed": crossed.lower() in _TRUE if crossed else None,
        "event_time": parse_timestamp(row[mapping["event_time"]]),
    }


def load_events(path: str | Path, mapping: dict = COLUMN_MAPPING) -> list[dict]:
    """Read a telemetry CSV, drop duplicate event IDs, and sort by event time."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = [mapping[field] for field in REQUIRED_FIELDS
                   if mapping.get(field) not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"{path} is missing required columns: {', '.join(missing)}")

        events, seen = [], set()
        for row in reader:
            event = normalize_event(row, mapping)
            if event["event_id"] in seen:
                continue
            seen.add(event["event_id"])
            events.append(event)

    events.sort(key=lambda e: e["event_time"])
    return events


def load_cameras(path: str | Path) -> list[dict]:
    """Read the camera registry (static reference data)."""
    with open(path, newline="", encoding="utf-8") as f:
        return [
            {
                "camera_id": row["camera_id"].strip(),
                "location_name": row["location_name"].strip(),
                "road_name": row["road_name"].strip(),
                "city": row["city"].strip(),
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "speed_limit_kph": int(row["speed_limit_kph"]),
                "camera_type": row["camera_type"].strip().upper(),
            }
            for row in csv.DictReader(f)
        ]
