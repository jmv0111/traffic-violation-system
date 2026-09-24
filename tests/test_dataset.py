import csv
from datetime import timedelta

import pytest

from ncap import config
from ncap.dataset.generate_placeholder import generate_dataset
from ncap.dataset.loader import COLUMN_MAPPING, load_cameras, load_events
from ncap.producer import to_message
from ncap.schemas import CAMERA_TYPES, TELEMETRY_FIELDS


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    path = tmp_path_factory.mktemp("data") / "events.csv"
    generated = generate_dataset(path, minutes=10, seed=7)
    return path, generated


def test_camera_registry_is_valid():
    cameras = load_cameras(config.CAMERAS_FILE)
    assert len({c["camera_id"] for c in cameras}) == len(cameras)
    assert {c["camera_type"] for c in cameras} == set(CAMERA_TYPES)
    for c in cameras:
        assert 14.3 < c["latitude"] < 14.8 and 120.9 < c["longitude"] < 121.2  # Metro Manila


def test_placeholder_plants_every_violation(dataset):
    _, generated = dataset
    scenarios = {e["simulated_scenario"] for e in generated}
    assert {"NORMAL", "SPEEDING", "RED_LIGHT", "ILLEGAL_PARKING"} <= scenarios


def test_planted_violations_match_detection_rules(dataset):
    _, generated = dataset
    cameras = {c["camera_id"]: c for c in load_cameras(config.CAMERAS_FILE)}
    for e in generated:
        camera = cameras[e["camera_id"]]
        if e["simulated_scenario"] == "RED_LIGHT":
            assert e["traffic_light_state"] == "RED" and e["stop_line_crossed"]
            assert e["speed_kph"] >= config.RED_LIGHT_MIN_SPEED_KPH
        if e["simulated_scenario"] == "ILLEGAL_PARKING":
            assert camera["camera_type"] == "NO_PARKING"
            assert e["speed_kph"] <= config.PARKING_MAX_SPEED_KPH


def test_loader_returns_sorted_typed_events(dataset):
    path, generated = dataset
    events = load_events(path)
    assert len(events) == len(generated)
    assert all(a["event_time"] <= b["event_time"] for a, b in zip(events, events[1:]))
    intersection = next(e for e in events if e["traffic_light_state"])
    assert isinstance(intersection["stop_line_crossed"], bool)
    assert isinstance(events[0]["speed_kph"], float)


def test_loader_supports_renamed_columns(tmp_path):
    path = tmp_path / "real.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["cam", "plate", "lat", "lon", "kph", "ts"])
        writer.writerow(["CAM-EDSA-GUA", "abc 1234", "14.56", "121.04", "72.5", "2026-09-01 07:00:00"])
    mapping = {**COLUMN_MAPPING, "camera_id": "cam", "plate_number": "plate", "latitude": "lat",
               "longitude": "lon", "speed_kph": "kph", "event_time": "ts"}
    [event] = load_events(path, mapping)
    assert event["plate_number"] == "ABC 1234"
    assert event["event_time"].utcoffset() == timedelta(hours=8)
    assert event["traffic_light_state"] is None


def test_loader_rejects_missing_required_columns(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("camera_id,plate_number\nCAM-1,ABC 1234\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing required columns"):
        load_events(path)


def test_message_contains_only_telemetry(dataset):
    path, _ = dataset
    event = load_events(path)[0]
    message = to_message(event, timedelta(hours=1))
    assert list(message) == TELEMETRY_FIELDS
    assert message["event_time"] == (event["event_time"] + timedelta(hours=1)).isoformat(timespec="milliseconds")
