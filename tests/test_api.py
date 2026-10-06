from collections import namedtuple
from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from ncap import api

Row = namedtuple("Row", "violation_id violation_time violation_type plate_number vehicle_type camera_id "
                        "location_name recorded_speed_kph speed_limit_kph evidence ticket_status detected_at")

NOW = datetime(2026, 9, 24, 2, 0, tzinfo=timezone.utc)  # 10:00 Manila
DAY = date(2026, 9, 24)


def row(vid, minutes_ago, vtype="SPEEDING", plate="ABC 1234", camera="CAM-EDSA-GUA",
        location="EDSA - Guadalupe", speed=80.0, detected_minutes_ago=None):
    detected = minutes_ago if detected_minutes_ago is None else detected_minutes_ago
    naive = lambda m: (NOW - timedelta(minutes=m)).replace(tzinfo=None)  # driver returns naive UTC
    return Row(vid, naive(minutes_ago), vtype, plate, "CAR", camera, location, speed, 60,
               "evidence", "PENDING_REVIEW", naive(detected))


@pytest.fixture
def records():
    return [api.to_record(r) for r in [
        row("a", 30, speed=80.0, detected_minutes_ago=28),
        row("b", 3, vtype="BEATING_RED_LIGHT", camera="CAM-EDSA-SHAW",
            location="EDSA - Shaw Boulevard", speed=40.0),
        row("c", 1, plate="XYZ 9876", speed=90.0),
    ]]


def test_record_times_are_manila_iso():
    record = api.to_record(row("a", 0))
    assert record["time"] == "2026-09-24T10:00:00.000+08:00"
    assert datetime.fromisoformat(record["time"]) == NOW


def test_stats(records):
    stats = api.compute_stats(records, NOW)
    assert stats["total"] == 3
    assert stats["avg_speed"] == pytest.approx(70.0)
    assert stats["by_camera"] == {"CAM-EDSA-GUA": 2, "CAM-EDSA-SHAW": 1}
    assert stats["busiest"] == "EDSA - Guadalupe"
    assert stats["per_minute"] == 0.4  # 2 detected in the last 5 minutes


def test_summary(records):
    summary = api.compute_summary(records, DAY)
    assert summary["date"] == "2026-09-24"
    assert summary["first"] < summary["last"]
    assert summary["by_type"] == {"SPEEDING": 2, "BEATING_RED_LIGHT": 1}
    assert summary["top_plate"] == ["ABC 1234", 2]


def test_empty_day():
    assert api.compute_stats([], NOW)["busiest"] is None
    assert api.compute_summary([], DAY)["top_plate"] is None


def test_store_reports_only_new_records_after_first_load(records):
    store = api.ViolationStore()
    assert store.apply(DAY, records[:2], full=True) == []  # history, not news
    fresh = store.apply(DAY, records, full=False)
    assert [r["id"] for r in fresh] == ["c"]
    assert store.apply(DAY, records, full=False) == []  # re-reading the lookback is a no-op
    assert store.apply(DAY + timedelta(days=1), [], full=True) == [] and store.records == {}


def test_normalize_plate():
    assert api.normalize_plate("  abc   1234 ") == "ABC 1234"


@pytest.fixture
def client(records, monkeypatch):
    store = api.ViolationStore()
    store.apply(DAY, records, full=True)
    store.ok = True
    monkeypatch.setattr(api, "store", store)
    return TestClient(api.app)  # not a context manager: the Cassandra/Kafka pollers stay off


def test_violations_endpoint_is_newest_first(client):
    body = client.get("/api/violations?limit=2").json()
    assert [r["id"] for r in body] == ["c", "b"]


def test_endpoints_respond(client):
    assert client.get("/api/stats").json()["total"] == 3
    assert client.get("/api/summary").json()["total"] == 3
    health = client.get("/api/health").json()
    assert health["cassandra"] == {"ok": True, "latency_ms": 0.0, "violations_today": 3}
    assert {c["id"] for c in client.get("/api/cameras").json()} >= {"CAM-EDSA-GUA", "CAM-ESP-LACSON"}
    assert "NCAP Live Violations" in client.get("/").text


def test_plate_endpoint_normalizes_and_reports_outage(client, monkeypatch):
    seen = []
    monkeypatch.setattr(api.store, "plate_history", lambda plate, limit: seen.append(plate) or [])
    assert client.get("/api/plate/abc%20%201234").json() == []
    assert seen == ["ABC 1234"]

    def down(*_):
        raise ConnectionError
    monkeypatch.setattr(api.store, "plate_history", down)
    assert client.get("/api/plate/ABC 1234").status_code == 503
