"""Generate a PLACEHOLDER traffic telemetry dataset.

This stands in for the real NCAP camera dataset until one is chosen. It simulates
the cameras in ``data/cameras.csv`` (EDSA, Commonwealth, Quezon Ave, Roxas Blvd,
Espana, Aurora) and deliberately plants each violation the pipeline must detect:

* SPEED cameras        - several radar readings per vehicle pass; some vehicles speed.
* INTERSECTION cameras - a traffic light cycle; some vehicles cross on RED.
* NO_PARKING cameras   - passing traffic, short stops (legal), and long stops (illegal).

The ``simulated_scenario`` column records what was planted so detections can be
checked. It is not part of the telemetry and is never sent to Kafka.

    python -m ncap.dataset.generate_placeholder --minutes 15 --seed 42
"""
from __future__ import annotations

import argparse
import csv
import random
import string
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from ncap import config
from ncap.dataset.loader import MANILA_TZ, load_cameras
from ncap.schemas import TELEMETRY_FIELDS

DEFAULT_START = datetime(2026, 9, 1, 7, 0, 0, tzinfo=MANILA_TZ)  # weekday morning rush

VEHICLE_TYPES = ["CAR", "SUV", "MOTORCYCLE", "JEEPNEY", "BUS", "TRUCK"]
VEHICLE_WEIGHTS = [0.55, 0.15, 0.15, 0.05, 0.04, 0.06]

# Traffic light cycle in seconds.
GREEN_SECONDS, YELLOW_SECONDS, RED_SECONDS = 60, 5, 55
CYCLE_SECONDS = GREEN_SECONDS + YELLOW_SECONDS + RED_SECONDS

PARKING_READING_INTERVAL = 30  # a no-parking camera re-reads a stationary vehicle every 30 s

OUTPUT_FIELDS = TELEMETRY_FIELDS + ["simulated_scenario"]


class PlaceholderGenerator:
    def __init__(self, cameras: list[dict], start: datetime, minutes: int, seed: int,
                 vehicles_per_minute: float, speeding_rate: float, red_light_rate: float,
                 parking_incidents_per_hour: float):
        self.cameras = cameras
        self.start = start
        self.duration = minutes * 60
        self.rng = random.Random(seed)
        self.vehicles_per_minute = vehicles_per_minute
        self.speeding_rate = speeding_rate
        self.red_light_rate = red_light_rate
        self.parking_incidents_per_hour = parking_incidents_per_hour
        self.used_plates: set[str] = set()

    # --- helpers ---------------------------------------------------------------
    def _plate(self) -> str:
        while True:
            letters = "".join(self.rng.choice(string.ascii_uppercase) for _ in range(3))
            plate = f"{letters} {self.rng.randint(1000, 9999)}"
            if plate not in self.used_plates:
                self.used_plates.add(plate)
                return plate

    def _vehicle_type(self) -> str:
        return self.rng.choices(VEHICLE_TYPES, VEHICLE_WEIGHTS)[0]

    def _arrivals(self):
        """Vehicle arrival offsets (seconds) as a Poisson process."""
        rate = self.vehicles_per_minute / 60.0
        t = self.rng.expovariate(rate)
        while t < self.duration:
            yield t
            t += self.rng.expovariate(rate)

    def _event(self, camera: dict, plate: str, vehicle_type: str, offset: float, speed: float,
               scenario: str, light: str | None = None, crossed: bool | None = None) -> dict:
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "camera_id": camera["camera_id"],
            "plate_number": plate,
            "vehicle_type": vehicle_type,
            "latitude": round(camera["latitude"] + self.rng.uniform(-0.0002, 0.0002), 6),
            "longitude": round(camera["longitude"] + self.rng.uniform(-0.0002, 0.0002), 6),
            "speed_kph": round(max(0.0, speed), 1),
            "traffic_light_state": light,
            "stop_line_crossed": crossed,
            "event_time": (self.start + timedelta(seconds=offset)).isoformat(timespec="milliseconds"),
            "simulated_scenario": scenario,
        }

    def _light_state(self, camera_index: int, offset: float) -> str:
        phase = (offset + camera_index * 17) % CYCLE_SECONDS
        if phase < GREEN_SECONDS:
            return "GREEN"
        if phase < GREEN_SECONDS + YELLOW_SECONDS:
            return "YELLOW"
        return "RED"

    # --- camera behaviours -----------------------------------------------------
    def _speed_camera(self, camera: dict) -> list[dict]:
        limit = camera["speed_limit_kph"]
        events = []
        for t in self._arrivals():
            plate, vtype = self._plate(), self._vehicle_type()
            speeding = self.rng.random() < self.speeding_rate
            if speeding:
                true_speed = limit + self.rng.uniform(12, 45)
            else:
                true_speed = self.rng.uniform(max(10, limit - 35), limit - 3)
            scenario = "SPEEDING" if speeding else "NORMAL"
            for i in range(self.rng.randint(3, 5)):  # radar readings during one pass
                speed = true_speed + self.rng.gauss(0, 1.5)
                events.append(self._event(camera, plate, vtype, t + i * 0.8, speed, scenario))
        return events

    def _intersection_camera(self, camera: dict, index: int) -> list[dict]:
        limit = camera["speed_limit_kph"]
        events = []
        for t in self._arrivals():
            plate, vtype = self._plate(), self._vehicle_type()
            light = self._light_state(index, t)
            if light == "RED":
                if self.rng.random() < self.red_light_rate:
                    events.append(self._event(camera, plate, vtype, t, self.rng.uniform(25, 55),
                                              "RED_LIGHT", light, True))
                else:  # stopped behind the line
                    events.append(self._event(camera, plate, vtype, t, self.rng.uniform(0, 3),
                                              "NORMAL", light, False))
            elif light == "YELLOW":
                events.append(self._event(camera, plate, vtype, t, self.rng.uniform(20, 45),
                                          "NORMAL", light, True))
            else:
                events.append(self._event(camera, plate, vtype, t, self.rng.uniform(15, limit - 5),
                                          "NORMAL", light, True))
        return events

    def _no_parking_camera(self, camera: dict) -> list[dict]:
        limit = camera["speed_limit_kph"]
        events = []
        for t in self._arrivals():  # passing traffic
            plate, vtype = self._plate(), self._vehicle_type()
            speed = self.rng.uniform(10, limit - 5)
            for i in range(self.rng.randint(1, 2)):
                events.append(self._event(camera, plate, vtype, t + i, speed, "NORMAL"))

        incidents = max(2, round(self.parking_incidents_per_hour * self.duration / 3600))
        for i in range(incidents):
            plate, vtype = self._plate(), self._vehicle_type()
            illegal = i == 0 or self.rng.random() < 0.6  # always plant at least one
            dwell = self.rng.uniform(240, 720) if illegal else self.rng.uniform(30, 150)
            begin = self.rng.uniform(0, max(1.0, self.duration - dwell))
            scenario = "ILLEGAL_PARKING" if illegal else "SHORT_STOP"
            elapsed = 0.0
            while elapsed <= dwell:
                events.append(self._event(camera, plate, vtype, begin + elapsed,
                                          self.rng.uniform(0, 1), scenario))
                elapsed += PARKING_READING_INTERVAL
        return events

    def generate(self) -> list[dict]:
        events = []
        for index, camera in enumerate(self.cameras):
            if camera["camera_type"] == "SPEED":
                events += self._speed_camera(camera)
            elif camera["camera_type"] == "INTERSECTION":
                events += self._intersection_camera(camera, index)
            elif camera["camera_type"] == "NO_PARKING":
                events += self._no_parking_camera(camera)
            else:
                raise ValueError(f"Unknown camera_type {camera['camera_type']!r} for {camera['camera_id']}")
        events.sort(key=lambda e: e["event_time"])
        return events


def write_csv(events: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for event in events:
            writer.writerow({k: ("" if v is None else v) for k, v in event.items()})


def generate_dataset(output: Path = config.DATASET_FILE, cameras_file: Path = config.CAMERAS_FILE,
                     minutes: int = 15, seed: int = 42, vehicles_per_minute: float = 30,
                     speeding_rate: float = 0.08, red_light_rate: float = 0.10,
                     parking_incidents_per_hour: float = 12) -> list[dict]:
    generator = PlaceholderGenerator(load_cameras(cameras_file), DEFAULT_START, minutes, seed,
                                     vehicles_per_minute, speeding_rate, red_light_rate,
                                     parking_incidents_per_hour)
    events = generator.generate()
    write_csv(events, Path(output))
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the PLACEHOLDER traffic telemetry dataset.")
    parser.add_argument("--output", type=Path, default=config.DATASET_FILE)
    parser.add_argument("--cameras", type=Path, default=config.CAMERAS_FILE)
    parser.add_argument("--minutes", type=int, default=15, help="simulated time span")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--vehicles-per-minute", type=float, default=30, help="per camera")
    parser.add_argument("--speeding-rate", type=float, default=0.08)
    parser.add_argument("--red-light-rate", type=float, default=0.10,
                        help="share of vehicles arriving on RED that run it")
    parser.add_argument("--parking-incidents-per-hour", type=float, default=12, help="per camera")
    args = parser.parse_args()

    events = generate_dataset(args.output, args.cameras, args.minutes, args.seed,
                              args.vehicles_per_minute, args.speeding_rate, args.red_light_rate,
                              args.parking_incidents_per_hour)
    scenarios: dict[str, set] = {}
    for e in events:
        scenarios.setdefault(e["simulated_scenario"], set()).add((e["plate_number"], e["camera_id"]))
    print(f"Wrote {len(events)} events to {args.output}")
    for name, vehicles in sorted(scenarios.items()):
        print(f"  {name:<16} {len(vehicles):>5} vehicles")


if __name__ == "__main__":
    main()
