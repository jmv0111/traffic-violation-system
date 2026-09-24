"""Retrieve stored violation records from Cassandra.

    python -m ncap.query_violations plate "ABC 1234"
    python -m ncap.query_violations camera CAM-EDSA-SHAW --date 2026-09-24
    python -m ncap.query_violations type SPEEDING --date 2026-09-24
    python -m ncap.query_violations stats CAM-EDSA-GUA --date 2026-09-24
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone

from ncap import config
from ncap.dataset.loader import MANILA_TZ

VIOLATION_COLUMNS = ("violation_time, violation_type, plate_number, camera_id, location_name, "
                     "recorded_speed_kph, speed_limit_kph, ticket_status, evidence")
STATS_COLUMNS = ("window_start, window_end, avg_speed_kph, max_speed_kph, vehicle_count, "
                 "reading_count, pct_over_limit, speed_limit_kph")

QUERIES = {
    "plate": f"SELECT {VIOLATION_COLUMNS} FROM violations_by_plate "
             "WHERE plate_number = %s LIMIT %s",
    "camera": f"SELECT {VIOLATION_COLUMNS} FROM violations_by_camera "
              "WHERE camera_id = %s AND violation_date = %s LIMIT %s",
    "type": f"SELECT {VIOLATION_COLUMNS} FROM violations_by_type "
            "WHERE violation_type = %s AND violation_date = %s LIMIT %s",
    "stats": f"SELECT {STATS_COLUMNS} FROM camera_speed_stats "
             "WHERE camera_id = %s AND stat_date = %s LIMIT %s",
}


def _format(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):  # the driver returns naive UTC timestamps
        return value.replace(tzinfo=timezone.utc).astimezone(MANILA_TZ).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def print_table(rows) -> None:
    rows = list(rows)
    if not rows:
        print("No records found.")
        return
    headers = list(rows[0]._fields)
    cells = [[_format(v) for v in row] for row in rows]
    widths = [max(len(h), *(len(r[i]) for r in cells)) for i, h in enumerate(headers)]
    print("  ".join(h.upper().ljust(w) for h, w in zip(headers, widths)))
    for r in cells:
        print("  ".join(c.ljust(w) for c, w in zip(r, widths)))
    print(f"\n{len(rows)} record(s)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Query NCAP violation records in Cassandra.")
    sub = parser.add_subparsers(dest="query", required=True)

    p = sub.add_parser("plate", help="all violations of one vehicle, newest first")
    p.add_argument("plate_number")
    p.add_argument("--limit", type=int, default=50)

    today = datetime.now(MANILA_TZ).date()
    for name, arg, help_text in [
        ("camera", "camera_id", "violations at one camera on one day"),
        ("type", "violation_type", "SPEEDING, BEATING_RED_LIGHT or ILLEGAL_PARKING on one day"),
        ("stats", "camera_id", "sliding-window speed statistics of one camera on one day"),
    ]:
        p = sub.add_parser(name, help=help_text)
        p.add_argument(arg)
        p.add_argument("--date", type=date.fromisoformat, default=today,
                       help="YYYY-MM-DD, Manila time (default: today)")
        p.add_argument("--limit", type=int, default=50)

    args = parser.parse_args()

    if args.query == "plate":
        params = (args.plate_number.strip().upper(), args.limit)
    elif args.query == "type":
        params = (args.violation_type.strip().upper(), args.date, args.limit)
    else:
        params = (args.camera_id.strip().upper(), args.date, args.limit)

    from cassandra.cluster import Cluster

    cluster = Cluster([config.CASSANDRA_HOST], port=config.CASSANDRA_PORT)
    try:
        session = cluster.connect(config.CASSANDRA_KEYSPACE)
        print_table(session.execute(QUERIES[args.query], params))
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
