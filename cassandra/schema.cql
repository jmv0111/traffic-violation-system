-- Persistence layer: finalized NCAP violation records for the ticketing workflow.
--
-- Cassandra tables are designed around the queries they serve, so the same
-- violation is written (denormalized) to one table per access pattern:
--   violations_by_plate   - all violations of a vehicle, newest first (ticketing / notices)
--   violations_by_camera  - violations at one camera on one day (location reports)
--   violations_by_type    - violations of one type on one day (enforcement reports)
--   camera_speed_stats    - sliding-window traffic statistics per camera
--
-- Replication: a single-node dev cluster uses SimpleStrategy with RF 1. For high
-- availability in production, use at least 3 nodes, e.g.
--   {'class': 'NetworkTopologyStrategy', 'datacenter1': 3}
-- and keep writes at LOCAL_QUORUM (CASSANDRA_WRITE_CONSISTENCY), so any one node
-- can fail without losing violation records or blocking writes.

CREATE KEYSPACE IF NOT EXISTS ncap
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS ncap.violations_by_plate (
  plate_number        text,
  violation_time      timestamp,
  violation_id        text,
  violation_type      text,
  violation_date      date,
  vehicle_type        text,
  camera_id           text,
  location_name       text,
  road_name           text,
  latitude            double,
  longitude           double,
  window_start        timestamp,
  window_end          timestamp,
  recorded_speed_kph  double,
  speed_limit_kph     int,
  reading_count       int,
  evidence            text,
  ticket_status       text,
  detected_at         timestamp,
  PRIMARY KEY ((plate_number), violation_time, violation_id)
) WITH CLUSTERING ORDER BY (violation_time DESC, violation_id ASC);

CREATE TABLE IF NOT EXISTS ncap.violations_by_camera (
  camera_id           text,
  violation_date      date,
  violation_time      timestamp,
  violation_id        text,
  violation_type      text,
  plate_number        text,
  vehicle_type        text,
  location_name       text,
  road_name           text,
  latitude            double,
  longitude           double,
  window_start        timestamp,
  window_end          timestamp,
  recorded_speed_kph  double,
  speed_limit_kph     int,
  reading_count       int,
  evidence            text,
  ticket_status       text,
  detected_at         timestamp,
  PRIMARY KEY ((camera_id, violation_date), violation_time, violation_id)
) WITH CLUSTERING ORDER BY (violation_time DESC, violation_id ASC);

CREATE TABLE IF NOT EXISTS ncap.violations_by_type (
  violation_type      text,
  violation_date      date,
  violation_time      timestamp,
  violation_id        text,
  plate_number        text,
  vehicle_type        text,
  camera_id           text,
  location_name       text,
  road_name           text,
  latitude            double,
  longitude           double,
  window_start        timestamp,
  window_end          timestamp,
  recorded_speed_kph  double,
  speed_limit_kph     int,
  reading_count       int,
  evidence            text,
  ticket_status       text,
  detected_at         timestamp,
  PRIMARY KEY ((violation_type, violation_date), violation_time, violation_id)
) WITH CLUSTERING ORDER BY (violation_time DESC, violation_id ASC);

CREATE TABLE IF NOT EXISTS ncap.camera_speed_stats (
  camera_id        text,
  stat_date        date,
  window_start     timestamp,
  window_end       timestamp,
  avg_speed_kph    double,
  max_speed_kph    double,
  reading_count    int,
  vehicle_count    int,
  pct_over_limit   double,
  speed_limit_kph  int,
  PRIMARY KEY ((camera_id, stat_date), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
