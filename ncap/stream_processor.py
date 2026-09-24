"""Transformation: Spark Structured Streaming violation detection.

Reads camera telemetry from Kafka, enriches it with the camera registry, applies
windowed analytics on event time, and writes finalized violations to Cassandra.

    Kafka topic -> parse JSON -> watermark -> join camera registry -> four queries:

    * SPEEDING            average speed per vehicle over a sliding window exceeds the limit
    * BEATING_RED_LIGHT   vehicle crosses the stop line while the light is RED
    * ILLEGAL_PARKING     vehicle stays stationary in a no-parking zone past the grace period
    * camera_speed_stats  per-camera sliding-window average speed and volume

Windowed queries run in append mode, so a window is written only once the
watermark has passed it. Each violation is therefore final when it is stored.

Run with spark-submit (see docker-compose.yml) or ``python -m ncap.stream_processor``.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ncap import config
from ncap.dataset.loader import load_cameras
from ncap.schemas import camera_spark_schema, telemetry_spark_schema

TICKET_STATUS = "PENDING_REVIEW"

VIOLATION_TABLES = ["violations_by_plate", "violations_by_camera", "violations_by_type"]
STATS_TABLES = ["camera_speed_stats"]


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("ncap-violation-detector")
        .config("spark.jars.packages", config.SPARK_PACKAGES)
        .config("spark.sql.session.timeZone", config.TIMEZONE)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .config("spark.cassandra.connection.host", config.CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", str(config.CASSANDRA_PORT))
        .config("spark.cassandra.output.consistency.level", config.CASSANDRA_WRITE_CONSISTENCY)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_telemetry(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", config.KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    events = (
        raw.select(F.from_json(F.col("value").cast("string"), telemetry_spark_schema()).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
    )
    # Malformed JSON parses to nulls; drop those rows instead of failing the stream.
    valid = (
        F.col("camera_id").isNotNull()
        & F.col("plate_number").isNotNull()
        & F.col("event_time").isNotNull()
        & (F.col("speed_kph") >= 0)
    )
    return events.filter(valid).withWatermark("event_time", config.WATERMARK_DELAY)


def load_camera_registry(spark: SparkSession) -> DataFrame:
    rows = [
        (c["camera_id"], c["location_name"], c["road_name"], c["camera_type"], c["speed_limit_kph"])
        for c in load_cameras(config.CAMERAS_FILE)
    ]
    return spark.createDataFrame(rows, camera_spark_schema())


def _violation_record(df: DataFrame, violation_type: str, *, violation_time: Column,
                      window_start: Column, window_end: Column, recorded_speed: Column,
                      reading_count: Column, evidence: Column) -> DataFrame:
    """Shape any detection into the common violation record stored in Cassandra.

    ``violation_id`` is derived from the detection itself, so re-detecting the same
    offence (for example, a pass seen by overlapping sliding windows) overwrites
    the same row instead of creating a new one.
    """
    violation_id = F.sha2(
        F.concat_ws("|", F.lit(violation_type), F.col("plate_number"), F.col("camera_id"),
                    violation_time.cast("string")),
        256,
    ).substr(1, 32)
    return df.select(
        violation_id.alias("violation_id"),
        F.lit(violation_type).alias("violation_type"),
        F.col("plate_number"),
        F.col("vehicle_type"),
        F.col("camera_id"),
        F.col("location_name"),
        F.col("road_name"),
        F.col("latitude"),
        F.col("longitude"),
        violation_time.alias("violation_time"),
        F.to_date(violation_time).alias("violation_date"),
        window_start.alias("window_start"),
        window_end.alias("window_end"),
        recorded_speed.cast("double").alias("recorded_speed_kph"),
        F.col("speed_limit_kph").cast("int").alias("speed_limit_kph"),
        reading_count.cast("int").alias("reading_count"),
        evidence.alias("evidence"),
        F.lit(TICKET_STATUS).alias("ticket_status"),
        F.current_timestamp().alias("detected_at"),
    )


def _vehicle_aggregates() -> list[Column]:
    return [
        F.avg("speed_kph").alias("avg_speed_kph"),
        F.max("speed_kph").alias("max_speed_kph"),
        F.count("*").alias("readings"),
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
        F.first("vehicle_type", ignorenulls=True).alias("vehicle_type"),
        F.avg("latitude").alias("latitude"),
        F.avg("longitude").alias("longitude"),
    ]


def detect_speeding(enriched: DataFrame) -> DataFrame:
    """Average speed per vehicle over a sliding window above the camera's limit."""
    windows = (
        enriched.filter(F.col("camera_type") == "SPEED")
        .groupBy(
            F.window("event_time", config.SPEED_WINDOW_DURATION, config.SPEED_WINDOW_SLIDE),
            "plate_number", "camera_id", "location_name", "road_name", "speed_limit_kph",
        )
        .agg(*_vehicle_aggregates())
    )
    speeders = windows.filter(
        (F.col("avg_speed_kph") > F.col("speed_limit_kph") + F.lit(config.SPEED_TOLERANCE_KPH))
        & (F.col("readings") >= config.MIN_SPEED_READINGS)
    )
    return _violation_record(
        speeders, "SPEEDING",
        violation_time=F.col("first_seen"),
        window_start=F.col("window.start"),
        window_end=F.col("window.end"),
        recorded_speed=F.col("avg_speed_kph"),
        reading_count=F.col("readings"),
        evidence=F.format_string(
            "Average %.1f kph (max %.1f kph) over %d readings in a %s sliding window; limit %d kph",
            F.col("avg_speed_kph"), F.col("max_speed_kph"), F.col("readings"),
            F.lit(config.SPEED_WINDOW_DURATION), F.col("speed_limit_kph"),
        ),
    )


def detect_red_light(enriched: DataFrame) -> DataFrame:
    """Vehicle crossing the stop line while the intersection light is RED."""
    runners = enriched.filter(
        (F.col("camera_type") == "INTERSECTION")
        & (F.col("traffic_light_state") == "RED")
        & F.col("stop_line_crossed")
        & (F.col("speed_kph") >= config.RED_LIGHT_MIN_SPEED_KPH)
    )
    return _violation_record(
        runners, "BEATING_RED_LIGHT",
        violation_time=F.col("event_time"),
        window_start=F.col("event_time"),
        window_end=F.col("event_time"),
        recorded_speed=F.col("speed_kph"),
        reading_count=F.lit(1),
        evidence=F.format_string("Crossed the stop line on RED at %.1f kph", F.col("speed_kph")),
    )


def detect_illegal_parking(enriched: DataFrame) -> DataFrame:
    """Vehicle stationary in a no-parking zone for longer than the grace period.

    A session window groups consecutive stationary readings of one vehicle, so
    one continuous stop becomes exactly one violation, however long it lasts.
    """
    stops = (
        enriched.filter(
            (F.col("camera_type") == "NO_PARKING")
            & (F.col("speed_kph") <= config.PARKING_MAX_SPEED_KPH)
        )
        .groupBy(
            F.session_window("event_time", config.PARKING_SESSION_GAP),
            "plate_number", "camera_id", "location_name", "road_name", "speed_limit_kph",
        )
        .agg(*_vehicle_aggregates())
        .withColumn("stationary_seconds",
                    F.col("last_seen").cast("long") - F.col("first_seen").cast("long"))
    )
    parked = stops.filter(F.col("stationary_seconds") >= config.PARKING_GRACE_SECONDS)
    return _violation_record(
        parked, "ILLEGAL_PARKING",
        violation_time=F.col("first_seen"),
        window_start=F.col("first_seen"),
        window_end=F.col("last_seen"),
        recorded_speed=F.col("avg_speed_kph"),
        reading_count=F.col("readings"),
        evidence=F.format_string(
            "Stationary for %d s in a no-parking zone (%d readings); grace period %d s",
            F.col("stationary_seconds"), F.col("readings"), F.lit(config.PARKING_GRACE_SECONDS),
        ),
    )


def camera_speed_stats(enriched: DataFrame) -> DataFrame:
    """Sliding-window average speed, volume, and share of readings over the limit per camera."""
    return (
        enriched.groupBy(
            F.window("event_time", config.STATS_WINDOW_DURATION, config.STATS_WINDOW_SLIDE),
            "camera_id", "speed_limit_kph",
        )
        .agg(
            F.avg("speed_kph").alias("avg_speed_kph"),
            F.max("speed_kph").alias("max_speed_kph"),
            F.count("*").cast("int").alias("reading_count"),
            F.approx_count_distinct("plate_number").cast("int").alias("vehicle_count"),
            F.avg((F.col("speed_kph") > F.col("speed_limit_kph")).cast("double")).alias("pct_over_limit"),
        )
        .select(
            "camera_id",
            F.to_date(F.col("window.start")).alias("stat_date"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "avg_speed_kph",
            "max_speed_kph",
            "reading_count",
            "vehicle_count",
            "pct_over_limit",
            F.col("speed_limit_kph").cast("int").alias("speed_limit_kph"),
        )
    )


def _cassandra_writer(name: str, tables: list[str], debug: bool):
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        batch_df.persist()
        try:
            for table in tables:
                (batch_df.write.format("org.apache.spark.sql.cassandra")
                 .options(keyspace=config.CASSANDRA_KEYSPACE, table=table)
                 .mode("append")
                 .save())
            print(f"[{name}] batch {batch_id}: wrote {batch_df.count()} rows to "
                  f"{', '.join(tables)}", flush=True)
            if debug:
                batch_df.show(10, truncate=False)
        finally:
            batch_df.unpersist()

    return write_batch


def start_query(df: DataFrame, name: str, tables: list[str], debug: bool):
    return (
        df.writeStream.queryName(name)
        .outputMode("append")
        .foreachBatch(_cassandra_writer(name, tables, debug))
        .option("checkpointLocation", str(Path(config.SPARK_CHECKPOINT_DIR) / name))
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .start()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="NCAP streaming violation detector.")
    parser.add_argument("--debug", action="store_true", help="print every written micro-batch")
    args = parser.parse_args()

    spark = create_spark_session()
    cameras = F.broadcast(load_camera_registry(spark))
    enriched = read_telemetry(spark).join(cameras, on="camera_id", how="inner")

    start_query(detect_speeding(enriched), "speeding_violations", VIOLATION_TABLES, args.debug)
    start_query(detect_red_light(enriched), "red_light_violations", VIOLATION_TABLES, args.debug)
    start_query(detect_illegal_parking(enriched), "illegal_parking_violations",
                VIOLATION_TABLES, args.debug)
    start_query(camera_speed_stats(enriched), "camera_speed_stats", STATS_TABLES, args.debug)

    print(f"Streaming from Kafka topic '{config.KAFKA_TOPIC}' into Cassandra keyspace "
          f"'{config.CASSANDRA_KEYSPACE}'. Press Ctrl+C to stop.", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
