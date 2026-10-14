"""
Spark Structured Streaming job for MapYourWay.
Consumes real TomTom traffic data from Kafka, computes features, applies ML model, writes predictions.

Data Flow:
1. Read from Kafka topics: gps_events, sensor_events, weather_events
2. Window by segment_id (30-second windows sliding every 10 seconds)
3. Compute the canonical features (speed_ratio, occupancy_pct, veh_per_lane, rain_mm_hr, visibility_km)
4. Apply trained RandomForest model to predict congestion level (0-3)
5. Write predictions to Kafka (congestion_predictions topic) and console
"""
import os
import logging
import shutil
import sys
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    avg,
    coalesce,
    lit,
    when,
    sum as spark_sum,
    max as spark_max,
    round as spark_round,
    to_date,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, LongType, BooleanType
)

from mapyourway import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Schema for GPS events (from TomTom real_producer.py)
GPS_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("segment_id", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("speed_kmph", DoubleType(), True),
    StructField("free_flow_speed_kmph", DoubleType(), True),
    StructField("current_travel_time_s", DoubleType(), True),
    StructField("free_flow_travel_time_s", DoubleType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("road_closure", BooleanType(), True),
    StructField("observed_at_ms", LongType(), True),
    StructField("source", StringType(), True),
    StructField("source_observation", StringType(), True),
    StructField("snapshot_id", StringType(), True),
    StructField("fetched_at_ms", LongType(), True),
    StructField("emitted_at_ms", LongType(), True),
    StructField("is_cached_observation", BooleanType(), True),
    StructField("ts_ms", LongType(), True),
])

# Schema for sensor events (occupancy & vehicle counts from TomTom estimates)
SENSOR_SCHEMA = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("segment_id", StringType(), True),
    StructField("occupancy_pct", DoubleType(), True),
    StructField("vehicle_count", DoubleType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("road_closure", BooleanType(), True),
    StructField("observed_at_ms", LongType(), True),
    StructField("source", StringType(), True),
    StructField("source_observation", StringType(), True),
    StructField("snapshot_id", StringType(), True),
    StructField("fetched_at_ms", LongType(), True),
    StructField("emitted_at_ms", LongType(), True),
    StructField("is_cached_observation", BooleanType(), True),
    StructField("ts_ms", LongType(), True),
])

# Schema for weather events
WEATHER_SCHEMA = StructType([
    StructField("station_id", StringType(), True),
    StructField("rain_mm_hr", DoubleType(), True),
    StructField("visibility_km", DoubleType(), True),
    StructField("temp_c", DoubleType(), True),
    StructField("humidity_pct", DoubleType(), True),
    StructField("wind_speed_kmph", DoubleType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("observed_at_ms", LongType(), True),
    StructField("source", StringType(), True),
    StructField("is_cached_observation", BooleanType(), True),
    StructField("ts_ms", LongType(), True),
])


def create_spark_session():
    """Create and configure Spark session for streaming with Kafka support."""
    # On Windows, Spark otherwise searches PATH for a command literally named
    # ``python``.  Point workers at the interpreter running this process; in
    # Docker this resolves to the image's Python executable.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = SparkSession.builder \
        .appName("MapYourWay-Streaming-RealData") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "false") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark, kafka_bootstrap_servers, topic, schema):
    """Read streaming data from Kafka and parse JSON with given schema."""
    logger.info(f"Connecting to Kafka at {kafka_bootstrap_servers}, topic: {topic}")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON from Kafka value and convert timestamp
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*") \
     .withColumn("event_time", (col("ts_ms") / 1000).cast("timestamp"))
    
    return parsed_df


def load_segment_metadata(spark):
    """Load segment metadata (free_flow_kmph, lanes) as static DataFrame."""
    from mapyourway.network import SEGMENTS
    segment_data = [
        (
            seg["segment_id"],
            float(seg["free_flow_kmph"]),
            int(seg["lanes"]),
            seg["name"],
        )
        for seg in SEGMENTS
    ]
    return spark.createDataFrame(
        segment_data,
        "segment_id string, free_flow_kmph double, lanes int, name string",
    )


def compute_windowed_features(
    gps_stream,
    sensor_stream,
    weather_stream,
    segment_meta,
    window_duration=config.WINDOW_DURATION,
    slide_duration=config.SLIDE_DURATION,
    watermark_delay=config.WATERMARK_DELAY,
):
    """
    Compute traffic features from windowed aggregations.
    
    Features computed (matching train.py):
    - speed_ratio: mean_speed / free_flow_speed
    - occupancy_pct: mean occupancy percentage
    - veh_per_lane: mean vehicle count divided by lanes
    - rain_mm_hr: mean rain in mm/hr
    - visibility_km: mean visibility in kilometres
    """
    
    # Stream-stream joins are not supported by Spark in update mode and are
    # especially fragile when weather arrives less often than traffic.  Unify
    # the three validated event types first, fan the city-wide weather event
    # out through the static segment table, and run one stateful aggregation.
    # This preserves event-time windows while keeping the query legal in both
    # local Spark and the Docker runtime.

    gps = gps_stream.filter(
        col("segment_id").isNotNull()
        & col("vehicle_id").isNotNull()
        & col("ts_ms").isNotNull()
        & col("event_time").isNotNull()
        & col("speed_kmph").between(0.0, 150.0)
    ).select(
        "segment_id",
        "event_time",
        lit("gps").alias("event_kind"),
        col("vehicle_id").alias("entity_id"),
        col("speed_kmph"),
        col("free_flow_speed_kmph").alias("free_flow_observed_kmph"),
        lit(None).cast("double").alias("occupancy_pct"),
        lit(None).cast("double").alias("vehicle_count"),
        lit(None).cast("double").alias("rain_mm_hr"),
        lit(None).cast("double").alias("visibility_km"),
        col("confidence"),
        coalesce(col("road_closure"), lit(False)).alias("road_closure"),
        coalesce(col("observed_at_ms"), col("ts_ms")).alias("observed_at_ms"),
        col("source"),
        col("snapshot_id"),
        col("fetched_at_ms"),
        coalesce(col("is_cached_observation"), lit(False)).alias("is_cached_observation"),
        col("ts_ms"),
    )

    sensor = sensor_stream.filter(
        col("segment_id").isNotNull()
        & col("sensor_id").isNotNull()
        & col("ts_ms").isNotNull()
        & col("event_time").isNotNull()
        & col("occupancy_pct").between(0.0, 100.0)
        & (col("vehicle_count") >= 0)
    ).select(
        "segment_id",
        "event_time",
        lit("sensor").alias("event_kind"),
        col("sensor_id").alias("entity_id"),
        lit(None).cast("double").alias("speed_kmph"),
        lit(None).cast("double").alias("free_flow_observed_kmph"),
        col("occupancy_pct"),
        col("vehicle_count"),
        lit(None).cast("double").alias("rain_mm_hr"),
        lit(None).cast("double").alias("visibility_km"),
        col("confidence"),
        coalesce(col("road_closure"), lit(False)).alias("road_closure"),
        coalesce(col("observed_at_ms"), col("ts_ms")).alias("observed_at_ms"),
        col("source"),
        col("snapshot_id").cast("string"),
        col("fetched_at_ms").cast("long"),
        coalesce(col("is_cached_observation"), lit(False)).alias("is_cached_observation"),
        col("ts_ms"),
    )

    weather = weather_stream.filter(
        col("station_id").isNotNull()
        & col("ts_ms").isNotNull()
        & col("event_time").isNotNull()
        & (col("rain_mm_hr") >= 0)
        & (col("visibility_km") >= 0)
    ).crossJoin(segment_meta.select("segment_id")).select(
        "segment_id",
        "event_time",
        lit("weather").alias("event_kind"),
        col("station_id").alias("entity_id"),
        lit(None).cast("double").alias("speed_kmph"),
        lit(None).cast("double").alias("free_flow_observed_kmph"),
        lit(None).cast("double").alias("occupancy_pct"),
        lit(None).cast("double").alias("vehicle_count"),
        col("rain_mm_hr"),
        col("visibility_km"),
        lit(None).cast("double").alias("confidence"),
        lit(False).alias("road_closure"),
        coalesce(col("observed_at_ms"), col("ts_ms")).alias("observed_at_ms"),
        col("source"),
        lit(None).cast("string").alias("snapshot_id"),
        lit(None).cast("long").alias("fetched_at_ms"),
        coalesce(col("is_cached_observation"), lit(False)).alias("is_cached_observation"),
        col("ts_ms"),
    )

    events = gps.unionByName(sensor).unionByName(weather)
    events = events.withWatermark("event_time", watermark_delay).dropDuplicates(
        ["event_kind", "entity_id", "segment_id", "ts_ms"]
    )
    grouped = events.groupBy(
        col("segment_id"), window(col("event_time"), window_duration, slide_duration)
    ).agg(
        avg(when(col("event_kind") == "gps", col("speed_kmph"))).alias("mean_speed"),
        avg(when(col("event_kind") == "gps", col("free_flow_observed_kmph"))).alias(
            "mean_observed_free_flow"
        ),
        avg(when(col("event_kind") == "sensor", col("occupancy_pct"))).alias(
            "mean_occupancy"
        ),
        avg(when(col("event_kind") == "sensor", col("vehicle_count"))).alias(
            "mean_vehicle_count"
        ),
        avg(when(col("event_kind") == "weather", col("rain_mm_hr"))).alias("mean_rain"),
        avg(when(col("event_kind") == "weather", col("visibility_km"))).alias(
            "mean_visibility_km"
        ),
        spark_sum(when(col("event_kind") == "gps", 1).otherwise(0)).cast("long").alias(
            "gps_count"
        ),
        spark_sum(when(col("event_kind") == "sensor", 1).otherwise(0)).cast("long").alias(
            "sensor_count"
        ),
        spark_sum(when(col("event_kind") == "weather", 1).otherwise(0)).cast("long").alias(
            "weather_count"
        ),
        spark_max(when(col("road_closure"), 1).otherwise(0)).cast("boolean").alias(
            "road_closure"
        ),
        spark_max(col("confidence")).alias("source_confidence"),
        spark_max(when(col("event_kind") == "gps", col("observed_at_ms"))).alias(
            "source_observed_at_ms"
        ),
        spark_max(when(col("event_kind") == "gps", col("snapshot_id"))).alias(
            "source_snapshot_id"
        ),
        spark_max(when(col("event_kind") == "weather", col("observed_at_ms"))).alias(
            "weather_observed_at_ms"
        ),
        spark_max(when(col("event_kind") == "weather", col("source"))).alias(
            "weather_source"
        ),
    ).where(col("gps_count") > 0)

    enriched = grouped.join(segment_meta, on="segment_id", how="left")
    enriched = enriched.withColumn(
        "free_flow_kmph",
        coalesce(col("mean_observed_free_flow"), col("free_flow_kmph")),
    )
    return (
        enriched.withColumn(
            "speed_ratio",
            when(
                col("free_flow_kmph") > 0,
                coalesce(col("mean_speed"), col("free_flow_kmph"))
                / col("free_flow_kmph"),
            ).otherwise(1.0),
        )
        .withColumn("occupancy_pct", coalesce(col("mean_occupancy"), lit(0.0)))
        .withColumn(
            "veh_per_lane",
            when(col("lanes") > 0, coalesce(col("mean_vehicle_count"), lit(0.0)) / col("lanes"))
            .otherwise(0.0),
        )
        .withColumn("rain_mm_hr", coalesce(col("mean_rain"), lit(0.0)))
        .withColumn("visibility_km", coalesce(col("mean_visibility_km"), lit(10.0)))
        .withColumn("weather_available", (col("weather_count") > 0))
        .fillna(0.0, subset=config.FEATURE_COLUMNS)
    )


def apply_ml_model(features_df, model_path):
    """Apply trained RandomForest model to predict congestion level."""
    from pyspark.ml import PipelineModel

    def rule_based_predictions(df):
        return df.withColumn(
            "congestion_level",
            when(col("speed_ratio") >= 0.8, lit(0))
            .when(col("speed_ratio") >= 0.5, lit(1))
            .when(col("speed_ratio") >= 0.3, lit(2))
            .otherwise(lit(3))
        ).withColumn("prediction_confidence", lit(0.5)).withColumn(
            "prediction_source", lit("rule_based_speed_ratio")
        )

    if not os.path.exists(model_path):
        logger.warning(f"Model not found at {model_path}, using rule-based classification")
        return rule_based_predictions(features_df)

    logger.info(f"Loading ML model from {model_path}")
    try:
        model = PipelineModel.load(model_path)
        # The current live stream and the original offline trainer use
        # different feature contracts.  Do not let an incompatible model
        # bring down the streaming job; use the transparent fallback until
        # the real-data training phase produces a matching model.
        assembler = next(
            (stage for stage in model.stages if hasattr(stage, "getInputCols")),
            None,
        )
        required = set(assembler.getInputCols()) if assembler else set()
        missing = required.difference(features_df.columns)
        if missing:
            logger.warning(
                "Model feature contract does not match the live stream; "
                "missing columns: %s. Using rule-based classification.",
                sorted(missing),
            )
            return rule_based_predictions(features_df)
    except Exception:
        logger.exception("Could not load model at %s; using rule-based classification", model_path)
        return rule_based_predictions(features_df)

    # Apply model.  A corrupt/incompatible runtime model must degrade to the
    # documented rule rather than taking down Kafka ingestion and the dashboard.
    try:
        predictions = model.transform(features_df)

        # Extract confidence (max probability)
        from pyspark.sql.functions import udf
        from pyspark.sql.types import FloatType

        @udf(returnType=FloatType())
        def extract_confidence(probability_vector):
            if probability_vector is None:
                return 0.5
            try:
                return float(max(probability_vector))
            except (TypeError, ValueError):
                return 0.5

        return (
            predictions
            .withColumn("congestion_level", col("prediction").cast(IntegerType()))
            .withColumn("prediction_confidence", extract_confidence(col("probability")))
            .withColumn("prediction_source", lit("spark_ml_pipeline"))
        )
    except Exception:
        logger.exception("Model inference failed; using rule-based classification")
        return rule_based_predictions(features_df)


def _directory_size(path: Path) -> int:
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def prune_lake() -> None:
    """Delete oldest batch partitions until the configured lake cap is met."""
    lake = Path(config.CONGESTION_LAKE)
    if not lake.exists():
        return
    max_bytes = int(config.LAKE_MAX_GB * 1024**3)
    batches = sorted(
        (path for path in lake.glob("dt=*/batch_id=*") if path.is_dir()),
        key=lambda path: path.stat().st_mtime,
    )
    total = sum(_directory_size(path) for path in batches)
    while total > max_bytes and batches:
        oldest = batches.pop(0)
        size = _directory_size(oldest)
        shutil.rmtree(oldest)
        total -= size
        logger.warning("Pruned old lake partition %s", oldest)


def _merge_export_frame(existing, current):
    """Merge CSV and Spark batch rows using one comparable timestamp dtype."""
    import pandas as pd

    merged = current.copy() if existing is None else pd.concat(
        [existing, current], ignore_index=True
    )
    if "timestamp" in merged.columns:
        merged["timestamp"] = pd.to_datetime(
            merged["timestamp"], utc=True, errors="coerce"
        )
        merged = merged.sort_values("timestamp", na_position="last").reset_index(drop=True)
    return merged

def write_stream_batch(batch_df, batch_id: int, kafka_bootstrap: str) -> None:
    """Write one micro-batch to Kafka, Parquet, and an inspection CSV.

    The batch-id partition makes replayed micro-batches idempotent for the
    Parquet sink.  Kafka remains at-least-once, which is appropriate for the
    serving topic because the dashboard keeps only the newest segment value.
    """
    if batch_df.limit(1).count() == 0:
        return

    batch_df = batch_df.withColumn("batch_id", lit(int(batch_id))).cache()
    try:
        batch_df.show(20, truncate=False)

        kafka_batch = batch_df.selectExpr(
            "CAST(segment_id AS STRING) AS key",
            "to_json(struct(*)) AS value",
        )
        (
            kafka_batch.write.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap)
            .option("topic", config.TOPIC_CONGESTION)
            .save()
        )

        # Write each UTC day separately so a batch crossing midnight is not
        # assigned to the first row's date.  A stable batch-id path makes a
        # retried foreachBatch overwrite its own Parquet output.
        dated = batch_df.withColumn("_dt", to_date(col("timestamp")))
        days = [row["_dt"] for row in dated.select("_dt").distinct().collect()]
        for day_value in days:
            day = (
                day_value.strftime("%Y-%m-%d")
                if day_value is not None
                else datetime.now(timezone.utc).strftime("%Y-%m-%d")
            )
            lake_path = os.path.join(
                config.CONGESTION_LAKE,
                f"dt={day}",
                f"batch_id={int(batch_id):08d}",
            )
            dated.filter(
                col("_dt").isNull() if day_value is None else col("_dt") == lit(day_value)
            ).drop("_dt").coalesce(1).write.mode("overwrite").parquet(lake_path)
        prune_lake()

        # A flat CSV is convenient for R and for quickly inspecting the data
        # from a text editor.  It is derived from the same batch as Parquet.
        os.makedirs(config.EXPORT_DIR, exist_ok=True)
        import pandas as pd

        csv_path = config.CONGESTION_CSV
        batch_pdf = batch_df.toPandas()
        existing = pd.read_csv(csv_path) if os.path.exists(csv_path) else None
        batch_pdf = _merge_export_frame(existing, batch_pdf)
        # A window revision is identified by segment and window timestamp;
        # batch_id is an execution detail and must not create duplicates after
        # a restart or checkpoint recovery.
        dedupe_columns = [
            column for column in ("segment_id", "timestamp") if column in batch_pdf.columns
        ]
        if dedupe_columns:
            batch_pdf = batch_pdf.drop_duplicates(subset=dedupe_columns, keep="last")
        if len(batch_pdf) > config.EXPORT_MAX_ROWS:
            batch_pdf = batch_pdf.tail(config.EXPORT_MAX_ROWS)
        batch_pdf.to_csv(csv_path, index=False)
        max_export_bytes = int(config.EXPORT_MAX_GB * 1024**3)
        if max_export_bytes > 0 and os.path.getsize(csv_path) > max_export_bytes:
            keep_rows = max(1, int(len(batch_pdf) * max_export_bytes / os.path.getsize(csv_path)))
            batch_pdf.tail(keep_rows).to_csv(csv_path, index=False)
    finally:
        batch_df.unpersist()


def main():
    """Main streaming job entry point."""
    # Configuration from environment or defaults
    kafka_bootstrap = config.KAFKA_BOOTSTRAP_SERVERS
    model_path = os.getenv("MODEL_PATH", config.MODEL_PATH)
    
    logger.info("=" * 70)
    logger.info("  MapYourWay Spark Streaming Job - Real TomTom Data")
    logger.info("=" * 70)
    logger.info(f"Kafka: {kafka_bootstrap}")
    logger.info(f"Input Topics: {', '.join(config.ALL_INPUT_TOPICS)}")
    logger.info(f"Output Topic: {config.TOPIC_CONGESTION}")
    logger.info(f"Model: {model_path}")
    logger.info(
        "Window: %s, slide: %s, watermark: %s, trigger: %s",
        config.WINDOW_DURATION,
        config.SLIDE_DURATION,
        config.WATERMARK_DELAY,
        config.TRIGGER_PROCESSING_TIME,
    )
    logger.info("=" * 70)
    
    # Create Spark session
    config.ensure_directories()
    spark = create_spark_session()
    
    try:
        # Load segment metadata
        segment_meta = load_segment_metadata(spark)
        logger.info(f"Loaded metadata for {segment_meta.count()} road segments")
        
        # Read streaming data from Kafka
        gps_stream = read_kafka_stream(spark, kafka_bootstrap, config.TOPIC_GPS, GPS_SCHEMA)
        sensor_stream = read_kafka_stream(spark, kafka_bootstrap, config.TOPIC_SENSOR, SENSOR_SCHEMA)
        weather_stream = read_kafka_stream(spark, kafka_bootstrap, config.TOPIC_WEATHER, WEATHER_SCHEMA)
        
        # Compute windowed features
        features = compute_windowed_features(
            gps_stream, sensor_stream, weather_stream,
            segment_meta,
            config.WINDOW_DURATION,
            config.SLIDE_DURATION,
            config.WATERMARK_DELAY,
        )
        
        # Apply ML model
        predictions = apply_ml_model(features, model_path)
        
        # Format output
        output = predictions.select(
            col("segment_id"),
            col("name").alias("road_name"),
            col("window.end").alias("timestamp"),
            col("congestion_level"),
            spark_round(col("prediction_confidence"), 3).alias("confidence"),
            spark_round(col("speed_ratio"), 3).alias("speed_ratio"),
            spark_round(col("occupancy_pct"), 1).alias("occupancy_pct"),
            spark_round(col("veh_per_lane"), 3).alias("veh_per_lane"),
            spark_round(col("rain_mm_hr"), 3).alias("rain_mm_hr"),
            spark_round(col("visibility_km"), 3).alias("visibility_km"),
            spark_round(col("mean_speed"), 1).alias("current_speed_kmph"),
            col("free_flow_kmph"),
            col("gps_count"),
            col("sensor_count"),
            col("weather_count"),
            spark_round(col("mean_vehicle_count"), 2).alias("estimated_vehicle_count"),
            (col("gps_count") + col("sensor_count")).alias("record_count"),
            col("road_closure"),
            col("source_confidence"),
            col("source_observed_at_ms"),
            col("source_snapshot_id"),
            col("weather_observed_at_ms"),
            col("weather_source"),
            col("weather_available"),
            col("prediction_source"),
            # A persistence forecast is deliberately exposed as a baseline,
            # not as a claim of future-label ML accuracy.  It lets routing use
            # a declared horizon while a supervised future model is trained.
            spark_round(col("mean_speed"), 1).alias("forecast_speed_kmph"),
            lit(config.FORECAST_HORIZON_MINUTES).alias("forecast_horizon_minutes"),
            (
                col("window.end").cast("long") * lit(1000)
                + lit(config.FORECAST_HORIZON_MINUTES * 60_000)
            ).alias("forecast_for_ms"),
            lit(config.FORECAST_SOURCE).alias("forecast_source"),
            lit(config.FORECAST_MODEL_ID).alias("forecast_model_id"),
            # This is the prediction emitted by the serving model, not
            # independent ground truth.  The training command refuses it as a
            # production label unless the user explicitly opts in.
            lit("live_prediction").alias("label_source"),
        ).withColumn(
            "congestion_label",
            when(col("congestion_level") == 0, lit("free_flow"))
            .when(col("congestion_level") == 1, lit("moderate"))
            .when(col("congestion_level") == 2, lit("heavy"))
            .otherwise(lit("severe"))
        )
        
        query = (
            output.writeStream.outputMode("update")
            .foreachBatch(
                lambda batch_df, batch_id: write_stream_batch(
                    batch_df, batch_id, kafka_bootstrap
                )
            )
            .option(
                "checkpointLocation",
                os.path.join(config.CHECKPOINT_DIR, config.CHECKPOINT_VERSION),
            )
            .trigger(processingTime=config.TRIGGER_PROCESSING_TIME)
            .start()
        )

        logger.info("Streaming query started: %s. Press Ctrl+C to stop.", query.id)
        spark.streams.awaitAnyTermination()
    
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Streaming job failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
