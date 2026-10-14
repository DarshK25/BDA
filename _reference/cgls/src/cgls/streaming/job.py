"""Spark Structured Streaming job: raw events in -> congestion estimate + forecast out.

Submit (done for you by docker-compose):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 job.py

Pipeline
  1. READ     one Kafka source subscribed to gps_events, sensor_events, weather_events
  2. PARSE    JSON -> typed columns, bad records dropped (corrupt JSON, impossible speeds)
  3. UNIFY    the three event types share one schema (weather is fanned out to every segment)
  4. WINDOW   30 s sliding windows, a new one every 5 s, per road segment, with a watermark
  5. ENRICH   join the static road table (free-flow speed, lanes) and build ML features
  6. PREDICT  the trained MLlib RandomForest gives level + probabilities
  7. FORECAST per-segment trend (kept on the driver) -> expected level in 60 s
  8. SINK     foreachBatch -> console table, Parquet "data lake", Kafka topic congestion_updates

Why 'append' mode + watermark: a window is emitted exactly once, when Spark is sure no more
events for it will arrive. Every output row is therefore final, which makes the Parquet
files and the Kafka topic clean (no half-finished windows to de-duplicate).
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import time
from collections import defaultdict, deque

import numpy as np
import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from cgls import config
from cgls.network import SEGMENTS
from cgls.traffic_model import LEVEL_MIDPOINTS, LEVEL_NAMES, level_from_score

SOURCE = os.getenv("CGLS_SOURCE", "kafka")      # kafka | files   (files is used by tests)
SINK = os.getenv("CGLS_SINK", "kafka")          # kafka | files
FILES_IN = os.getenv("CGLS_FILES_IN", "/tmp/cgls_events")
FILES_OUT = os.getenv("CGLS_FILES_OUT", "/tmp/cgls_out")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER", "100000")
AVAILABLE_NOW = os.getenv("CGLS_AVAILABLE_NOW", "0") == "1"   # process what exists, then stop (tests)

GPS_SCHEMA = T.StructType([
    T.StructField("vehicle_id", T.StringType()), T.StructField("segment_id", T.StringType()),
    T.StructField("lat", T.DoubleType()), T.StructField("lon", T.DoubleType()),
    T.StructField("speed_kmph", T.DoubleType()), T.StructField("ts_ms", T.LongType()),
])
SENSOR_SCHEMA = T.StructType([
    T.StructField("sensor_id", T.StringType()), T.StructField("segment_id", T.StringType()),
    T.StructField("occupancy_pct", T.DoubleType()), T.StructField("vehicle_count", T.DoubleType()),
    T.StructField("ts_ms", T.LongType()),
])
WEATHER_SCHEMA = T.StructType([
    T.StructField("station_id", T.StringType()), T.StructField("rain_mm_hr", T.DoubleType()),
    T.StructField("visibility_km", T.DoubleType()), T.StructField("temp_c", T.DoubleType()),
    T.StructField("humidity_pct", T.DoubleType()), T.StructField("ts_ms", T.LongType()),
])

# columns every event type is converted to (missing ones become NULL)
UNIFIED = {"vehicle_id": "string", "speed_kmph": "double", "occupancy_pct": "double",
           "vehicle_count": "double", "rain_mm_hr": "double", "visibility_km": "double"}


OUT_SCHEMA = T.StructType([T.StructField(n, t, True) for n, t in [
    ("segment_id", T.StringType()), ("name", T.StringType()),
    ("window_start_ms", T.LongType()), ("window_end_ms", T.LongType()),
    ("avg_speed_kmph", T.DoubleType()), ("free_flow_kmph", T.DoubleType()), ("speed_ratio", T.DoubleType()),
    ("occupancy_pct", T.DoubleType()), ("veh_per_lane", T.DoubleType()), ("rain_mm_hr", T.DoubleType()),
    ("visibility_km", T.DoubleType()), ("gps_pings", T.LongType()), ("vehicles", T.LongType()),
    ("level", T.LongType()), ("confidence", T.DoubleType()), ("score", T.DoubleType()),
    ("forecast_score", T.DoubleType()), ("forecast_level", T.LongType()),
    ("level_name", T.StringType()), ("forecast_level_name", T.StringType()),
    ("est_speed_kmph", T.DoubleType()), ("incident_suspected", T.BooleanType()),
    ("forecast_source", T.StringType()), ("emitted_ms", T.LongType()), ("batch_id", T.LongType()),
    ("dt", T.StringType())]])
ALERT_SCHEMA = ("segment_id string, name string, window_end_ms long, speed_ratio double, occupancy_pct double, "
                "speed_drop double, occupancy_rise double, emitted_ms long")


# ----------------------------------------------------------------------------- spark
def get_spark() -> SparkSession:
    spark = (SparkSession.builder.appName("cgls-congestion-stream")
             .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "4"))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def segments_df(spark: SparkSession) -> DataFrame:
    rows = [(s["segment_id"], s["name"], s["free_flow_kmph"], float(s["lanes"]), s["length_km"]) for s in SEGMENTS]
    return spark.createDataFrame(rows, "segment_id string, name string, free_flow_kmph double, lanes double, length_km double")


# ----------------------------------------------------------------------------- 1. read
def read_raw(spark: SparkSession) -> DataFrame:
    """Returns a streaming DataFrame with columns (topic, value)."""
    if SOURCE == "kafka":
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
                .option("subscribe", ",".join(config.INPUT_TOPICS))
                .option("startingOffsets", STARTING_OFFSETS)
                .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
                .option("failOnDataLoss", "false")
                .load()
                .select("topic", F.col("value").cast("string").alias("value")))
    parts = [spark.readStream.text(os.path.join(FILES_IN, t)).withColumn("topic", F.lit(t))
             for t in config.INPUT_TOPICS]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.select("topic", "value")


# ----------------------------------------------------------------------------- 2+3. parse / unify
def _event_time(col: str = "ts_ms"):
    return (F.col(col) / 1000).cast("timestamp")


def _unify(df: DataFrame, present: list[str], key_col: str, kind: str) -> DataFrame:
    """Common schema for all three event types. `dedup_key` identifies 'the same message'
    (kind + vehicle/segment); together with event_ts it is used to drop network retries."""
    cols = [F.col(c) if c in present else F.lit(None).cast(t).alias(c) for c, t in UNIFIED.items()]
    return df.select("segment_id", "event_ts", F.concat_ws(":", F.lit(kind), F.col(key_col)).alias("dedup_key"), *cols)


def parse_events(raw: DataFrame, spark: SparkSession) -> DataFrame:
    gps = (raw.filter(F.col("topic") == config.TOPIC_GPS)
           .select(F.from_json("value", GPS_SCHEMA).alias("j")).select("j.*")
           .filter(F.col("segment_id").isNotNull() & F.col("ts_ms").isNotNull()
                   & F.col("speed_kmph").between(0.0, 150.0))
           .withColumn("event_ts", _event_time()))
    gps = _unify(gps, ["vehicle_id", "speed_kmph"], key_col="vehicle_id", kind="gps")

    sensor = (raw.filter(F.col("topic") == config.TOPIC_SENSOR)
              .select(F.from_json("value", SENSOR_SCHEMA).alias("j")).select("j.*")
              .filter(F.col("segment_id").isNotNull() & F.col("ts_ms").isNotNull()
                      & F.col("occupancy_pct").between(0.0, 100.0) & (F.col("vehicle_count") >= 0))
              .withColumn("event_ts", _event_time()))
    sensor = _unify(sensor, ["occupancy_pct", "vehicle_count"], key_col="segment_id", kind="sensor")

    # Weather is city-wide: fan it out to every segment so it can be averaged per segment.
    # (stream-static cross join: each weather event x 19 segments)
    seg_ids = F.broadcast(segments_df(spark).select("segment_id"))
    weather = (raw.filter(F.col("topic") == config.TOPIC_WEATHER)
               .select(F.from_json("value", WEATHER_SCHEMA).alias("j")).select("j.*")
               .filter(F.col("ts_ms").isNotNull() & (F.col("rain_mm_hr") >= 0) & (F.col("visibility_km") >= 0))
               .withColumn("event_ts", _event_time())
               .crossJoin(seg_ids))
    weather = _unify(weather, ["rain_mm_hr", "visibility_km"], key_col="segment_id", kind="weather")

    return gps.unionByName(sensor).unionByName(weather)


# ----------------------------------------------------------------------------- 4. window
def window_aggregate(events: DataFrame) -> DataFrame:
    return (events.withWatermark("event_ts", config.WATERMARK)
            .dropDuplicates(["dedup_key", "event_ts"])        # network retries: same message twice -> once
            .groupBy(F.window("event_ts", config.WINDOW_DURATION, config.SLIDE_DURATION), "segment_id")
            .agg(F.avg("speed_kmph").alias("avg_speed_kmph"),
                 F.count("speed_kmph").alias("gps_pings"),
                 F.approx_count_distinct("vehicle_id").alias("vehicles"),
                 F.avg("occupancy_pct").alias("avg_occ"),
                 F.avg("vehicle_count").alias("avg_vc"),
                 F.avg("rain_mm_hr").alias("rain_mm_hr_raw"),
                 F.avg("visibility_km").alias("vis_raw")))


# ----------------------------------------------------------------------------- 5. features
def add_features(agg: DataFrame, spark: SparkSession) -> DataFrame:
    """Spark-SQL twin of cgls.features.compute_features - keep the two in sync."""
    d = (agg.join(F.broadcast(segments_df(spark)), "segment_id")
         .withColumn("window_start_ms", F.col("window.start").cast("double") * 1000)
         .withColumn("window_end_ms", F.col("window.end").cast("double") * 1000)
         .drop("window"))
    ratio = F.coalesce(F.col("avg_speed_kmph") / F.col("free_flow_kmph"),
                       1.0 - 0.8 * F.col("avg_occ") / 100.0, F.lit(1.0))
    d = d.withColumn("speed_ratio", F.least(F.greatest(ratio, F.lit(0.05)), F.lit(1.3)))
    d = d.withColumn("occupancy_pct",
                     F.coalesce(F.col("avg_occ"), F.greatest(F.lit(0.0), (1.0 - F.col("speed_ratio")) / 0.8 * 100.0)))
    d = d.withColumn("veh_per_lane",
                     F.coalesce(F.col("avg_vc") / F.col("lanes"), 3.0 + 9.0 * F.col("occupancy_pct") / 100.0))
    d = d.withColumn("rain_mm_hr", F.coalesce(F.col("rain_mm_hr_raw"), F.lit(0.0)))
    d = d.withColumn("visibility_km", F.coalesce(F.col("vis_raw"), F.lit(10.0)))
    return d


# ----------------------------------------------------------------------------- 6. predict
def ensure_model(spark: SparkSession) -> PipelineModel:
    if not os.path.exists(os.path.join(config.MODEL_DIR, "metadata")):
        print("[job] no trained model found - training one now (takes about a minute) ...", flush=True)
        from cgls.ml.train import train_and_save
        train_and_save(spark, n_rows=int(os.getenv("CGLS_TRAIN_ROWS", "60000")))
    return PipelineModel.load(config.MODEL_DIR)


def ensure_forecaster(spark: SparkSession):
    """Learned 60 s forecaster (GBT). Returns None if disabled with CGLS_FORECASTER=off."""
    if os.getenv("CGLS_FORECASTER", "on") == "off":
        return None
    if not os.path.exists(os.path.join(config.FORECAST_MODEL_DIR, "metadata")):
        print("[job] no forecaster model found - training one now (takes about 2 minutes) ...", flush=True)
        from cgls.ml.forecaster import train_and_save as train_fc
        train_fc(spark, n_runs=int(os.getenv("CGLS_FORECAST_RUNS", "24")))
    return PipelineModel.load(config.FORECAST_MODEL_DIR)


def predict(feat: DataFrame, model: PipelineModel) -> DataFrame:
    p = model.transform(feat).withColumn("proba", vector_to_array("probability"))
    score = sum(F.col("proba")[i] * LEVEL_MIDPOINTS[i] for i in range(4))
    return (p.withColumn("level", F.col("prediction").cast("int"))
            .withColumn("confidence", F.array_max("proba"))
            .withColumn("score", score)
            .select("segment_id", "name", "window_start_ms", "window_end_ms",
                    "avg_speed_kmph", "free_flow_kmph", "speed_ratio", "occupancy_pct", "veh_per_lane",
                    "rain_mm_hr", "visibility_km", "gps_pings", "vehicles", "level", "confidence", "score"))


# ----------------------------------------------------------------------------- 7+8. foreachBatch
class BatchProcessor:
    """Runs on the driver once per micro-batch. A batch is small (about 19 rows per window),
    so pandas is the right tool for per-segment history, the forecaster input and the console table.

    State kept between batches (rebuilt within ~50 s after a restart):
      history[segment][window_end_ms] = (speed_ratio, occupancy, rain, visibility)
      alert_active[segment]           = an incident is currently suspected there
    """

    ALERT_SPEED_DROP = 0.20      # speed ratio fell by >= 0.20 within 30 s ...
    ALERT_OCC_RISE = 15.0        # ... while sensor occupancy rose by >= 15 points ...
    ALERT_MAX_RATIO = 0.65       # ... and the road is now actually slow
    CLEAR_RATIO = 0.65           # alert ends when speed recovers above this

    def __init__(self, spark: SparkSession, forecaster=None):
        self.spark = spark
        self.forecaster = forecaster
        self.history: dict[str, dict[int, tuple]] = defaultdict(dict)
        self.alert_active: dict[str, bool] = defaultdict(bool)

    # ---- learned forecast + incident detection -----------------------------------------
    def _enrich(self, pdf: pd.DataFrame) -> pd.DataFrame:
        from cgls.ml.forecaster import FORECAST_FEATURES, add_lag_features

        for r in pdf.itertuples():
            self.history[r.segment_id][int(r.window_end_ms)] = (r.speed_ratio, r.occupancy_pct, r.rain_mm_hr, r.visibility_km)
        newest = int(pdf["window_end_ms"].max())
        for seg in self.history:
            self.history[seg] = {k: v for k, v in self.history[seg].items() if k >= newest - 70000}

        hist = pd.DataFrame([{"segment_id": seg, "window_end_ms": end, "speed_ratio": v[0], "occupancy_pct": v[1],
                              "rain_mm_hr": v[2], "visibility_km": v[3]}
                             for seg, d in self.history.items() for end, v in d.items()])
        feat = add_lag_features(hist)
        cur = pdf[["segment_id", "window_end_ms"]].astype({"window_end_ms": "int64"})
        feat = cur.merge(feat, on=["segment_id", "window_end_ms"], how="left")
        pdf = pdf.reset_index(drop=True)

        # 1) forecast: GBT where the full 45 s history exists, otherwise 'nothing changes'
        pdf["forecast_score"] = pdf["score"]
        pdf["forecast_source"] = "persistence"
        ok = feat[FORECAST_FEATURES].notna().all(axis=1).to_numpy()
        if self.forecaster is not None and ok.any():
            x = self.spark.createDataFrame(feat.loc[ok, FORECAST_FEATURES].astype("float64"))
            pred = self.forecaster.transform(x).select("prediction").toPandas()["prediction"].to_numpy()
            pdf.loc[ok, "forecast_score"] = pred.clip(0, 1)
            pdf.loc[ok, "forecast_source"] = "gbt"

        # 2) incident detection: abrupt speed collapse together with an occupancy jump
        alerts, flags = [], []
        for i, r in pdf.iterrows():
            seg, f = r["segment_id"], feat.iloc[i]
            drop = f["speed_ratio_l30"] - r["speed_ratio"] if pd.notna(f["speed_ratio_l30"]) else 0.0
            rise = r["occupancy_pct"] - f["occupancy_l30"] if pd.notna(f["occupancy_l30"]) else 0.0
            if not self.alert_active[seg] and drop >= self.ALERT_SPEED_DROP and rise >= self.ALERT_OCC_RISE \
                    and r["speed_ratio"] < self.ALERT_MAX_RATIO:
                self.alert_active[seg] = True
                alerts.append((seg, r["name"], int(r["window_end_ms"]), float(r["speed_ratio"]),
                               float(r["occupancy_pct"]), float(drop), float(rise), int(time.time() * 1000)))
            elif self.alert_active[seg] and r["speed_ratio"] >= self.CLEAR_RATIO:
                self.alert_active[seg] = False
            flags.append(self.alert_active[seg])
        pdf["incident_suspected"] = flags
        return pdf, alerts

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        pdf = batch.toPandas()
        if pdf.empty:
            return
        pdf = pdf.sort_values(["window_end_ms", "segment_id"]).reset_index(drop=True)
        pdf["window_start_ms"] = pdf["window_start_ms"].astype("int64")
        pdf["window_end_ms"] = pdf["window_end_ms"].astype("int64")
        pdf, alerts = self._enrich(pdf)
        pdf["forecast_level"] = pdf["forecast_score"].map(level_from_score)
        pdf["level_name"] = pdf["level"].map(lambda i: LEVEL_NAMES[i])
        pdf["forecast_level_name"] = pdf["forecast_level"].map(lambda i: LEVEL_NAMES[i])
        pdf["est_speed_kmph"] = (pdf["free_flow_kmph"] * (1.0 - 0.8 * pdf["score"])).round(1)
        pdf["emitted_ms"] = int(time.time() * 1000)
        pdf["batch_id"] = int(batch_id)
        for c in ["avg_speed_kmph", "speed_ratio", "occupancy_pct", "veh_per_lane", "rain_mm_hr",
                  "visibility_km", "confidence", "score", "forecast_score"]:
            pdf[c] = pdf[c].astype("float64").round(3)
        day = dt.datetime.fromtimestamp(int(pdf["window_end_ms"].max()) / 1000, dt.timezone.utc).strftime("%Y-%m-%d")
        pdf["dt"] = day

        cols = [f.name for f in OUT_SCHEMA.fields]
        self._append_csv(pdf[cols])
        records = pdf[cols].astype(object).where(pdf[cols].notna(), None).to_dict("records")   # NaN -> null
        out = self.spark.createDataFrame(records, schema=OUT_SCHEMA)
        self._write_lake(out, day, batch_id)
        self._write_serving(out)
        if alerts:
            self._write_alerts(alerts)
        self._print_table(pdf, batch_id, time.time() - t0)

    @staticmethod
    def _append_csv(df: pd.DataFrame) -> None:
        """Flat CSV for the R analytics (ggplot2 / Shiny read this). Duplicates after a replay are
        harmless: the R code keeps the last row per (segment, window)."""
        os.makedirs(config.EXPORT_DIR, exist_ok=True)
        path = os.path.join(config.EXPORT_DIR, "congestion.csv")
        df.to_csv(path, mode="a", header=not os.path.exists(path), index=False)

    def _write_alerts(self, alerts: list) -> None:
        a = self.spark.createDataFrame(alerts, ALERT_SCHEMA)
        for r in a.collect():
            print(f"\n!!! INCIDENT SUSPECTED on {r['segment_id']} ({r['name']}): speed ratio dropped "
                  f"{r['speed_drop']:.2f}, occupancy +{r['occupancy_rise']:.0f} points", flush=True)
        payload = a.select(F.col("segment_id").alias("key"), F.to_json(F.struct(*[F.col(c) for c in a.columns])).alias("value"))
        if SINK == "kafka":
            (payload.write.format("kafka").option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
             .option("topic", config.TOPIC_ALERTS).save())
        else:
            payload.select("value").write.mode("append").text(FILES_OUT + "_alerts")

    # Parquet "data lake" (stands in for HDFS). One folder per batch id -> if Spark ever replays
    # a batch after a failure it simply overwrites the same folder: the sink is idempotent.
    def _write_lake(self, out: DataFrame, day: str, batch_id: int) -> None:
        out.drop("dt", "batch_id").coalesce(1).write.mode("overwrite").parquet(
            os.path.join(config.LAKE_DIR, f"dt={day}", f"batch_id={int(batch_id):08d}"))

    def _write_serving(self, out: DataFrame) -> None:
        payload = out.select(F.col("segment_id").alias("key"),
                             F.to_json(F.struct(*[F.col(c) for c in out.columns])).alias("value"))
        if SINK == "kafka":
            (payload.write.format("kafka").option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
             .option("topic", config.TOPIC_OUT).save())
        else:
            payload.select("value").write.mode("append").text(FILES_OUT)

    @staticmethod
    def _print_table(pdf: pd.DataFrame, batch_id: int, secs: float) -> None:
        latest = pdf[pdf["window_end_ms"] == pdf["window_end_ms"].max()].sort_values("score", ascending=False)
        end = dt.datetime.fromtimestamp(latest["window_end_ms"].iloc[0] / 1000, dt.timezone.utc)
        lag = time.time() - latest["window_end_ms"].iloc[0] / 1000
        print("\n" + "=" * 108)
        print(f"Batch {batch_id}  |  window ending {end:%Y-%m-%d %H:%M:%S} UTC  |  "
              f"{len(pdf)} window rows  |  result is {lag:4.1f}s old  |  batch took {secs:.1f}s")
        print("=" * 108)
        show = pd.DataFrame({
            "segment": latest["segment_id"], "road": latest["name"],
            "gps": latest["gps_pings"], "speed km/h": latest["avg_speed_kmph"].round(1),
            "occ %": latest["occupancy_pct"].round(0), "rain": latest["rain_mm_hr"].round(1),
            "NOW": latest["level_name"], "conf": (latest["confidence"] * 100).round(0).astype(int).astype(str) + "%",
            "in 60s": latest["forecast_level_name"],
            "alert": latest["incident_suspected"].map(lambda b: "<< INCIDENT?" if b else ""),
        })
        print(show.to_string(index=False), flush=True)


# ----------------------------------------------------------------------------- main
def build_query_df(spark: SparkSession, model: PipelineModel) -> DataFrame:
    return predict(add_features(window_aggregate(parse_events(read_raw(spark), spark)), spark), model)


def main() -> int:
    spark = get_spark()
    model = ensure_model(spark)
    forecaster = ensure_forecaster(spark)
    result = build_query_df(spark, model)
    os.makedirs(config.CHECKPOINT_DIR, exist_ok=True)
    writer = (result.writeStream.outputMode("append")
              .foreachBatch(BatchProcessor(spark, forecaster))
              .option("checkpointLocation", os.path.join(config.CHECKPOINT_DIR, "congestion_v1")))
    writer = writer.trigger(availableNow=True) if AVAILABLE_NOW else writer.trigger(processingTime=f"{config.TRIGGER_SECONDS} seconds")
    print(f"[job] source={SOURCE} sink={SINK} window={config.WINDOW_DURATION}/{config.SLIDE_DURATION} "
          f"watermark={config.WATERMARK} trigger={config.TRIGGER_SECONDS}s", flush=True)
    query = writer.start()
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
