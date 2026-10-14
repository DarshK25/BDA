"""One Spark micro-batch: GPS + sensors + weather -> congestion, routes, lake.

Spark hands this function the rows that arrived since the previous trigger
(every 10 seconds). The arithmetic is ordinary and visible on purpose. A
congestion score is a weighted mix of how far speed has fallen below free flow
and how full the Greenshields occupancy says the road is. Rain and fog from
the weather topic scale the speed down before that score is computed.
"""
from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timezone

import pandas as pd

from mapyourway import config
from mapyourway.ml.forecaster import NextStepModel, features_of
from mapyourway.network import PLACE_BY_NAME, SEGMENTS
from mapyourway.routing import compare

HEADLINE_TRIPS = [
    ("Borivali", "Colaba"),
    ("Andheri", "Nariman Point"),
    ("Bandra", "Ghatkopar"),
    ("Dadar", "BKC"),
]

_model = NextStepModel()
_previous: dict[str, tuple[list[float], float]] = {}
_weather = {
    "rain_mm_hr": 0.0,
    "visibility_km": 10.0,
    "temp_c": None,
    "humidity_pct": None,
    "source": "none_yet",
    "observed_at": None,
}
_severe: set[str] = set()


def _weather_factor(rain: float, visibility_km: float) -> float:
    wet = min(max(rain, 0.0), 25.0) / 25.0
    fog = (10.0 - min(max(visibility_km, 0.0), 10.0)) / 10.0
    return 0.7 * wet + 0.3 * fog


def _level(score: float) -> str:
    if score < 0.30:
        return "low"
    if score < 0.55:
        return "medium"
    if score < 0.75:
        return "high"
    return "severe"


def _write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temporary = path + ".tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    os.replace(temporary, path)


def _routes(speeds: dict[str, float]) -> list[dict]:
    routes = []
    for origin, dest in HEADLINE_TRIPS:
        if origin not in PLACE_BY_NAME or dest not in PLACE_BY_NAME:
            continue
        try:
            result = compare(origin, dest, speeds)
        except Exception as exc:  # noqa: BLE001 - one disconnected pair should not stop the batch
            routes.append({"origin": origin, "destination": dest, "error": str(exc)})
            continue
        routes.append(
            {
                "origin": origin,
                "destination": dest,
                "usual_min": round(result["usual"].minutes, 1),
                "best_min": round(result["best"].minutes, 1),
                "saving_min": round(result["saving_min"], 1),
                "reroute": bool(result["reroute"]),
                "usual_label": result["usual"].label(),
                "best_label": result["best"].label(),
                "best_segment_ids": result["best"].segment_ids,
                "usual_segment_ids": result["usual"].segment_ids,
                "expanded_astar": result["expanded_astar"],
                "expanded_dijkstra": result["expanded_dijkstra"],
            }
        )
    return routes


def _publish_kafka(batch_df, rows: list[dict], routes: list[dict], incidents: list[dict]) -> None:
    spark = batch_df.sparkSession
    from pyspark.sql import Row

    def _send(topic: str, key_field: str, payload: list[dict]) -> None:
        if not payload:
            return
        frame = spark.createDataFrame([Row(key=item[key_field], value=json.dumps(item)) for item in payload])
        (
            frame.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
            .write.format("kafka")
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
            .option("topic", topic)
            .save()
        )

    _send(config.TOPIC_CONGESTION, "segment_id", rows)
    _send(config.TOPIC_ROUTES, "origin", [r for r in routes if "error" not in r])
    _send(config.TOPIC_INCIDENTS, "segment_id", incidents)


def process_batch(batch_df, epoch_id: int) -> None:
    global _weather, _severe
    config.ensure_directories()
    frame = batch_df.selectExpr("CAST(topic AS STRING) AS topic", "CAST(value AS STRING) AS value").toPandas()
    if frame.empty:
        print(f"[spark] epoch {epoch_id}: no new events", flush=True)
        return

    gps_speed: dict[str, list[float]] = {}
    gps_source: dict[str, str] = {}
    occupancy: dict[str, list[float]] = {}
    vehicles: dict[str, list[float]] = {}
    ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    for topic, raw in zip(frame["topic"], frame["value"]):
        try:
            event = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            continue
        if event.get("ts_ms"):
            ts_ms = int(event["ts_ms"])
        if topic == config.TOPIC_GPS:
            segment_id = event.get("segment_id")
            if not segment_id or event.get("speed_kmph") is None:
                continue
            gps_speed.setdefault(segment_id, []).append(float(event["speed_kmph"]))
            gps_source[segment_id] = event.get("speed_source") or "unknown"
        elif topic == config.TOPIC_SENSOR:
            segment_id = event.get("segment_id")
            if not segment_id:
                continue
            if event.get("occupancy_pct") is not None:
                occupancy.setdefault(segment_id, []).append(float(event["occupancy_pct"]))
            if event.get("vehicle_count") is not None:
                vehicles.setdefault(segment_id, []).append(float(event["vehicle_count"]))
        elif topic == config.TOPIC_WEATHER:
            _weather = {
                "rain_mm_hr": float(event.get("rain_mm_hr") or 0.0),
                "visibility_km": float(event.get("visibility_km") or 10.0),
                "temp_c": event.get("temp_c"),
                "humidity_pct": event.get("humidity_pct"),
                "source": event.get("source") or "open-meteo",
                "observed_at": event.get("observed_at"),
            }

    factor = _weather_factor(_weather["rain_mm_hr"], _weather["visibility_km"])
    rows = []
    speeds: dict[str, float] = {}
    now_severe: set[str] = set()
    for segment in SEGMENTS:
        segment_id = segment["segment_id"]
        if segment_id not in gps_speed:
            continue
        measured = sum(gps_speed[segment_id]) / len(gps_speed[segment_id])
        adjusted = max(5.0, measured * (1.0 - 0.35 * factor))
        free_flow = float(segment["free_flow_kmph"])
        speed_component = 1.0 - min(1.0, adjusted / max(free_flow, 1.0))
        occ_values = occupancy.get(segment_id) or [max(0.0, (1.0 - measured / max(free_flow, 1.0)) * 100.0)]
        occ = sum(occ_values) / len(occ_values)
        score = max(0.0, min(1.0, 0.75 * speed_component + 0.25 * (occ / 100.0)))
        level = _level(score)
        source = gps_source.get(segment_id, "unknown")
        feature = features_of(adjusted, occ, _weather["rain_mm_hr"], ts_ms)
        if segment_id in _previous:
            _model.observe(_previous[segment_id][0], score)
        forecast = _model.predict(feature, score)
        _previous[segment_id] = (feature, score)
        speeds[segment_id] = adjusted
        if level == "severe" and source != "urban_default":
            now_severe.add(segment_id)
        rows.append(
            {
                "segment_id": segment_id,
                "name": segment["name"],
                "road_class": segment["road_class"],
                "measured_speed_kmph": round(measured, 1),
                "speed_kmph": round(adjusted, 1),
                "free_flow_kmph": free_flow,
                "occupancy_pct": round(occ, 1),
                "vehicle_count": round(sum(vehicles.get(segment_id, [0])) / max(len(vehicles.get(segment_id, [1])), 1)),
                "congestion": round(score, 3),
                "forecast": round(forecast, 3),
                "level": level,
                "speed_source": source,
                "probes": len(gps_speed[segment_id]),
                "ts_ms": ts_ms,
            }
        )

    _model.maybe_train()
    incidents = [
        {
            "segment_id": row["segment_id"],
            "name": row["name"],
            "congestion": row["congestion"],
            "level": "severe",
            "kind": "congestion",
            "ts_ms": ts_ms,
        }
        for row in rows
        if row["segment_id"] in now_severe and row["segment_id"] not in _severe
    ]
    _severe = now_severe
    routes = _routes(speeds) if speeds else []

    snapshot = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "epoch": epoch_id,
        "weather": _weather,
        "weather_factor": round(factor, 3),
        "model": _model.status(),
        "segments": {
            row["segment_id"]: {key: value for key, value in row.items() if key != "segment_id"}
            for row in rows
        },
        "routes": routes,
        "new_severe": [item["segment_id"] for item in incidents],
    }
    _write_json(config.LATEST_JSON, snapshot)

    if rows:
        lake = pd.DataFrame(rows)
        lake["epoch"] = epoch_id
        lake["rain_mm_hr"] = _weather["rain_mm_hr"]
        lake_dir = config.CONGESTION_LAKE
        os.makedirs(lake_dir, exist_ok=True)
        lake.to_parquet(os.path.join(lake_dir, f"epoch-{epoch_id:06d}.parquet"), index=False)

    try:
        _publish_kafka(batch_df, rows, routes, incidents)
    except Exception:
        print("[spark] Kafka sink failed; the dashboard file was still written", flush=True)
        traceback.print_exc()

    severe = sum(1 for row in rows if row["level"] == "severe" and row["speed_source"] != "urban_default")
    reroutes = [r for r in routes if r.get("reroute")]
    reroute_text = reroutes[0]["best_label"] if reroutes else "none"
    print(
        f"[spark] epoch {epoch_id}: {len(rows)} segments, {severe} severe, "
        f"rain {_weather['rain_mm_hr']} mm, reroute {reroute_text}",
        flush=True,
    )
