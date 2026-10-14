"""Live inputs for the Mumbai pipeline.

What is measured, and what is not:

* TomTom's flow API returns the speed on the road nearest a point. The free
  account allows about 2500 calls a day, so this process samples 36 spread-out
  anchor points and never more than one new call every 40 seconds. A restart
  reuses samples younger than 20 minutes.
* A segment of the same named road within 4 km inherits that sample's
  speed ratio. The dashboard labels this "same road", not a fresh measurement.
* Every other street keeps a cautious urban speed (75% of free-flow) until a
  sample exists. Those streets are drawn pale, not as measured congestion.
* GPS and sensor topics carry one probe per segment. The speed is the value
  above. Occupancy is not a loop detector; it is the Greenshields speed-density
  relationship applied to that speed. The event says so.
* Weather is the current Open-Meteo observation for Mumbai, refreshed every
  5 minutes. Spark is what turns these three topics into a congestion score.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import requests
import urllib3
from dotenv import load_dotenv
from kafka import KafkaProducer

from mapyourway import config
from mapyourway.network import ANCHOR_IDS, SEGMENT_BY_ID, SEGMENTS

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

POLL_SECONDS = 40
BURST_SECONDS = 3
BURST_CALLS = 12
PUBLISH_SECONDS = 10
WEATHER_SECONDS = 300
CACHE_MAX_AGE = 20 * 60
DAILY_CALL_BUDGET = 2400
URBAN_RATIO = 0.75

OPEN_METEO = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=19.0760&longitude=72.8777"
    "&current=temperature_2m,relative_humidity_2m,precipitation,rain,visibility"
    "&timezone=Asia%2FKolkata"
)

_ssl_fallback = False


def http_get(url: str, timeout: int = 15) -> requests.Response:
    """The lab network sometimes intercepts TLS. Fall back once, and say so."""
    global _ssl_fallback
    try:
        if not _ssl_fallback:
            return requests.get(url, timeout=timeout)
    except requests.exceptions.SSLError:
        _ssl_fallback = True
        print("[warn] TLS certificate check failed; retrying without verification", flush=True)
    return requests.get(url, timeout=timeout, verify=False)


def _today() -> str:
    return datetime.now().astimezone().date().isoformat()


def load_cache() -> dict:
    path = config.TOMTOM_CACHE_JSON
    if not os.path.exists(path):
        return {"samples": {}, "calls": {}}
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {"samples": {}, "calls": {}}
    data.setdefault("samples", {})
    data.setdefault("calls", {})
    return data


def save_cache(cache: dict) -> None:
    os.makedirs(os.path.dirname(config.TOMTOM_CACHE_JSON), exist_ok=True)
    temporary = config.TOMTOM_CACHE_JSON + ".tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(cache, handle)
    os.replace(temporary, config.TOMTOM_CACHE_JSON)


def calls_today(cache: dict) -> int:
    return int(cache.get("calls", {}).get(_today(), 0))


def create_producer() -> KafkaProducer:
    for attempt in range(30):
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda key: key.encode("utf-8") if key else None,
                acks=1,
                linger_ms=200,
                retries=3,
            )
            print(f"[info] Kafka connected at {config.KAFKA_BOOTSTRAP}", flush=True)
            return producer
        except Exception as exc:  # noqa: BLE001 - retry any broker-not-ready error
            print(f"[warn] Kafka attempt {attempt + 1}/30 failed: {exc}", flush=True)
            time.sleep(2)
    raise RuntimeError("Kafka did not become reachable")


def sample_anchor(segment: dict, api_key: str) -> dict | None:
    url = (
        "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        f"?key={api_key}&point={segment['mid_lat']},{segment['mid_lon']}"
    )
    response = http_get(url, timeout=12)
    if response.status_code != 200:
        print(f"[warn] TomTom HTTP {response.status_code} for {segment['name']}", flush=True)
        return {"error": str(response.status_code)}
    flow = response.json().get("flowSegmentData") or {}
    current = flow.get("currentSpeed")
    free = flow.get("freeFlowSpeed") or segment["free_flow_kmph"]
    if current is None:
        return None
    return {
        "speed_kmph": float(current),
        "free_flow_kmph": float(free) if float(free) > 0 else float(segment["free_flow_kmph"]),
        "fetched_at": time.time(),
        "name": segment["name"],
    }


def speed_for(segment: dict, cache: dict, now: float) -> tuple[float, str, float | None]:
    """Return speed km/h, source label, and age in seconds of the TomTom sample."""
    samples = cache["samples"]
    inherit = segment.get("inherit") or "urban_default"
    anchor_id = segment.get("anchor_segment_id")
    if inherit in {"direct", "same_road"} and anchor_id in samples:
        sample = samples[anchor_id]
        ratio = float(sample["speed_kmph"]) / max(float(sample["free_flow_kmph"]), 1.0)
        ratio = max(0.08, min(ratio, 1.25))
        speed = max(5.0, segment["free_flow_kmph"] * ratio)
        source = "tomtom_direct" if inherit == "direct" else "tomtom_same_road"
        return speed, source, now - float(sample["fetched_at"])
    return max(5.0, segment["free_flow_kmph"] * URBAN_RATIO), "urban_default", None


def fetch_weather() -> dict:
    response = http_get(OPEN_METEO, timeout=15)
    response.raise_for_status()
    current = response.json().get("current") or {}
    visibility_m = current.get("visibility")
    visibility_km = 10.0 if visibility_m is None else max(0.1, float(visibility_m) / 1000.0)
    rain = current.get("rain")
    if rain is None:
        rain = current.get("precipitation") or 0.0
    return {
        "station_id": "OPEN-METEO-MUMBAI",
        "rain_mm_hr": round(float(rain), 2),
        "visibility_km": round(visibility_km, 2),
        "temp_c": round(float(current.get("temperature_2m") or 0.0), 1),
        "humidity_pct": round(float(current.get("relative_humidity_2m") or 0.0), 1),
        "source": "open-meteo",
        "observed_at": current.get("time"),
    }


def greenshields_occupancy(speed: float, free_flow: float) -> float:
    """v = v_free * (1 - occupancy)  =>  occupancy = 1 - v/v_free, as a percent."""
    ratio = speed / max(free_flow, 1.0)
    return round(max(0.0, min(100.0, (1.0 - ratio) * 100.0)), 1)


def publish_cycle(producer: KafkaProducer, cache: dict, weather: dict) -> None:
    now_ms = int(time.time() * 1000)
    now = time.time()
    for segment in SEGMENTS:
        speed, source, _age = speed_for(segment, cache, now)
        # Unmeasured streets stay at free-flow inside the router. Publishing all
        # 11,000 of them every 10 s would bury the measured corridors.
        if source == "urban_default":
            continue
        producer.send(
            config.TOPIC_GPS,
            key=segment["segment_id"],
            value={
                "vehicle_id": f"PROBE-{segment['segment_id']}",
                "segment_id": segment["segment_id"],
                "lat": segment["mid_lat"],
                "lon": segment["mid_lon"],
                "speed_kmph": round(speed, 1),
                "speed_source": source,
                "ts_ms": now_ms,
            },
        )
        occupancy = greenshields_occupancy(speed, segment["free_flow_kmph"])
        producer.send(
            config.TOPIC_SENSOR,
            key=segment["segment_id"],
            value={
                "sensor_id": f"INFERRED-{segment['segment_id']}",
                "segment_id": segment["segment_id"],
                "occupancy_pct": occupancy,
                "vehicle_count": int(round(segment["lanes"] * (2.0 + 8.0 * occupancy / 100.0))),
                "inference": "greenshields_from_speed",
                "speed_source": source,
                "ts_ms": now_ms,
            },
        )
    producer.send(
        config.TOPIC_WEATHER,
        key="MUMBAI",
        value={**weather, "ts_ms": now_ms},
    )
    producer.flush()


def write_status(cache: dict, weather: dict, last_error: str | None) -> None:
    fresh = 0
    now = time.time()
    for anchor_id in ANCHOR_IDS:
        sample = cache["samples"].get(anchor_id)
        if sample and now - float(sample["fetched_at"]) < CACHE_MAX_AGE:
            fresh += 1
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "anchors_total": len(ANCHOR_IDS),
        "anchors_fresh": fresh,
        "tomtom_calls_today": calls_today(cache),
        "tomtom_daily_budget": DAILY_CALL_BUDGET,
        "segments": len(SEGMENTS),
        "weather": weather,
        "last_error": last_error,
    }
    os.makedirs(os.path.dirname(config.PRODUCER_STATUS_JSON), exist_ok=True)
    temporary = config.PRODUCER_STATUS_JSON + ".tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    os.replace(temporary, config.PRODUCER_STATUS_JSON)


def main() -> None:
    if not SEGMENTS:
        raise SystemExit(
            "The Mumbai network file is missing. Run scripts/fetch_osm_roads.py "
            "and scripts/build_network.py before starting the producer."
        )
    api_key = os.getenv("TOM_TOM_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("TOM_TOM_API_KEY is not set")

    config.ensure_directories()
    producer = create_producer()
    cache = load_cache()
    weather = {
        "station_id": "OPEN-METEO-MUMBAI",
        "rain_mm_hr": 0.0,
        "visibility_km": 10.0,
        "temp_c": None,
        "humidity_pct": None,
        "source": "waiting",
        "observed_at": None,
    }
    print(
        f"[info] {len(SEGMENTS)} segments, {len(ANCHOR_IDS)} TomTom anchors, "
        f"budget {DAILY_CALL_BUDGET}/day, already used today {calls_today(cache)}",
        flush=True,
    )

    anchor_cursor = 0
    calls_this_process = 0
    next_tomtom = time.time()
    next_weather = time.time()
    next_publish = time.time()
    last_error = None
    try:
        while True:
            now = time.time()
            if now >= next_weather:
                try:
                    weather = fetch_weather()
                    last_error = None
                    print(
                        f"[weather] {weather['temp_c']} C, rain {weather['rain_mm_hr']} mm, "
                        f"visibility {weather['visibility_km']} km",
                        flush=True,
                    )
                except Exception as exc:  # noqa: BLE001 - keep the last good observation
                    last_error = f"weather: {exc}"
                    print(f"[warn] weather fetch failed: {exc}", flush=True)
                next_weather = time.time() + WEATHER_SECONDS

            if now >= next_tomtom and ANCHOR_IDS:
                anchor_id = ANCHOR_IDS[anchor_cursor % len(ANCHOR_IDS)]
                anchor_cursor += 1
                sample = cache["samples"].get(anchor_id)
                stale = sample is None or now - float(sample["fetched_at"]) >= CACHE_MAX_AGE
                gap = BURST_SECONDS if calls_this_process < BURST_CALLS else POLL_SECONDS
                if not stale:
                    next_tomtom = time.time() + 1
                elif calls_today(cache) >= DAILY_CALL_BUDGET:
                    print("[warn] TomTom daily budget reached; keeping cached speeds", flush=True)
                    next_tomtom = time.time() + 300
                else:
                    segment = SEGMENT_BY_ID[anchor_id]
                    try:
                        measured = sample_anchor(segment, api_key)
                    except Exception as exc:  # noqa: BLE001
                        measured = None
                        last_error = f"tomtom: {exc}"
                        print(f"[warn] TomTom call failed: {exc}", flush=True)
                    if measured and "error" in measured:
                        if measured["error"] == "429":
                            gap = 90
                    elif measured:
                        cache["samples"][anchor_id] = measured
                        cache["calls"][_today()] = calls_today(cache) + 1
                        calls_this_process += 1
                        save_cache(cache)
                        print(
                            f"[tomtom] {segment['name'][:42]:42} "
                            f"{measured['speed_kmph']:.0f}/{measured['free_flow_kmph']:.0f} km/h "
                            f"({calls_today(cache)}/{DAILY_CALL_BUDGET} today)",
                            flush=True,
                        )
                    next_tomtom = time.time() + gap

            if now >= next_publish:
                publish_cycle(producer, cache, weather)
                write_status(cache, weather, last_error)
                next_publish = time.time() + PUBLISH_SECONDS

            time.sleep(0.4)
    except KeyboardInterrupt:
        print("[info] producer stopped", flush=True)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
