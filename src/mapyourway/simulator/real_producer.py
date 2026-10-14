"""
Real-time TomTom Traffic Data Producer for MapYourWay Mumbai
Fetches live traffic data from TomTom API and streams to Kafka

This replaces the synthetic simulator with actual Mumbai traffic data.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Iterable, Optional

import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

from mapyourway.network import SEGMENTS, point_along_segment
from mapyourway import config

logger = logging.getLogger(__name__)

TRAFFIC_FLOW_URL = (
    "https://api.tomtom.com/traffic/services/4/flowSegmentData/"
    "absolute/10/json"
)


def _segment_road_sample(segment: dict) -> tuple[float, float]:
    """Return a TomTom/GPS sample in ``(lat, lon)`` order.

    The shared path-distance helper is the source of truth for the midpoint.
    The checked-in ``road_sample`` metadata is retained for provenance, but it
    must not become a second interpolation implementation or allow stale data
    to put a lookup back on the endpoint chord.
    """
    return point_along_segment(segment, 0.5)


def get_tomtom_api_key() -> str:
    """Load the TomTom key without doing work at module-import time.

    ``TOM_TOM_API_KEY`` is the name already used by this project.  The more
    common ``TOMTOM_API_KEY`` spelling is accepted as well, so the producer
    can be moved between environments without editing code.
    """
    load_dotenv()
    api_key = os.getenv("TOM_TOM_API_KEY") or os.getenv("TOMTOM_API_KEY")
    if not api_key:
        raise ValueError(
            "TomTom API key not found. Set TOM_TOM_API_KEY (or TOMTOM_API_KEY) "
            "in .env or the process environment."
        )
    return api_key


# Initialize Kafka Producer with retry logic
def create_kafka_producer(bootstrap_servers: Optional[str] = None) -> KafkaProducer:
    """Create Kafka producer with connection retry"""
    bootstrap_servers = bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS
    max_retries = 30
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks=1,
                retries=3
            )
            print(f"[INFO] Connected to Kafka at {bootstrap_servers}")
            return producer
        except Exception as e:
            print(f"[WARN] Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}")
            time.sleep(2)

    raise RuntimeError(f"Failed to connect to Kafka after {max_retries} attempts")


class ProbeFleet:
    """Create individual probe-shaped observations from segment-level flow.

    TomTom supplies an aggregate speed for the road fragment nearest a point;
    it does not supply vehicle identities.  This small deterministic fleet is
    therefore explicitly marked as ``derived_probe_from_tomtom_flow``.  It
    gives the streaming layer realistic GPS-shaped records without claiming
    that they are actual private-vehicle traces.
    """

    def __init__(self, vehicles_per_segment: int = 3) -> None:
        self.vehicles_per_segment = max(1, int(vehicles_per_segment))
        self._fractions: dict[tuple[str, int], float] = {}

    def events(
        self,
        segment: dict,
        speed_kmph: float,
        free_speed_kmph: float,
        now_ms: int,
        elapsed_seconds: float,
        confidence: float,
        road_closure: bool = False,
        observed_at_ms: Optional[int] = None,
        current_travel_time_s: int = 0,
        free_flow_travel_time_s: int = 0,
        snapshot_id: Optional[str] = None,
        fetched_at_ms: Optional[int] = None,
    ) -> list[dict]:
        events = []
        distance_fraction = (
            max(speed_kmph, 0.0) * max(elapsed_seconds, 0.0) / 3600.0
        ) / max(float(segment["length_km"]), 0.01)

        for index in range(self.vehicles_per_segment):
            key = (segment["segment_id"], index)
            fraction = self._fractions.setdefault(
                key, (index + 1) / (self.vehicles_per_segment + 1)
            )
            fraction = (fraction + distance_fraction) % 1.0
            self._fractions[key] = fraction
            lat, lon = point_along_segment(segment, fraction)
            events.append(
                {
                    "vehicle_id": f"TOMTOM-PROBE-{segment['segment_id']}-{index + 1:02d}",
                    "segment_id": segment["segment_id"],
                    "lat": round(lat, 6),
                    "lon": round(lon, 6),
                    "speed_kmph": speed_kmph,
                    "free_flow_speed_kmph": free_speed_kmph,
                    "current_travel_time_s": current_travel_time_s,
                    "free_flow_travel_time_s": free_flow_travel_time_s,
                    "confidence": round(confidence, 3),
                    "road_closure": bool(road_closure),
                    "source": "derived_probe_from_tomtom_flow",
                    "source_observation": "tomtom_flow_segment",
                    "snapshot_id": snapshot_id or f"{segment['segment_id']}:{observed_at_ms or now_ms}",
                    "observed_at_ms": observed_at_ms or now_ms,
                    "fetched_at_ms": fetched_at_ms or observed_at_ms or now_ms,
                    "emitted_at_ms": now_ms,
                    "is_cached_observation": now_ms != (fetched_at_ms or observed_at_ms or now_ms),
                    "ts_ms": now_ms,
                }
            )
        return events


def _timestamp_to_ms(value: Optional[str], fallback_ms: int) -> int:
    if not value:
        return fallback_ms
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except (TypeError, ValueError):
        return fallback_ms


def fetch_open_meteo_weather(
    session: Optional[requests.Session] = None,
    now_ms: Optional[int] = None,
) -> dict:
    """Fetch current Mumbai weather from Open-Meteo.

    Open-Meteo is used without an API key.  The values are normalized to the
    weather event contract consumed by Spark.  Visibility is converted from
    metres to kilometres because that is the unit used by the traffic model.
    """
    session = session or requests.Session()
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    response = session.get(
        config.OPEN_METEO_URL,
        params={
            "latitude": config.OPEN_METEO_LATITUDE,
            "longitude": config.OPEN_METEO_LONGITUDE,
            "current": ",".join(
                [
                    "temperature_2m",
                    "relative_humidity_2m",
                    "rain",
                    "visibility",
                    "wind_speed_10m",
                    "weather_code",
                ]
            ),
            "temperature_unit": "celsius",
            "wind_speed_unit": "kmh",
            "precipitation_unit": "mm",
            "timezone": config.OPEN_METEO_TIMEZONE,
        },
        timeout=10,
    )
    response.raise_for_status()
    current = response.json().get("current", {})
    if not current:
        raise ValueError("Open-Meteo response did not include current conditions")

    visibility_value = current.get("visibility")
    visibility_m = 10000.0 if visibility_value is None else float(visibility_value)
    return {
        "station_id": "MUMBAI-OPEN-METEO",
        "rain_mm_hr": float(current.get("rain", 0.0) or 0.0),
        "visibility_km": round(max(0.0, visibility_m / 1000.0), 3),
        "temp_c": float(current.get("temperature_2m", 0.0) or 0.0),
        "humidity_pct": float(current.get("relative_humidity_2m", 0.0) or 0.0),
        "wind_speed_kmph": float(current.get("wind_speed_10m", 0.0) or 0.0),
        "weather_code": int(current.get("weather_code", 0) or 0),
        "observed_at_ms": _timestamp_to_ms(current.get("time"), now_ms),
        "source": "open_meteo_current",
        "ts_ms": now_ms,
    }


def fetch_and_publish_traffic(
    producer: KafkaProducer,
    session: Optional[requests.Session] = None,
    api_key: Optional[str] = None,
    segments: Optional[Iterable[dict]] = None,
    now_ms: Optional[int] = None,
    probe_fleet: Optional[ProbeFleet] = None,
    movement_seconds: float = 0.0,
    snapshot_cache: Optional[dict[str, dict]] = None,
) -> tuple[int, int]:
    """
    Fetch live traffic data from TomTom API for all Mumbai segments
    and publish to Kafka topics (gps_events and sensor_events)

    The producer is passed in rather than created globally.  This keeps
    importing the module safe for tests and for tools that only inspect the
    network schema.

    Returns:
        ``(successful_segments, failed_segments)`` for the poll.
    """
    api_key = api_key or get_tomtom_api_key()
    session = session or requests.Session()
    segments = list(segments or SEGMENTS)
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

    print(f"\n[{timestamp}] Fetching live traffic from TomTom for {len(segments)} Mumbai segments...")

    success_count = 0
    error_count = 0

    for seg in segments:
        try:
            # Sample the fetched driving path, not the chord between named
            # junction anchors.  TomTom needs latitude,longitude order.
            lat, lon = _segment_road_sample(seg)
            lat = round(lat, 6)
            lon = round(lon, 6)

            # TomTom Flow Segment Data API
            response = session.get(
                TRAFFIC_FLOW_URL,
                params={"key": api_key, "point": f"{lat},{lon}"},
                timeout=10,
            )
            
            if response.status_code == 200:
                data = response.json()
                flow = data.get("flowSegmentData", {})
                if not flow:
                    raise ValueError("TomTom response did not include flowSegmentData")
                
                # Extract live traffic data
                if flow.get("currentSpeed") is None or flow.get("freeFlowSpeed") is None:
                    raise ValueError("TomTom flow response is missing speed fields")
                current_speed = float(flow["currentSpeed"])
                free_speed = float(flow["freeFlowSpeed"])
                if not (0.0 <= current_speed <= 150.0 and 0.0 < free_speed <= 150.0):
                    raise ValueError(
                        f"TomTom returned invalid speeds: {current_speed}/{free_speed}"
                    )
                current_travel_time = int(flow.get("currentTravelTime", 0))
                free_flow_travel_time = int(flow.get("freeFlowTravelTime", 0))
                confidence = float(flow.get("confidence", 0.8))
                
                # Calculate congestion metrics
                speed_ratio = current_speed / max(free_speed, 1.0)
                
                # Occupancy estimation: based on speed reduction
                # When speed is 50% of free-flow, occupancy is ~60%
                occupancy = max(
                    0.0,
                    min(100.0, (1.0 - speed_ratio) * config.OCCUPANCY_SPEED_FACTOR),
                )
                
                # TomTom's flow endpoint sometimes sets roadClosure=true on a
                # fragment that still reports normal flow (e.g. currentSpeed
                # equal to freeFlowSpeed).  Treat it as a real closure only when
                # the observed speed shows there is effectively no movement;
                # otherwise a healthy road would be wrongly removed from routing.
                road_closed = bool(flow.get("roadClosure", False)) and current_speed <= config.CLOSURE_MAX_SPEED_KMPH

                snapshot = {
                    "current_speed": current_speed,
                    "free_speed": free_speed,
                    "current_travel_time": current_travel_time,
                    "free_flow_travel_time": free_flow_travel_time,
                    "confidence": max(0.0, min(1.0, confidence)),
                    "road_closure": road_closed,
                    # The Flow Segment response has no universally available
                    # observation timestamp.  The request completion time is
                    # the honest lower bound and is retained separately from
                    # Kafka delivery/emission time.
                    "observed_at_ms": now_ms,
                    "fetched_at_ms": now_ms,
                    "snapshot_id": f"{seg['segment_id']}:{now_ms}",
                    "source": "tomtom_flow_segment",
                }
                if snapshot_cache is not None:
                    snapshot_cache[seg["segment_id"]] = snapshot
                publish_snapshot_events(
                    producer,
                    seg,
                    snapshot,
                    now_ms,
                    probe_fleet,
                    movement_seconds,
                )

                success_count += 1
                
                # Log interesting segments (congested or very fast)
                if occupancy > 70 or speed_ratio < 0.5:
                    print(f"  🔴 {seg['name']:20s} | Speed: {current_speed:4.0f}/{free_speed:.0f} km/h | Occupancy: {occupancy:5.1f}%")
                elif occupancy < 20:
                    print(f"  🟢 {seg['name']:20s} | Speed: {current_speed:4.0f}/{free_speed:.0f} km/h | Occupancy: {occupancy:5.1f}%")

            elif response.status_code == 403:
                print("  ❌ API Key issue (403) - check your TomTom key validity")
                error_count += 1
            elif response.status_code == 429:
                print("  ⚠️  Rate limit exceeded (429) - waiting before retry...")
                time.sleep(5)
                error_count += 1
            else:
                print(f"  ⚠️  {seg['segment_id']}: HTTP {response.status_code}")
                error_count += 1

        except requests.exceptions.Timeout:
            print(f"  ⏱️  Timeout for {seg['segment_id']}")
            error_count += 1
        except Exception as e:
            print(f"  ❌ Error for {seg['segment_id']}: {e}")
            error_count += 1

    producer.flush()

    print(f"[{time.strftime('%H:%M:%S')}] ✅ Published {success_count} segments | ❌ {error_count} errors")
    return success_count, error_count


def snapshot_is_fresh(snapshot: dict, now_ms: int) -> bool:
    """Return whether a cached TomTom observation may be re-emitted."""
    observed_at = int(snapshot.get("observed_at_ms") or 0)
    return bool(observed_at) and now_ms - observed_at <= config.TRAFFIC_SOURCE_TTL_SECONDS * 1000


def publish_snapshot_events(
    producer: KafkaProducer,
    segment: dict,
    snapshot: dict,
    now_ms: int,
    probe_fleet: Optional[ProbeFleet],
    movement_seconds: float,
) -> bool:
    """Publish GPS-shaped and sensor-shaped observations from one snapshot.

    A cached source observation gets a new Kafka timestamp so the stream stays
    alive, but its original observation time and snapshot ID remain unchanged.
    Once the source TTL expires it is not replayed as if it were live.
    """
    if not snapshot_is_fresh(snapshot, now_ms):
        logger.warning("Skipping stale TomTom snapshot for %s", segment["segment_id"])
        return False
    current_speed = float(snapshot["current_speed"])
    free_speed = float(snapshot["free_speed"])
    confidence = float(snapshot.get("confidence", 0.0))
    speed_ratio = current_speed / max(free_speed, 1.0)
    occupancy = max(
        0.0,
        min(100.0, (1.0 - speed_ratio) * config.OCCUPANCY_SPEED_FACTOR),
    )
    vehicle_count = round(
        max(
            0,
            segment["lanes"]
            * (
                config.BASE_VEHICLES_PER_LANE
                + config.MAX_ADDITIONAL_VEHICLES_PER_LANE * (occupancy / 100.0)
            ),
        )
    )
    sensor_payload = {
        "sensor_id": f"DERIVED-IOT-{segment['segment_id']}",
        "segment_id": segment["segment_id"],
        "occupancy_pct": round(occupancy, 1),
        "vehicle_count": vehicle_count,
        "confidence": round(confidence, 3),
        "road_closure": bool(snapshot.get("road_closure", False)),
        "source": "derived_sensor_from_tomtom_flow",
        "source_observation": "tomtom_flow_segment",
        "snapshot_id": snapshot.get("snapshot_id"),
        "observed_at_ms": int(snapshot.get("observed_at_ms", now_ms)),
        "fetched_at_ms": int(snapshot.get("fetched_at_ms", now_ms)),
        "emitted_at_ms": now_ms,
        "is_cached_observation": now_ms != int(snapshot.get("fetched_at_ms", now_ms)),
        "ts_ms": now_ms,
    }
    producer.send(
        config.TOPIC_SENSOR,
        key=segment["segment_id"],
        value=sensor_payload,
    )

    if probe_fleet is None:
        lat, lon = _segment_road_sample(segment)
        gps_events = [
            {
                "vehicle_id": f"TOMTOM-AGG-{segment['segment_id']}",
                "segment_id": segment["segment_id"],
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "speed_kmph": current_speed,
                "free_flow_speed_kmph": free_speed,
                "current_travel_time_s": snapshot["current_travel_time"],
                "free_flow_travel_time_s": snapshot["free_flow_travel_time"],
                "confidence": round(confidence, 3),
                "road_closure": bool(snapshot.get("road_closure", False)),
                "source": "tomtom_flow_segment_aggregate",
                "source_observation": "tomtom_flow_segment",
                "snapshot_id": snapshot.get("snapshot_id"),
                "observed_at_ms": int(snapshot.get("observed_at_ms", now_ms)),
                "fetched_at_ms": int(snapshot.get("fetched_at_ms", now_ms)),
                "emitted_at_ms": now_ms,
                "is_cached_observation": now_ms != int(snapshot.get("fetched_at_ms", now_ms)),
                "ts_ms": now_ms,
            }
        ]
    else:
        gps_events = probe_fleet.events(
            segment,
            current_speed,
            free_speed,
            now_ms,
            movement_seconds,
            confidence,
            bool(snapshot.get("road_closure", False)),
            int(snapshot.get("observed_at_ms", now_ms)),
            int(snapshot.get("current_travel_time", 0)),
            int(snapshot.get("free_flow_travel_time", 0)),
            snapshot.get("snapshot_id"),
            int(snapshot.get("fetched_at_ms", now_ms)),
        )
    for gps_payload in gps_events:
        producer.send(
            config.TOPIC_GPS,
            key=segment["segment_id"],
            value=gps_payload,
        )
    return True


def publish_weather_data(
    producer: KafkaProducer,
    session: Optional[requests.Session] = None,
) -> bool:
    """Fetch and publish current Mumbai weather; return whether it succeeded."""
    try:
        weather_payload = fetch_open_meteo_weather(session=session)
        producer.send(config.TOPIC_WEATHER, key="MUMBAI", value=weather_payload)
        producer.flush()
        print(
            "[weather] Open-Meteo: "
            f"rain={weather_payload['rain_mm_hr']:.2f} mm/h, "
            f"visibility={weather_payload['visibility_km']:.1f} km"
        )
        return True
    except Exception as exc:
        logger.warning("Open-Meteo weather fetch failed: %s", exc)
        return False


def publish_weather_snapshot(
    producer: KafkaProducer, weather_payload: dict, emitted_at_ms: Optional[int] = None
) -> bool:
    """Republish the latest weather observation with a fresh event timestamp.

    The source observation time remains in ``observed_at_ms``.  The fresh
    ``ts_ms`` keeps the event-time stream supplied while the weather API is
    polled at its much slower, quota-friendly interval.
    """
    payload = dict(weather_payload)
    emitted_at_ms = int(time.time() * 1000) if emitted_at_ms is None else emitted_at_ms
    observed_at_ms = int(payload.get("observed_at_ms") or 0)
    if (
        not observed_at_ms
        or emitted_at_ms - observed_at_ms > config.WEATHER_SOURCE_TTL_SECONDS * 1000
    ):
        logger.warning("Skipping stale Open-Meteo observation")
        return False
    payload["ts_ms"] = emitted_at_ms
    payload["is_cached_observation"] = payload["ts_ms"] != weather_payload.get("ts_ms")
    producer.send(config.TOPIC_WEATHER, key="MUMBAI", value=payload)
    return True


def main():
    """Main producer loop"""
    # The Flow Segment endpoint is one request per segment.  The default is
    # intentionally configurable because the request allowance depends on the
    # TomTom plan.  For this 19-segment network, the estimate is:
    #     19 * 86400 / interval  requests per day.
    poll_interval = config.TOMTOM_POLL_INTERVAL_SECONDS
    emit_interval = config.TRAFFIC_EMIT_INTERVAL_SECONDS
    weather_interval = max(
        60, int(os.getenv("OPEN_METEO_POLL_INTERVAL_SECONDS", "300"))
    )
    producer = None
    session = requests.Session()
    probe_fleet = ProbeFleet(config.SIMULATOR_CONFIG["probe_vehicles_per_segment"])
    snapshot_cache: dict[str, dict] = {}
    weather_cache: Optional[dict] = None
    segment_by_id = {segment["segment_id"]: segment for segment in SEGMENTS}
    
    print("\n" + "=" * 70)
    print("  MapYourWay - Real-Time Mumbai Traffic Producer")
    print("  Traffic source: TomTom Flow Segment API")
    print("  Probe source: derived probe fleet from TomTom segment speeds")
    print("  Weather source: Open-Meteo current conditions")
    print("  Network: 19 segments across Western & Central Lines")
    print(
        f"  Poll interval: {poll_interval}s "
        f"(~{len(SEGMENTS) * 86400 / poll_interval:,.0f} requests/day)"
    )
    print(f"  Event emission interval: {emit_interval}s")
    print("=" * 70)

    try:
        # Validate the key before opening a long-running Kafka connection.
        get_tomtom_api_key()
        producer = create_kafka_producer()

        try:
            weather_cache = fetch_open_meteo_weather(session=session)
            publish_weather_snapshot(producer, weather_cache)
            producer.flush()
        except Exception as exc:
            logger.warning("Initial Open-Meteo fetch failed: %s", exc)

        iteration = 0
        last_weather_at = time.monotonic()
        last_poll_at: Optional[float] = None
        last_emit_at = time.monotonic()
        while True:
            iteration += 1
            now = time.monotonic()
            if last_poll_at is None or now - last_poll_at >= poll_interval:
                # Advance the schedule before making the request.  A failed
                # request must not spin at API speed while the cache is empty.
                last_poll_at = now
                fetch_and_publish_traffic(
                    producer,
                    session=session,
                    probe_fleet=probe_fleet,
                    snapshot_cache=snapshot_cache,
                )

            if snapshot_cache and now - last_emit_at >= emit_interval:
                elapsed = now - last_emit_at
                emitted_at = int(time.time() * 1000)
                for segment_id, snapshot in snapshot_cache.items():
                    publish_snapshot_events(
                        producer,
                        segment_by_id[segment_id],
                        snapshot,
                        emitted_at,
                        probe_fleet,
                        elapsed,
                    )
                if weather_cache is not None:
                    publish_weather_snapshot(producer, weather_cache, emitted_at)
                producer.flush()
                last_emit_at = now

            if now - last_weather_at >= weather_interval:
                try:
                    weather_cache = fetch_open_meteo_weather(session=session)
                except Exception as exc:
                    logger.warning("Open-Meteo weather refresh failed: %s", exc)
                last_weather_at = now

            time.sleep(min(1.0, emit_interval))

    except KeyboardInterrupt:
        print("\n[INFO] Stopping producer... Goodbye! 👋")
    except Exception as e:
        print(f"\n[ERROR] Producer crashed: {e}")
        raise
    finally:
        if producer is not None:
            producer.close()
        session.close()


if __name__ == "__main__":
    main()
