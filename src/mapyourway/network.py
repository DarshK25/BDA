"""
Mumbai Road Network - Western & Central Railway Lines Corridor
MapYourWay Traffic Network covering major junctions from Borivali to Worli.

Nodes are junction areas with real-world approximate coordinates; segments are the road
links between them. The same table is used by the simulator, the Spark job (as a static
lookup table), the router and the dashboard, so they can never disagree about the map.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

# name -> (lat, lon).  These are arterial/highway junction anchors rather
# than neighbourhood centroids, chosen so OSRM returns direct, road-shaped
# routes between them (anchors on flyovers or limited-access ramps such as the
# Sea Link forced 2-3x detours, so they were moved onto the adjacent surface
# arterials: WEH / S.V. Road / L.B.S. Marg).  The Bandra anchor is the Kalanagar
# junction at the Sea Link's Bandra approach so S19 still routes over the bridge.
NODES: dict[str, tuple[float, float]] = {
    "Borivali": (19.2307, 72.8567),
    "Malad": (19.1872, 72.8495),
    "Goregaon": (19.1646, 72.8493),
    "Andheri": (19.1197, 72.8500),
    "Powai": (19.1176, 72.9060),
    "Santacruz": (19.0817, 72.8560),
    "Bandra": (19.0607, 72.8503),
    "Kurla": (19.0700, 72.8790),
    "Ghatkopar": (19.0860, 72.9081),
    "Mahim": (19.0430, 72.8420),
    "Sion": (19.0390, 72.8619),
    "Dadar": (19.0180, 72.8420),
    "Worli": (19.008655, 72.817498),
}

# (segment_id, node_a, node_b, free_flow_kmph, lanes, demand_bias)
# demand_bias > 1 means the road is a typical bottleneck.
_EDGES = [
    # North-South arterials (Western Line)
    ("S01", "Borivali", "Malad", 60, 3, 0.9),
    ("S02", "Malad", "Goregaon", 55, 3, 1.0),
    ("S03", "Goregaon", "Andheri", 55, 3, 1.1),
    ("S04", "Andheri", "Santacruz", 50, 3, 1.1),
    ("S05", "Santacruz", "Bandra", 45, 3, 1.2),
    ("S06", "Bandra", "Mahim", 40, 2, 1.2),
    ("S07", "Mahim", "Dadar", 40, 2, 1.2),
    ("S08", "Dadar", "Worli", 45, 3, 1.1),
    
    # Eastern connections
    ("S09", "Andheri", "Powai", 40, 2, 0.9),
    ("S10", "Powai", "Ghatkopar", 40, 2, 0.9),
    ("S11", "Ghatkopar", "Kurla", 45, 2, 1.0),
    ("S12", "Kurla", "Sion", 40, 2, 1.1),
    ("S13", "Sion", "Dadar", 40, 2, 1.2),
    
    # Cross-connections
    ("S14", "Santacruz", "Kurla", 50, 3, 1.0),
    ("S15", "Bandra", "Kurla", 50, 3, 1.1),
    ("S16", "Andheri", "Kurla", 45, 2, 1.0),
    ("S17", "Mahim", "Sion", 40, 2, 1.0),
    ("S18", "Goregaon", "Powai", 45, 2, 0.8),
    
    # Bandra-Worli Sea Link
    ("S19", "Bandra", "Worli", 70, 4, 0.8),  # Fastest route
]

ROAD_FACTOR = 1.25  # retained for the explicit, offline fallback only

NETWORK_GEOMETRY_PATH = Path(__file__).with_name("network_geometry.json")


def _valid_coordinate_pair(value: Any) -> bool:
    return (
        isinstance(value, (list, tuple))
        and len(value) == 2
        and all(
            isinstance(item, (int, float))
            and not isinstance(item, bool)
            and math.isfinite(float(item))
            for item in value
        )
        and -180 <= float(value[0]) <= 180
        and -90 <= float(value[1]) <= 90
    )


def _valid_geometry(value: Any) -> bool:
    return isinstance(value, list) and len(value) >= 2 and all(
        _valid_coordinate_pair(point) for point in value
    )


def load_network_geometry(path: str | Path | None = None) -> tuple[dict[str, dict], bool]:
    """Load the generated OSRM geometry asset without making import fragile.

    The generator emits all 19 segments as one asset.  A missing, malformed,
    or incomplete asset is rejected as a unit so the caller can make the
    explicit fallback choice instead of silently mixing sourced and invented
    paths.  The boolean return value is ``True`` when the caller must use the
    endpoint-chord fallback.
    """
    asset_path = Path(path) if path is not None else NETWORK_GEOMETRY_PATH
    try:
        with asset_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        raw_segments = payload["segments"]
        if isinstance(raw_segments, dict):
            records = list(raw_segments.values())
        elif isinstance(raw_segments, list):
            records = raw_segments
        else:
            return {}, True

        expected_segments = {
            segment_id: (node_a, node_b)
            for segment_id, node_a, node_b, *_ in _EDGES
        }
        loaded: dict[str, dict] = {}
        for record in records:
            if not isinstance(record, dict):
                return {}, True
            segment_id = record.get("segment_id")
            distance_m = record.get("route_distance_m")
            if (
                not isinstance(segment_id, str)
                or segment_id in loaded
                or segment_id not in expected_segments
                or (record.get("node_a"), record.get("node_b"))
                != expected_segments[segment_id]
                or not _valid_geometry(record.get("geometry"))
                or not isinstance(distance_m, (int, float))
                or isinstance(distance_m, bool)
                or not math.isfinite(float(distance_m))
                or float(distance_m) <= 0
                or record.get("geometry_source") != "osrm"
                or not str(record.get("source_url", "")).startswith(
                    "https://router.project-osrm.org/route/v1/driving/"
                )
            ):
                return {}, True
            node_a, node_b = expected_segments[segment_id]
            if (
                haversine_km(
                    (record["geometry"][0][1], record["geometry"][0][0]),
                    NODES[node_a],
                )
                > 0.75
                or haversine_km(
                    (record["geometry"][-1][1], record["geometry"][-1][0]),
                    NODES[node_b],
                )
                > 0.75
                or float(distance_m)
                < haversine_km(NODES[node_a], NODES[node_b]) * 1000
            ):
                return {}, True
            loaded[segment_id] = record
        if set(loaded) != set(expected_segments):
            return {}, True
        return loaded, False
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        return {}, True


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(h))


def point_along_segment(segment: dict, fraction: float) -> tuple[float, float]:
    """Return ``(lat, lon)`` at a fraction of the segment's path distance.

    Sourced geometry is stored as GeoJSON ``[lon, lat]`` points.  Distances
    between consecutive points are accumulated geodesically, then the target
    point is interpolated within the containing path leg.  Segments without a
    valid geometry deliberately fall back to the old endpoint chord.
    """
    try:
        fraction = min(1.0, max(0.0, float(fraction)))
    except (TypeError, ValueError):
        fraction = 0.5

    geometry = segment.get("geometry")
    if segment.get("geometry_fallback") or not _valid_geometry(geometry):
        lat1, lon1 = float(segment["lat1"]), float(segment["lon1"])
        lat2, lon2 = float(segment["lat2"]), float(segment["lon2"])
        return lat1 + (lat2 - lat1) * fraction, lon1 + (lon2 - lon1) * fraction

    legs = [
        haversine_km((first[1], first[0]), (second[1], second[0]))
        for first, second in zip(geometry, geometry[1:])
    ]
    total = sum(legs)
    if total <= 0:
        first = geometry[0]
        return float(first[1]), float(first[0])

    target = total * fraction
    travelled = 0.0
    for first, second, leg_length in zip(geometry, geometry[1:], legs):
        if leg_length <= 0:
            continue
        if travelled + leg_length >= target:
            ratio = (target - travelled) / leg_length
            lon = first[0] + (second[0] - first[0]) * ratio
            lat = first[1] + (second[1] - first[1]) * ratio
            return float(lat), float(lon)
        travelled += leg_length

    last = geometry[-1]
    return float(last[1]), float(last[0])


def _build_segments() -> list[dict]:
    out = []
    geometry_by_id, fallback_used = load_network_geometry()
    for sid, a, b, ff, lanes, bias in _EDGES:
        (lat1, lon1), (lat2, lon2) = NODES[a], NODES[b]
        source = geometry_by_id.get(sid)
        if source is None:
            geometry = [[lon1, lat1], [lon2, lat2]]
            length_km = round(haversine_km(NODES[a], NODES[b]) * ROAD_FACTOR, 6)
            geometry_source = "fallback_chord"
            route_distance_m = None
            source_url = None
            road_sample = None
        else:
            geometry = source["geometry"]
            length_km = float(source["route_distance_m"]) / 1000.0
            geometry_source = "osrm"
            route_distance_m = float(source["route_distance_m"])
            source_url = source["source_url"]
            road_sample = source.get("road_sample")
        out.append(
            {
                "segment_id": sid,
                "name": f"{a}-{b}",
                "node_a": a,
                "node_b": b,
                "length_km": length_km,
                "free_flow_kmph": float(ff),
                "lanes": int(lanes),
                "demand_bias": float(bias),
                "lat1": lat1, "lon1": lon1, "lat2": lat2, "lon2": lon2,
                "geometry": geometry,
                "geometry_source": geometry_source,
                "geometry_fallback": bool(fallback_used or source is None),
                "route_distance_m": route_distance_m,
                "source_url": source_url,
                "road_sample": road_sample,
            }
        )
    return out


SEGMENTS: list[dict] = _build_segments()
SEGMENT_BY_ID: dict[str, dict] = {s["segment_id"]: s for s in SEGMENTS}
GEOMETRY_FALLBACK_USED = any(s["geometry_fallback"] for s in SEGMENTS)
# Public alias kept intentionally obvious for callers/health checks.
NETWORK_GEOMETRY_FALLBACK = GEOMETRY_FALLBACK_USED


def neighbours(segment_id: str) -> list[str]:
    """Segments that share a junction with this one (used for incident spill-back)."""
    s = SEGMENT_BY_ID[segment_id]
    ends = {s["node_a"], s["node_b"]}
    return [o["segment_id"] for o in SEGMENTS
            if o["segment_id"] != segment_id and ends & {o["node_a"], o["node_b"]}]


def segments_at_node(node: str) -> list[str]:
    return [s["segment_id"] for s in SEGMENTS if node in (s["node_a"], s["node_b"])]


def get_network_summary() -> dict:
    """Return summary statistics about the Mumbai network."""
    total_length = sum(road["length_km"] for road in SEGMENTS)
    total_capacity = sum(road["capacity_vehicles"] if "capacity_vehicles" in road else road["lanes"] * 50 for road in SEGMENTS)
    avg_speed_limit = sum(road["free_flow_kmph"] for road in SEGMENTS) / len(SEGMENTS)
    
    return {
        "city": "Mumbai",
        "area": "Western & Central Railway Lines",
        "num_junctions": len(NODES),
        "num_roads": len(SEGMENTS),
        "total_length_km": round(total_length, 2),
        "avg_speed_limit_kmh": round(avg_speed_limit, 2),
    }


if __name__ == "__main__":
    # Print network summary
    import json
    summary = get_network_summary()
    print("MapYourWay Mumbai Network Summary:")
    print(json.dumps(summary, indent=2))
    
    # Example: Find connected roads
    print("\nRoads connected to Andheri (Western & Central hub):")
    for road in SEGMENTS:
        if "Andheri" in (road["node_a"], road["node_b"]):
            print(f"  {road['segment_id']}: {road['name']}")
