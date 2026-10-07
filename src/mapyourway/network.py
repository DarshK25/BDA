"""
Mumbai Road Network - Western & Central Railway Lines Corridor
MapYourWay Traffic Network covering major junctions from Borivali to Worli.

Nodes are junction areas with real-world approximate coordinates; segments are the road
links between them. The same table is used by the simulator, the Spark job (as a static
lookup table), the router and the dashboard, so they can never disagree about the map.
"""
from __future__ import annotations

import math

# name -> (lat, lon) - Real Mumbai coordinates
NODES: dict[str, tuple[float, float]] = {
    "Borivali": (19.2307, 72.8567),
    "Malad": (19.1864, 72.8484),
    "Goregaon": (19.1663, 72.8526),
    "Andheri": (19.1136, 72.8697),
    "Powai": (19.1176, 72.9060),
    "Santacruz": (19.0817, 72.8411),
    "Bandra": (19.0596, 72.8295),
    "Kurla": (19.0726, 72.8845),
    "Ghatkopar": (19.0860, 72.9081),
    "Mahim": (19.0409, 72.8403),
    "Sion": (19.0390, 72.8619),
    "Dadar": (19.0178, 72.8478),
    "Worli": (19.0096, 72.8176),
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

ROAD_FACTOR = 1.25  # roads are longer than the straight line between two junctions


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(h))


def _build_segments() -> list[dict]:
    out = []
    for sid, a, b, ff, lanes, bias in _EDGES:
        (lat1, lon1), (lat2, lon2) = NODES[a], NODES[b]
        out.append(
            {
                "segment_id": sid,
                "name": f"{a}-{b}",
                "node_a": a,
                "node_b": b,
                "length_km": round(haversine_km(NODES[a], NODES[b]) * ROAD_FACTOR, 2),
                "free_flow_kmph": float(ff),
                "lanes": int(lanes),
                "demand_bias": float(bias),
                "lat1": lat1, "lon1": lon1, "lat2": lat2, "lon2": lon2,
            }
        )
    return out


SEGMENTS: list[dict] = _build_segments()
SEGMENT_BY_ID: dict[str, dict] = {s["segment_id"]: s for s in SEGMENTS}


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
