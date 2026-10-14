"""Mumbai road network loaded from OpenStreetMap geometries.

scripts/build_network.py turns raw Overpass ways into this file. Every other
part of the system (producer, Spark, router, dashboard) reads the same table,
so they cannot disagree about which road a segment id refers to.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

NETWORK_PATH = Path(__file__).resolve().parent / "data" / "mumbai_roads.json"

# rank used when a place name has to snap to a nearby junction
CLASS_RANK = {
    "motorway": 6,
    "motorway_link": 5,
    "trunk": 5,
    "trunk_link": 4,
    "primary": 4,
    "primary_link": 3,
    "secondary": 3,
    "secondary_link": 2,
    "tertiary": 1,
    "tertiary_link": 1,
}


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(min(1.0, h)))


def _empty() -> dict:
    return {"nodes": {}, "segments": [], "places": [], "anchor_ids": []}


def load_network(path: Path = NETWORK_PATH) -> dict:
    if not path.exists():
        return _empty()
    return json.loads(path.read_text(encoding="utf-8"))


_DATA = load_network()

JUNCTIONS: dict[str, tuple[float, float]] = {
    node_id: (float(pair[0]), float(pair[1])) for node_id, pair in _DATA.get("nodes", {}).items()
}
SEGMENTS: list[dict] = list(_DATA.get("segments", []))
SEGMENT_BY_ID: dict[str, dict] = {s["segment_id"]: s for s in SEGMENTS}
PLACES: list[dict] = list(_DATA.get("places", []))
PLACE_BY_NAME: dict[str, dict] = {p["name"]: p for p in PLACES}
ANCHOR_IDS: list[str] = list(_DATA.get("anchor_ids", []))

# Backwards-compatible name used by older teaching notes.
NODES = {p["name"]: (p["lat"], p["lon"]) for p in PLACES}


def neighbours(segment_id: str) -> list[str]:
    """Segments that share a junction with this one."""
    segment = SEGMENT_BY_ID[segment_id]
    ends = {segment["node_a"], segment["node_b"]}
    return [
        other["segment_id"]
        for other in SEGMENTS
        if other["segment_id"] != segment_id and ends & {other["node_a"], other["node_b"]}
    ]


def segments_at_node(node: str) -> list[str]:
    return [s["segment_id"] for s in SEGMENTS if node in (s["node_a"], s["node_b"])]


def place_names() -> list[str]:
    """North to south, which matches how people describe a Mumbai trip."""
    return [p["name"] for p in sorted(PLACES, key=lambda p: -p["lat"])]


def get_network_summary() -> dict:
    total_length = sum(road["length_km"] for road in SEGMENTS)
    classes: dict[str, int] = {}
    for road in SEGMENTS:
        classes[road["road_class"]] = classes.get(road["road_class"], 0) + 1
    return {
        "city": "Mumbai",
        "source": _DATA.get("source", "OpenStreetMap"),
        "num_junctions": len(JUNCTIONS),
        "num_roads": len(SEGMENTS),
        "num_places": len(PLACES),
        "num_anchors": len(ANCHOR_IDS),
        "total_length_km": round(total_length, 1),
        "by_class": classes,
    }
