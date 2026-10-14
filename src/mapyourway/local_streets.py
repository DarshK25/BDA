"""OSM local streets used for alternate routing, not TomTom polling.

Monitored corridors stay in ``network.SEGMENTS`` and are the only edges Spark
and the producer observe.  These local edges are a real OpenStreetMap extract
along the same corridor.  Live speed is inherited from the nearest monitored
segment's speed ratio so we never invent a second GPS feed.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from mapyourway.network import NODES, SEGMENTS, haversine_km

LOCAL_STREETS_PATH = Path(__file__).with_name("local_streets.json")
SNAP_LIMIT_KM = 0.25


def _valid_point(value: Any) -> bool:
    return (
        isinstance(value, (list, tuple))
        and len(value) == 2
        and all(isinstance(item, (int, float)) and not isinstance(item, bool) and math.isfinite(float(item)) for item in value)
    )


def load_local_streets(path: str | Path | None = None) -> dict[str, Any]:
    asset_path = Path(path) if path is not None else LOCAL_STREETS_PATH
    empty = {"nodes": {}, "edges": [], "links": [], "available": False, "source": None}
    try:
        payload = json.loads(asset_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return empty
    raw_nodes = payload.get("nodes")
    raw_edges = payload.get("edges")
    if not isinstance(raw_nodes, dict) or not isinstance(raw_edges, list) or not raw_edges:
        return empty

    nodes: dict[str, tuple[float, float]] = {}
    for node_id, coord in raw_nodes.items():
        if not isinstance(node_id, str) or not _valid_point(coord):
            continue
        nodes[node_id] = (float(coord[0]), float(coord[1]))

    edges: list[dict] = []
    for record in raw_edges:
        if not isinstance(record, dict):
            continue
        node_a, node_b = record.get("node_a"), record.get("node_b")
        geometry = record.get("geometry")
        length_km = record.get("length_km")
        free_flow = record.get("free_flow_kmph")
        if (
            node_a not in nodes
            or node_b not in nodes
            or not isinstance(record.get("segment_id"), str)
            or not isinstance(geometry, list)
            or len(geometry) < 2
            or not all(_valid_point(point) for point in geometry)
            or not isinstance(length_km, (int, float))
            or not isinstance(free_flow, (int, float))
            or length_km <= 0
            or free_flow <= 0
        ):
            continue
        mid = geometry[len(geometry) // 2]
        edges.append(
            {
                "segment_id": record["segment_id"],
                "name": record.get("name") or record["segment_id"],
                "node_a": node_a,
                "node_b": node_b,
                "length_km": float(length_km),
                "free_flow_kmph": float(free_flow),
                "lanes": int(record.get("lanes") or 1),
                "highway": record.get("highway") or "residential",
                "geometry": [[float(p[0]), float(p[1])] for p in geometry],
                "geometry_source": "osm_overpass",
                "monitored": False,
                "nearest_monitored": _nearest_monitored(float(mid[1]), float(mid[0])),
                "lat1": nodes[node_a][0],
                "lon1": nodes[node_a][1],
                "lat2": nodes[node_b][0],
                "lon2": nodes[node_b][1],
            }
        )
    if not edges:
        return empty

    links = []
    for name, (lat, lon) in NODES.items():
        nearest_id = min(nodes, key=lambda nid: haversine_km((lat, lon), nodes[nid]))
        distance = haversine_km((lat, lon), nodes[nearest_id])
        if distance <= SNAP_LIMIT_KM:
            links.append(
                {
                    "segment_id": f"SNAP-{name}",
                    "node_a": name,
                    "node_b": nearest_id,
                    "length_km": max(distance, 0.01),
                    "free_flow_kmph": 25.0,
                    "geometry": [[lon, lat], [nodes[nearest_id][1], nodes[nearest_id][0]]],
                }
            )
    return {
        "nodes": nodes,
        "edges": edges,
        "links": links,
        "available": True,
        "source": payload.get("source"),
        "attribution": payload.get("attribution"),
        "generated_on": payload.get("generated_on"),
        "edge_count": len(edges),
        "node_count": len(nodes),
    }


def _nearest_monitored(lat: float, lon: float) -> str:
    best_id = SEGMENTS[0]["segment_id"]
    best = float("inf")
    for segment in SEGMENTS:
        geometry = segment.get("geometry") or []
        mid = geometry[len(geometry) // 2] if geometry else [segment["lon1"], segment["lat1"]]
        distance = haversine_km((lat, lon), (float(mid[1]), float(mid[0])))
        if distance < best:
            best = distance
            best_id = segment["segment_id"]
    return best_id


def inherit_local_speeds(monitored_speeds: dict[str, float] | None) -> dict[str, float]:
    """Map each local street to nearest-corridor speed_ratio * OSM free-flow."""
    if not monitored_speeds:
        return {}
    ratios: dict[str, float] = {}
    by_id = {segment["segment_id"]: segment for segment in SEGMENTS}
    for segment_id, speed in monitored_speeds.items():
        segment = by_id.get(segment_id)
        if segment is None or segment["free_flow_kmph"] <= 0:
            continue
        ratios[segment_id] = max(0.15, min(1.0, float(speed) / segment["free_flow_kmph"]))
    inherited: dict[str, float] = {}
    for edge in LOCAL_EDGES:
        ratio = ratios.get(edge["nearest_monitored"])
        if ratio is None:
            continue
        inherited[edge["segment_id"]] = edge["free_flow_kmph"] * ratio
    return inherited


LOCAL_STREETS = load_local_streets()
LOCAL_NODES: dict[str, tuple[float, float]] = LOCAL_STREETS["nodes"]
LOCAL_EDGES: list[dict] = LOCAL_STREETS["edges"]
LOCAL_LINKS: list[dict] = LOCAL_STREETS["links"]
LOCAL_STREETS_AVAILABLE = bool(LOCAL_STREETS["available"])
ALL_ROUTING_NODES: dict[str, tuple[float, float]] = {**NODES, **LOCAL_NODES}
