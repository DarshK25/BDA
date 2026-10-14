"""Build a real OSM local-street graph for the Borivali–South Mumbai corridor.

This is a bounded generator, not a runtime dependency.  It queries Overpass for
driveable ways, contracts degree-2 nodes, keeps streets near the monitored
arterials, and writes ``src/mapyourway/local_streets.json``.

Speeds on these streets are not TomTom observations.  The live router inherits
a congestion ratio from the nearest monitored corridor.
"""
from __future__ import annotations

import json
import math
import sys
import time
from datetime import date
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mapyourway.network import SEGMENTS, haversine_km  # noqa: E402

OVERPASS_URLS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
)
BBOX = (18.915, 72.795, 19.248, 72.918)  # south, west, north, east
OUTPUT_PATH = SRC / "mapyourway" / "local_streets.json"
HIGHWAY_SPEEDS = {
    "motorway": 80,
    "motorway_link": 50,
    "trunk": 70,
    "trunk_link": 45,
    "primary": 50,
    "primary_link": 40,
    "secondary": 40,
    "secondary_link": 35,
    "tertiary": 35,
    "tertiary_link": 30,
    "unclassified": 30,
    "residential": 25,
}
KEEP_HIGHWAYS = set(HIGHWAY_SPEEDS)
MIN_EDGE_M = 60.0
CORRIDOR_BUFFER_KM = 0.55
MAX_GEOMETRY_STEP_M = 80.0
QUERY = f"""
[out:json][timeout:180];
(
  way["highway"~"^({'|'.join(sorted(KEEP_HIGHWAYS))})$"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
);
out geom;
"""


def _simplify(points: list[list[float]]) -> list[list[float]]:
    if len(points) <= 2:
        return points
    kept = [points[0]]
    travelled = 0.0
    for prev, curr in zip(points, points[1:]):
        travelled += haversine_km((prev[1], prev[0]), (curr[1], curr[0])) * 1000
        if travelled >= MAX_GEOMETRY_STEP_M:
            kept.append(curr)
            travelled = 0.0
    if kept[-1] != points[-1]:
        kept.append(points[-1])
    return kept


def _corridor_samples() -> list[tuple[float, float]]:
    samples: list[tuple[float, float]] = []
    for segment in SEGMENTS:
        geometry = segment.get("geometry") or []
        if len(geometry) < 2:
            continue
        samples.append((geometry[0][1], geometry[0][0]))
        travelled = 0.0
        for prev, curr in zip(geometry, geometry[1:]):
            travelled += haversine_km((prev[1], prev[0]), (curr[1], curr[0]))
            if travelled >= 0.2:
                samples.append((curr[1], curr[0]))
                travelled = 0.0
        samples.append((geometry[-1][1], geometry[-1][0]))
    return samples


def _near_corridor(mid_lat: float, mid_lon: float, samples: list[tuple[float, float]]) -> bool:
    return any(haversine_km((mid_lat, mid_lon), sample) <= CORRIDOR_BUFFER_KM for sample in samples)


def _fetch_ways() -> list[dict]:
    last_error = None
    for url in OVERPASS_URLS:
        for attempt in range(3):
            try:
                response = requests.post(url, data={"data": QUERY}, timeout=200)
                response.raise_for_status()
                payload = response.json()
                return [el for el in payload.get("elements", []) if el.get("type") == "way"]
            except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
                last_error = exc
                time.sleep(4 * (attempt + 1))
    raise RuntimeError(f"Overpass extract failed: {last_error}") from last_error


def _split_way(way: dict, intersection_ids: set[int]) -> list[dict]:
    geometry = way.get("geometry") or []
    node_ids = way.get("nodes") or []
    if len(geometry) < 2 or len(node_ids) != len(geometry):
        return []
    highway = way.get("tags", {}).get("highway")
    if highway not in KEEP_HIGHWAYS:
        return []
    chunks: list[dict] = []
    start = 0
    for index in range(1, len(node_ids) - 1):
        if node_ids[index] in intersection_ids:
            chunks.append(
                {
                    "node_a": node_ids[start],
                    "node_b": node_ids[index],
                    "geometry": [[pt["lon"], pt["lat"]] for pt in geometry[start : index + 1]],
                    "highway": highway,
                    "name": way.get("tags", {}).get("name") or highway,
                }
            )
            start = index
    chunks.append(
        {
            "node_a": node_ids[start],
            "node_b": node_ids[-1],
            "geometry": [[pt["lon"], pt["lat"]] for pt in geometry[start:]],
            "highway": highway,
            "name": way.get("tags", {}).get("name") or highway,
        }
    )
    return chunks


def build() -> dict:
    ways = _fetch_ways()
    usage: dict[int, int] = {}
    for way in ways:
        for node_id in way.get("nodes") or []:
            usage[node_id] = usage.get(node_id, 0) + 1
    intersections = {node_id for node_id, count in usage.items() if count >= 2}

    samples = _corridor_samples()
    nodes: dict[str, list[float]] = {}
    edges: list[dict] = []
    seen: set[tuple[int, int]] = set()
    for way in ways:
        for chunk in _split_way(way, intersections):
            geometry = _simplify(chunk["geometry"])
            if len(geometry) < 2:
                continue
            length_m = sum(
                haversine_km((a[1], a[0]), (b[1], b[0])) * 1000
                for a, b in zip(geometry, geometry[1:])
            )
            if length_m < MIN_EDGE_M:
                continue
            mid = geometry[len(geometry) // 2]
            if not _near_corridor(mid[1], mid[0], samples):
                continue
            pair = tuple(sorted((chunk["node_a"], chunk["node_b"])))
            if pair in seen or pair[0] == pair[1]:
                continue
            seen.add(pair)
            node_a = f"osm-{chunk['node_a']}"
            node_b = f"osm-{chunk['node_b']}"
            nodes[node_a] = [geometry[0][1], geometry[0][0]]
            nodes[node_b] = [geometry[-1][1], geometry[-1][0]]
            edges.append(
                {
                    "segment_id": f"L{len(edges) + 1:04d}",
                    "node_a": node_a,
                    "node_b": node_b,
                    "name": chunk["name"],
                    "highway": chunk["highway"],
                    "length_km": round(length_m / 1000.0, 6),
                    "free_flow_kmph": float(HIGHWAY_SPEEDS[chunk["highway"]]),
                    "lanes": 1 if chunk["highway"] in {"residential", "unclassified"} else 2,
                    "geometry": geometry,
                    "geometry_source": "osm_overpass",
                    "monitored": False,
                }
            )

    return {
        "source": "OpenStreetMap Overpass",
        "license": "ODbL",
        "attribution": "© OpenStreetMap contributors",
        "query_url": OVERPASS_URLS[0],
        "bbox": {"south": BBOX[0], "west": BBOX[1], "north": BBOX[2], "east": BBOX[3]},
        "generated_on": date.today().isoformat(),
        "corridor_buffer_km": CORRIDOR_BUFFER_KM,
        "nodes": nodes,
        "edges": edges,
    }


def main() -> None:
    payload = build()
    OUTPUT_PATH.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    print(
        f"Wrote {OUTPUT_PATH} with {len(payload['nodes'])} nodes and "
        f"{len(payload['edges'])} local edges"
    )


if __name__ == "__main__":
    main()
