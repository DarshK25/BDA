"""Turn Overpass ways into the routable Mumbai network.

A side street joins a highway in the middle of that highway's geometry, not only
at its ends. Every coordinate shared by two or more ways becomes a junction, and
ways are split there. That is what makes Link Road, SV Road and the Western
Express Highway usable as alternate routes instead of three disconnected lines.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mapyourway.network import CLASS_RANK, haversine_km  # noqa: E402

RAW = ROOT / "data" / "osm_raw.json"
OUT = ROOT / "src" / "mapyourway" / "data" / "mumbai_roads.json"
GEOJSON = ROOT / "src" / "mapyourway" / "dashboard" / "static" / "network.geojson"

CLASS_SPEED = {
    "motorway": 70.0,
    "motorway_link": 40.0,
    "trunk": 60.0,
    "trunk_link": 40.0,
    "primary": 45.0,
    "primary_link": 35.0,
    "secondary": 35.0,
    "secondary_link": 30.0,
    "tertiary": 30.0,
    "tertiary_link": 25.0,
}
CLASS_LANES = {
    "motorway": 3,
    "trunk": 3,
    "primary": 2,
    "secondary": 2,
    "tertiary": 1,
}
# Seconds added to every segment so a zigzag through local streets is not free.
CLASS_DELAY = {
    "motorway": 0.0,
    "motorway_link": 2.0,
    "trunk": 1.0,
    "trunk_link": 2.0,
    "primary": 6.0,
    "primary_link": 6.0,
    "secondary": 10.0,
    "secondary_link": 10.0,
    "tertiary": 22.0,
    "tertiary_link": 22.0,
}
MAX_KM = {
    "motorway": 1.1,
    "trunk": 1.0,
    "primary": 0.8,
    "secondary": 0.65,
    "tertiary": 0.5,
}

PLACES = [
    ("Dahisar", 19.2546, 72.8598),
    ("Borivali", 19.2307, 72.8567),
    ("Kandivali", 19.2045, 72.8518),
    ("Malad", 19.1868, 72.8484),
    ("Goregaon", 19.1648, 72.8496),
    ("Jogeshwari", 19.1364, 72.8490),
    ("Andheri", 19.1197, 72.8468),
    ("Vile Parle", 19.0998, 72.8436),
    ("Santacruz", 19.0814, 72.8412),
    ("Khar", 19.0698, 72.8396),
    ("Bandra", 19.0544, 72.8406),
    ("BKC", 19.0675, 72.8690),
    ("Mahim", 19.0410, 72.8409),
    ("Matunga", 19.0269, 72.8553),
    ("Dadar", 19.0183, 72.8448),
    ("Wadala", 19.0210, 72.8640),
    ("Worli", 19.0174, 72.8156),
    ("Prabhadevi", 19.0158, 72.8280),
    ("Lower Parel", 18.9956, 72.8300),
    ("Mahalaxmi", 18.9829, 72.8240),
    ("Haji Ali", 18.9828, 72.8093),
    ("Tardeo", 18.9706, 72.8124),
    ("Grant Road", 18.9645, 72.8162),
    ("Nana Chowk", 18.9574, 72.8128),
    ("Marine Lines", 18.9454, 72.8238),
    ("Churchgate", 18.9322, 72.8264),
    ("Nariman Point", 18.9256, 72.8242),
    ("Colaba", 18.9156, 72.8256),
    ("Sion", 19.0394, 72.8616),
    ("Kurla", 19.0726, 72.8826),
    ("Chembur", 19.0522, 72.9005),
    ("Ghatkopar", 19.0860, 72.9081),
    ("Vikhroli", 19.1104, 72.9280),
    ("Powai", 19.1176, 72.9051),
    ("Mulund", 19.1726, 72.9565),
]


def _base_class(highway: str) -> str:
    return highway.replace("_link", "")


def _speed(tags: dict, highway: str) -> float:
    raw = str(tags.get("maxspeed", "")).lower().replace("km/h", "").replace("kmh", "").strip()
    if raw.endswith("mph"):
        try:
            return float(raw.replace("mph", "").strip()) * 1.609
        except ValueError:
            pass
    try:
        value = float(raw.split()[0])
        if 15 <= value <= 100:
            return value
    except (ValueError, IndexError):
        pass
    return CLASS_SPEED.get(highway, 30.0)


def _lanes(tags: dict, highway: str) -> int:
    raw = str(tags.get("lanes", "")).split(";")[0].strip()
    try:
        value = int(float(raw))
        if 1 <= value <= 8:
            return value
    except ValueError:
        pass
    return CLASS_LANES.get(_base_class(highway), 1)


def _oneway(tags: dict) -> int:
    value = str(tags.get("oneway", "")).lower()
    if value in {"yes", "true", "1"}:
        return 1
    if value == "-1":
        return -1
    return 0


def _points(element: dict) -> list[tuple[float, float]]:
    points = []
    for point in element.get("geometry") or []:
        lat = round(float(point["lat"]), 5)
        lon = round(float(point["lon"]), 5)
        if not points or points[-1] != (lat, lon):
            points.append((lat, lon))
    return points


def _thin(points: list[tuple[float, float]], min_km: float = 0.035) -> list[tuple[float, float]]:
    if len(points) <= 2:
        return points
    kept = [points[0]]
    for point in points[1:-1]:
        if haversine_km(kept[-1], point) >= min_km:
            kept.append(point)
    if kept[-1] != points[-1]:
        kept.append(points[-1])
    return kept


def _length(points: list[tuple[float, float]]) -> float:
    return sum(haversine_km(a, b) for a, b in zip(points, points[1:]))


def _midpoint(points: list[tuple[float, float]]) -> tuple[float, float]:
    target = _length(points) / 2.0
    walked = 0.0
    for start, end in zip(points, points[1:]):
        step = haversine_km(start, end)
        if walked + step >= target and step > 0:
            frac = (target - walked) / step
            return (start[0] + (end[0] - start[0]) * frac, start[1] + (end[1] - start[1]) * frac)
        walked += step
    return points[len(points) // 2]


def _usable(tags: dict) -> bool:
    if tags.get("area") == "yes":
        return False
    access = str(tags.get("access", "")).lower()
    motor = str(tags.get("motor_vehicle", "")).lower()
    if access in {"private", "no"} or motor in {"no", "private"}:
        return False
    return True


def _node_id(point: tuple[float, float]) -> str:
    return f"{point[0]:.5f},{point[1]:.5f}"


def build_segments(elements: list[dict]) -> list[dict]:
    ways = []
    users: dict[tuple[float, float], set[int]] = defaultdict(set)
    for element in elements:
        if element.get("type") != "way":
            continue
        tags = element.get("tags") or {}
        highway = str(tags.get("highway", ""))
        if highway not in CLASS_SPEED or not _usable(tags):
            continue
        points = _points(element)
        if _oneway(tags) < 0:
            points.reverse()
        if len(points) < 2 or _length(points) < 0.03:
            continue
        ways.append((int(element["id"]), tags, highway, points))
        for point in points:
            users[point].add(int(element["id"]))

    junctions = {point for point, owners in users.items() if len(owners) >= 2}
    segments = []
    for way_id, tags, highway, points in ways:
        name = tags.get("name") or tags.get("ref") or f"Local {highway.replace('_', ' ')}"
        part: list[tuple[float, float]] = [points[0]]
        part_index = 0
        max_km = MAX_KM.get(_base_class(highway), 0.6)

        def flush(force: bool = False) -> None:
            nonlocal part, part_index
            if len(part) < 2:
                return
            length = _length(part)
            if length < 0.04 and not force:
                return
            thinned = _thin(part)
            mid = _midpoint(thinned)
            segments.append(
                {
                    "segment_id": f"w{way_id}_{part_index}",
                    "osm_way_id": way_id,
                    "name": name,
                    "road_class": highway,
                    "node_a": _node_id(thinned[0]),
                    "node_b": _node_id(thinned[-1]),
                    "length_km": round(length, 3),
                    "free_flow_kmph": round(_speed(tags, highway), 1),
                    "lanes": _lanes(tags, highway),
                    "oneway": _oneway(tags) != 0,
                    "delay_s": CLASS_DELAY.get(highway, 12.0),
                    "coords": [[round(lat, 5), round(lon, 5)] for lat, lon in thinned],
                    "mid_lat": round(mid[0], 5),
                    "mid_lon": round(mid[1], 5),
                }
            )
            part_index += 1

        for point in points[1:]:
            part.append(point)
            at_junction = point in junctions and point != part[0]
            if at_junction or _length(part) >= max_km:
                flush(force=at_junction)
                part = [point]
        if len(part) > 1:
            flush(force=True)
    return [s for s in segments if s["node_a"] != s["node_b"]]


def _norm_name(name: str) -> str:
    if name.startswith("Local "):
        return ""
    return " ".join(name.lower().replace(".", " ").split())


def weld_dangling_ends(segments: list[dict], radius_km: float = 0.02) -> list[dict]:
    """Join dead-ends that stop a few metres short of another junction.

    OpenStreetMap sometimes ends one way just short of the road it should meet.
    Only degree-1 ends are moved, so an elevated highway is not glued to the
    street underneath it.
    """
    parent: dict[str, str] = {}

    def find(node: str) -> str:
        parent.setdefault(node, node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    coords: dict[str, tuple[float, float]] = {}
    degree: dict[str, int] = defaultdict(int)
    for segment in segments:
        for node_id, point in (
            (segment["node_a"], tuple(segment["coords"][0])),
            (segment["node_b"], tuple(segment["coords"][-1])),
        ):
            coords[node_id] = point
            degree[node_id] += 1

    cell = 0.0003  # about 30 m
    buckets: dict[tuple[int, int], list[str]] = defaultdict(list)
    for node_id, (lat, lon) in coords.items():
        buckets[(int(lat / cell), int(lon / cell))].append(node_id)

    for node_id, (lat, lon) in coords.items():
        if degree[node_id] != 1:
            continue
        cx, cy = int(lat / cell), int(lon / cell)
        best = None
        for ix in range(cx - 1, cx + 2):
            for iy in range(cy - 1, cy + 2):
                for other in buckets.get((ix, iy), []):
                    if other == node_id:
                        continue
                    distance = haversine_km((lat, lon), coords[other])
                    if distance > radius_km:
                        continue
                    # Prefer a real junction over another dangling end.
                    rank = 0 if degree[other] > 1 else 1
                    if best is None or (rank, distance) < best[0]:
                        best = ((rank, distance), other)
        if best:
            union(best[1], node_id)

    welded = []
    for segment in segments:
        segment["node_a"] = find(segment["node_a"])
        segment["node_b"] = find(segment["node_b"])
        if segment["node_a"] != segment["node_b"]:
            welded.append(segment)
    print(f"welded dangling ends: {len(segments) - len(welded)} collapsed, {len(welded)} remain")
    return welded


def choose_anchors(segments: list[dict]) -> list[str]:
    """Spread live TomTom samples across the city, one strong road per cell."""
    cell = 0.03
    cells: dict[tuple[int, int], list[dict]] = defaultdict(list)
    for segment in segments:
        if segment["length_km"] < 0.15 and CLASS_RANK.get(segment["road_class"], 0) < 4:
            continue
        key = (round(segment["mid_lat"] / cell), round(segment["mid_lon"] / cell))
        cells[key].append(segment)
    candidates = []
    for group in cells.values():
        group.sort(key=lambda s: (-CLASS_RANK.get(s["road_class"], 0), -s["length_km"]))
        candidates.append(group[0])
    selected: list[dict] = []
    remaining = candidates
    while remaining and len(selected) < 36:
        if not selected:
            pick = max(remaining, key=lambda s: CLASS_RANK.get(s["road_class"], 0))
        else:

            def score(segment: dict) -> tuple[float, int]:
                distance = min(
                    haversine_km((segment["mid_lat"], segment["mid_lon"]), (other["mid_lat"], other["mid_lon"]))
                    for other in selected
                )
                return (distance, CLASS_RANK.get(segment["road_class"], 0))

            pick = max(remaining, key=score)
        selected.append(pick)
        remaining = [s for s in remaining if s is not pick]
    return [s["segment_id"] for s in selected]


def attach_anchors(segments: list[dict], anchor_ids: list[str]) -> None:
    by_id = {s["segment_id"]: s for s in segments}
    by_name: dict[str, list[dict]] = defaultdict(list)
    for anchor_id in anchor_ids:
        anchor = by_id[anchor_id]
        anchor["inherit"] = "direct"
        anchor["anchor_segment_id"] = anchor_id
        key = _norm_name(anchor["name"])
        if key:
            by_name[key].append(anchor)
    anchor_set = set(anchor_ids)
    for segment in segments:
        if segment["segment_id"] in anchor_set:
            continue
        key = _norm_name(segment["name"])
        named = by_name.get(key) or []
        if named:
            nearest = min(
                named,
                key=lambda a: haversine_km((segment["mid_lat"], segment["mid_lon"]), (a["mid_lat"], a["mid_lon"])),
            )
            distance = haversine_km(
                (segment["mid_lat"], segment["mid_lon"]), (nearest["mid_lat"], nearest["mid_lon"])
            )
            if distance <= 4.0:
                segment["inherit"] = "same_road"
                segment["anchor_segment_id"] = nearest["segment_id"]
                continue
        segment["inherit"] = "urban_default"
        segment["anchor_segment_id"] = None


def snap_places(segments: list[dict]) -> list[dict]:
    incident: dict[str, int] = defaultdict(int)
    nodes: dict[str, tuple[float, float]] = {}
    for segment in segments:
        rank = CLASS_RANK.get(segment["road_class"], 0)
        for node_id, coord in (
            (segment["node_a"], segment["coords"][0]),
            (segment["node_b"], segment["coords"][-1]),
        ):
            nodes[node_id] = (coord[0], coord[1])
            incident[node_id] = max(incident[node_id], rank)
    places = []
    for name, lat, lon in PLACES:
        best = None
        best_score = None
        for node_id, (nlat, nlon) in nodes.items():
            distance = haversine_km((lat, lon), (nlat, nlon))
            if distance > 1.6:
                continue
            score = distance - 0.12 * incident[node_id]
            if best_score is None or score < best_score:
                best_score = score
                best = (node_id, nlat, nlon, distance, incident[node_id])
        if best is None:
            print(f"  place {name} has no junction within 1.6 km")
            continue
        node_id, nlat, nlon, distance, rank = best
        places.append(
            {
                "name": name,
                "node_id": node_id,
                "lat": round(nlat, 5),
                "lon": round(nlon, 5),
                "snap_km": round(distance, 3),
                "road_rank": rank,
            }
        )
        print(f"  {name:16} snaps {distance*1000:6.0f} m to rank-{rank} junction")
    return places


def write_geojson(segments: list[dict]) -> None:
    features = []
    for segment in segments:
        features.append(
            {
                "type": "Feature",
                "id": segment["segment_id"],
                "properties": {
                    "segment_id": segment["segment_id"],
                    "name": segment["name"],
                    "road_class": segment["road_class"],
                    "local": segment["road_class"] in {"tertiary", "tertiary_link"},
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[lon, lat] for lat, lon in segment["coords"]],
                },
            }
        )
    GEOJSON.parent.mkdir(parents=True, exist_ok=True)
    GEOJSON.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}, separators=(",", ":")),
        encoding="utf-8",
    )


def main() -> None:
    if not RAW.exists():
        raise SystemExit(f"missing {RAW}; run scripts/fetch_osm_roads.py first")
    elements = json.loads(RAW.read_text(encoding="utf-8")).get("elements", [])
    print(f"raw ways {len(elements)}")
    segments = weld_dangling_ends(build_segments(elements))
    anchor_ids = choose_anchors(segments)
    attach_anchors(segments, anchor_ids)
    print("places:")
    places = snap_places(segments)
    nodes = {}
    for segment in segments:
        nodes[segment["node_a"]] = segment["coords"][0]
        nodes[segment["node_b"]] = segment["coords"][-1]
    payload = {
        "city": "Mumbai",
        "source": "OpenStreetMap via the Overpass API",
        "license": "ODbL-1.0",
        "segments": segments,
        "nodes": nodes,
        "places": places,
        "anchor_ids": anchor_ids,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    write_geojson(segments)
    classes: dict[str, int] = defaultdict(int)
    inherits: dict[str, int] = defaultdict(int)
    for segment in segments:
        classes[segment["road_class"]] += 1
        inherits[segment["inherit"]] += 1
    print(f"segments {len(segments)} junctions {len(nodes)} anchors {len(anchor_ids)} places {len(places)}")
    print("classes", dict(classes))
    print("inherit", dict(inherits))
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")
    print(f"wrote {GEOJSON} ({GEOJSON.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
