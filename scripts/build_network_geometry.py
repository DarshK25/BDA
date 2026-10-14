"""Fetch and persist the Mumbai network's real driving geometries from OSRM.

This is deliberately a bounded, sequential generator.  It should be run only
when regenerating the checked-in asset, not by the application at runtime.
OSRM uses OpenStreetMap data, so the generated JSON retains each request URL,
the route distance, and the generation date for provenance.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from datetime import date
from pathlib import Path
from typing import Iterable
from urllib.parse import quote

import requests


OSRM_BASE_URL = "https://router.project-osrm.org"
OSRM_ROUTE_URL = f"{OSRM_BASE_URL}/route/v1/driving"
REQUEST_DELAY_SECONDS = 1.1
MAX_RETRIES = 3

# Keep these in lat/lon order, matching mapyourway.network.NODES.  They are
# road anchors, not neighbourhood-centre centroids.
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

EDGES = [
    ("S01", "Borivali", "Malad"),
    ("S02", "Malad", "Goregaon"),
    ("S03", "Goregaon", "Andheri"),
    ("S04", "Andheri", "Santacruz"),
    ("S05", "Santacruz", "Bandra"),
    ("S06", "Bandra", "Mahim"),
    ("S07", "Mahim", "Dadar"),
    ("S08", "Dadar", "Worli"),
    ("S09", "Andheri", "Powai"),
    ("S10", "Powai", "Ghatkopar"),
    ("S11", "Ghatkopar", "Kurla"),
    ("S12", "Kurla", "Sion"),
    ("S13", "Sion", "Dadar"),
    ("S14", "Santacruz", "Kurla"),
    ("S15", "Bandra", "Kurla"),
    ("S16", "Andheri", "Kurla"),
    ("S17", "Mahim", "Sion"),
    ("S18", "Goregaon", "Powai"),
    ("S19", "Bandra", "Worli"),
]

# Waypoints constrain a route only where a generic shortest route would take a
# visually misleading shortcut.  They are real road points supplied to OSRM,
# not hand-drawn geometry points.
WAYPOINTS: dict[str, list[tuple[float, float]]] = {
    # Jogeshwari-Vikhroli Link Road corridor, keeping S18 on the JVLR instead
    # of a diagonal across the Aarey/Powai open areas.
    "S18": [(19.1450, 72.8740)],
    # Middle of the Rajiv Gandhi Sea Link, keeping S19 on the bridge rather
    # than any local-road alternative.
    "S19": [(19.0230, 72.8160)],
}


def _osrm_coordinate(point: tuple[float, float]) -> str:
    lat, lon = point
    return f"{lon:.6f},{lat:.6f}"


def route_url(points: Iterable[tuple[float, float]]) -> str:
    coordinates = ";".join(_osrm_coordinate(point) for point in points)
    return (
        f"{OSRM_ROUTE_URL}/{quote(coordinates, safe=',;.-')}"
        "?overview=full&geometries=geojson&steps=false"
    )


def fetch_route(session: requests.Session, url: str) -> dict:
    """Fetch one route with bounded exponential retries."""
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            payload = response.json()
            route = payload.get("routes", [None])[0]
            if not route or not route.get("geometry", {}).get("coordinates"):
                raise ValueError("OSRM response did not contain a route geometry")
            return route
        except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
            last_error = exc
            if attempt + 1 < MAX_RETRIES:
                time.sleep(2**attempt)
    raise RuntimeError(f"OSRM route failed after {MAX_RETRIES} attempts: {last_error}")


def path_midpoint(geometry: list[list[float]]) -> list[float]:
    """Return the midpoint by geodesic path distance as [lat, lon]."""
    legs = []
    for first, second in zip(geometry, geometry[1:]):
        lat1, lon1 = math.radians(first[1]), math.radians(first[0])
        lat2, lon2 = math.radians(second[1]), math.radians(second[0])
        h = (
            math.sin((lat2 - lat1) / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
        )
        legs.append(2 * 6371.0 * math.asin(math.sqrt(h)))
    target = sum(legs) / 2
    travelled = 0.0
    for first, second, leg in zip(geometry, geometry[1:], legs):
        if travelled + leg >= target and leg:
            ratio = (target - travelled) / leg
            return [
                first[1] + (second[1] - first[1]) * ratio,
                first[0] + (second[0] - first[0]) * ratio,
            ]
        travelled += leg
    return [geometry[-1][1], geometry[-1][0]]


def build(output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    segments = []
    try:
        for index, (segment_id, node_a, node_b) in enumerate(EDGES):
            points = [NODES[node_a], *WAYPOINTS.get(segment_id, []), NODES[node_b]]
            url = route_url(points)
            started = time.monotonic()
            route = fetch_route(session, url)
            geometry = route["geometry"]["coordinates"]
            segments.append(
                {
                    "segment_id": segment_id,
                    "node_a": node_a,
                    "node_b": node_b,
                    "geometry": geometry,
                    "geometry_source": "osrm",
                    "source_url": url,
                    "route_distance_m": route["distance"],
                    "route_duration_s": route["duration"],
                    # This explicit sample is in the producer's (lat, lon)
                    # order and is itself taken from the fetched path.
                    "road_sample": path_midpoint(geometry),
                }
            )
            print(f"{segment_id}: {route['distance'] / 1000:.3f} km")
            if index + 1 < len(EDGES):
                time.sleep(max(0.0, REQUEST_DELAY_SECONDS - (time.monotonic() - started)))
    finally:
        session.close()

    payload = {
        "schema_version": 1,
        "city": "Mumbai",
        "generated_at": date.today().isoformat(),
        "attribution": "© OpenStreetMap contributors",
        "routing_service": "OSRM",
        "routing_service_url": OSRM_BASE_URL,
        "nodes": {
            name: {"lat": coordinates[0], "lon": coordinates[1]}
            for name, coordinates in NODES.items()
        },
        "segments": segments,
    }
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "src"
        / "mapyourway"
        / "network_geometry.json",
    )
    args = parser.parse_args()
    build(args.output)


if __name__ == "__main__":
    main()
