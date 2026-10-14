"""Download Mumbai arterial and local-street geometries from OpenStreetMap.

Tiles are small and slow on purpose. The public Overpass servers reject bursts,
and the lab proxy cuts off long calls. Progress is saved after every tile, so
the script can be run again and it will continue.
"""
from __future__ import annotations

import json
import pathlib
import ssl
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "osm_raw.json"
DONE = ROOT / "data" / "osm_tiles_done.json"

SOUTH, NORTH = 18.90, 19.26
WEST, EAST = 72.78, 72.99
STEP = 0.045

ENDPOINTS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://overpass.openstreetmap.fr/api/interpreter",
]

QUERY = """
[out:json][timeout:25];
way["highway"~"^(motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|tertiary|tertiary_link)$"]({south:.5f},{west:.5f},{north:.5f},{east:.5f});
out geom;
"""


def _fetch(url: str, query: str) -> dict:
    req = urllib.request.Request(
        url,
        data=query.encode("utf-8"),
        headers={"User-Agent": "mapyourway-student/1.0 (academic project; contact none)"},
    )
    ctx = ssl._create_unverified_context()
    with urllib.request.urlopen(req, context=ctx, timeout=45) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _tiles() -> list[tuple[float, float, float, float]]:
    boxes = []
    lat = SOUTH
    while lat < NORTH - 1e-9:
        lon = WEST
        lat2 = min(NORTH, round(lat + STEP, 5))
        while lon < EAST - 1e-9:
            lon2 = min(EAST, round(lon + STEP, 5))
            boxes.append((round(lat, 5), round(lon, 5), lat2, lon2))
            lon = lon2
        lat = lat2
    return boxes


def _load() -> tuple[dict[int, dict], set[str]]:
    seen: dict[int, dict] = {}
    if OUT.exists():
        payload = json.loads(OUT.read_text(encoding="utf-8"))
        for el in payload.get("elements", []):
            seen[int(el["id"])] = el
    done: set[str] = set()
    if DONE.exists():
        done = set(json.loads(DONE.read_text(encoding="utf-8")))
    return seen, done


def _save(seen: dict[int, dict], done: set[str]) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"elements": list(seen.values())}), encoding="utf-8")
    DONE.write_text(json.dumps(sorted(done)), encoding="utf-8")


def main() -> None:
    seen, done = _load()
    boxes = _tiles()
    print(f"resume ways={len(seen)} done_tiles={len(done)} total_tiles={len(boxes)}", flush=True)
    endpoint_i = 0
    for south, west, north, east in boxes:
        key = f"{south:.5f},{west:.5f},{north:.5f},{east:.5f}"
        if key in done:
            continue
        query = QUERY.format(south=south, west=west, north=north, east=east)
        payload = None
        for attempt in range(6):
            url = ENDPOINTS[endpoint_i % len(ENDPOINTS)]
            endpoint_i += 1
            try:
                payload = _fetch(url, query)
                break
            except urllib.error.HTTPError as exc:
                wait = 20 if exc.code == 429 else 8
                print(f"retry {attempt + 1} {key} HTTP {exc.code} via {url} sleep {wait}s", flush=True)
                time.sleep(wait)
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                print(f"retry {attempt + 1} {key} {exc}", flush=True)
                time.sleep(8)
        if payload is None:
            print(f"SKIP tile {key} after retries; run the script again to resume", flush=True)
            _save(seen, done)
            continue
        added = 0
        for el in payload.get("elements", []):
            if el.get("type") == "way" and int(el["id"]) not in seen:
                seen[int(el["id"])] = el
                added += 1
        done.add(key)
        _save(seen, done)
        print(f"tile {key} +{added} total {len(seen)}", flush=True)
        time.sleep(8)
    print(f"complete ways={len(seen)}", flush=True)


if __name__ == "__main__":
    main()
