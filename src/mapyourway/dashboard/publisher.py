"""Keep the map's data file fresh without reloading the map.

Streamlit would otherwise rebuild the page every few seconds and throw away
whatever the person had panned to. This thread only rewrites view.json.
The page's JavaScript polls that file.
"""
from __future__ import annotations

import json
import os
import threading
import time
import traceback

from mapyourway import config
from mapyourway.network import SEGMENT_BY_ID
from mapyourway.routing import compare

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
VIEW_PATH = os.path.join(STATIC_DIR, "view.json")
CONFIG_PATH = os.path.join(STATIC_DIR, "config.json")

_started = False
_lock = threading.Lock()

COLORS = {
    "low": "#1f8f4d",
    "medium": "#c89612",
    "high": "#e06a12",
    "severe": "#c62828",
}
WIDTH = {
    "motorway": 5.0,
    "motorway_link": 3.0,
    "trunk": 4.5,
    "trunk_link": 3.0,
    "primary": 3.2,
    "primary_link": 2.4,
    "secondary": 2.4,
    "secondary_link": 2.0,
    "tertiary": 1.6,
    "tertiary_link": 1.4,
}
HALO = ["#143d8f", "#6d28d9", "#0f766e", "#9a3412"]
ASSUMED_CLEAR = "#7dcea0"


def _write(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temporary = path + ".tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"))
    os.replace(temporary, path)


def _stitch(segment_ids: list[str]) -> list[list[float]]:
    line: list[list[float]] = []
    for segment_id in segment_ids:
        segment = SEGMENT_BY_ID.get(segment_id)
        if not segment:
            continue
        coords = [[point[1], point[0]] for point in segment["coords"]]
        if line:
            if _near(line[-1], coords[-1]) < _near(line[-1], coords[0]):
                coords.reverse()
        if line and coords and line[-1] == coords[0]:
            line.extend(coords[1:])
        else:
            line.extend(coords)
    return line


def _near(a: list[float], b: list[float]) -> float:
    return (a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2


def publish_once() -> None:
    latest = {}
    if os.path.exists(config.LATEST_JSON):
        with open(config.LATEST_JSON, encoding="utf-8") as handle:
            latest = json.load(handle)
    ui = {"origin": "Borivali", "destination": "Colaba"}
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, encoding="utf-8") as handle:
            ui.update(json.load(handle))

    segments_out = {}
    speeds = {}
    for segment_id, row in (latest.get("segments") or {}).items():
        source = row.get("speed_source") or "urban_default"
        measured = source in {"tomtom_direct", "tomtom_same_road"}
        level = row.get("level") or "low"
        road_class = row.get("road_class") or "tertiary"
        segments_out[segment_id] = {
            "color": COLORS.get(level, "#9aa3ad") if measured else "#4d5966",
            "opacity": 0.95 if measured else 0.55,
            "width": WIDTH.get(road_class, 1.5) if measured else 1.6,
            "speed": row.get("speed_kmph"),
            "level": level if measured else "not measured",
            "source": source,
            "name": row.get("name") or SEGMENT_BY_ID.get(segment_id, {}).get("name", segment_id),
        }
        if row.get("speed_kmph") is not None:
            speeds[segment_id] = float(row["speed_kmph"])

    route = None
    origin, dest = ui.get("origin"), ui.get("destination")
    if speeds and origin and dest and origin != dest:
        try:
            result = compare(origin, dest, speeds, corridor_count=4)
            corridors_out = []
            on_route: set[str] = set()
            for index, corridor in enumerate(result["corridors"][:4]):
                if corridor.segment_ids == result["best"].segment_ids:
                    role = "fastest"
                elif corridor.segment_ids == result["usual"].segment_ids:
                    role = "usual"
                else:
                    role = "alternate"
                corridors_out.append(
                    {
                        "role": role,
                        "minutes": round(corridor.minutes, 1),
                        "km": round(corridor.km, 1),
                        "label": corridor.label(),
                        "color": HALO[index % len(HALO)],
                        "line": _stitch(corridor.segment_ids),
                    }
                )
                on_route.update(corridor.segment_ids)
            for segment_id in on_route:
                segment = SEGMENT_BY_ID.get(segment_id)
                if segment_id in segments_out:
                    segments_out[segment_id]["width"] = max(float(segments_out[segment_id]["width"]), 3.4)
                    continue
                segments_out[segment_id] = {
                    "color": ASSUMED_CLEAR,
                    "opacity": 0.95,
                    "width": 2.8,
                    "speed": None if segment is None else segment["free_flow_kmph"],
                    "level": "low",
                    "source": "free_flow",
                    "name": segment_id if segment is None else segment["name"],
                }
            # Keep the city network visible in neutral gray, but reserve traffic
            # colours for the roads that belong to one of the displayed drives.
            # sampled_total still reports all live measurements in the batch.
            segments_out = {
                segment_id: info for segment_id, info in segments_out.items() if segment_id in on_route
            }
            route = {
                "origin": origin,
                "destination": dest,
                "usual_min": round(result["usual"].minutes, 1),
                "best_min": round(result["best"].minutes, 1),
                "saving_min": round(max(result["saving_min"], 0.0), 1),
                "reroute": bool(result["reroute"]),
                "usual_label": result["usual"].label(),
                "best_label": result["best"].label(),
                "usual_line": _stitch(result["usual"].segment_ids),
                "best_line": _stitch(result["best"].segment_ids),
                "corridors": corridors_out,
                "expanded_astar": result["expanded_astar"],
                "expanded_dijkstra": result["expanded_dijkstra"],
            }
        except Exception as exc:  # noqa: BLE001
            route = {"origin": origin, "destination": dest, "error": str(exc)}

    _write(
        VIEW_PATH,
        {
            "updated_at": latest.get("updated_at"),
            "epoch": latest.get("epoch"),
            "weather": latest.get("weather"),
            "model": latest.get("model"),
            "sampled_total": sum(
                1
                for row in (latest.get("segments") or {}).values()
                if row.get("speed_source") in {"tomtom_direct", "tomtom_same_road"}
            ),
            "segments": segments_out,
            "route": route,
        },
    )


def _loop() -> None:
    while True:
        try:
            publish_once()
        except Exception:
            traceback.print_exc()
        time.sleep(3)


def start() -> None:
    global _started
    with _lock:
        if _started:
            return
        _started = True
    threading.Thread(target=_loop, name="map-publisher", daemon=True).start()
