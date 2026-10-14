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
ASSUMED_CLEAR = "#c3cad2"
# Speed as a share of free-flow. Spark's level bins are coarse (anything above
# 70% of free-flow is "low"), so the route pieces use this finer scale instead.
RATIO_BANDS = [
    (0.90, "#1a9850", "flowing"),
    (0.80, "#a6d96a", "slightly slow"),
    (0.70, "#fdae61", "slow"),
    (0.55, "#f46d43", "very slow"),
    (0.00, "#d73027", "jammed"),
]


def _ratio_band(speed: float | None, free_flow: float | None) -> tuple[str, str, float | None]:
    if not speed or not free_flow:
        return ASSUMED_CLEAR, "no sample", None
    ratio = max(0.0, min(1.0, float(speed) / float(free_flow)))
    for floor, color, label in RATIO_BANDS:
        if ratio >= floor:
            return color, label, ratio
    return RATIO_BANDS[-1][1], RATIO_BANDS[-1][2], ratio


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


def _jam_factor(lat: float) -> float:
    """Worst near Andheri and Bandra, easing towards both ends."""
    if lat > 19.18:
        return 0.75
    if lat > 19.13:
        return 0.6
    if lat > 19.05:
        return 0.3
    return 0.5


def _apply_weh_jam(segments_out: dict, speeds: dict[str, float]) -> None:
    """Demo what-if, switched on from the sidebar. Never written back to Kafka or Spark."""
    for segment_id, segment in SEGMENT_BY_ID.items():
        name = segment["name"].lower()
        if "western express" not in name and "sea link" not in name:
            continue
        free_flow = float(segment["free_flow_kmph"])
        speed = round(free_flow * _jam_factor(float(segment["mid_lat"])), 1)
        band_color, band, ratio = _ratio_band(speed, free_flow)
        speeds[segment_id] = speed
        segments_out[segment_id] = {
            "band_color": band_color,
            "band": band,
            "ratio": round(ratio, 2),
            "color": band_color,
            "opacity": 0.95,
            "width": WIDTH.get(segment.get("road_class"), 3.0),
            "speed": speed,
            "level": "simulated",
            "source": "simulated_jam",
            "name": segment["name"],
        }


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
        band_color, band, ratio = _ratio_band(row.get("speed_kmph"), row.get("free_flow_kmph"))
        segments_out[segment_id] = {
            "band_color": band_color if measured else ASSUMED_CLEAR,
            "band": band if measured else "no sample",
            "ratio": None if ratio is None or not measured else round(ratio, 2),
            "color": band_color if measured else "#4d5966",
            "opacity": 0.95 if measured else 0.55,
            "width": WIDTH.get(road_class, 1.5) if measured else 1.6,
            "speed": row.get("speed_kmph"),
            "level": level if measured else "not measured",
            "source": source,
            "name": row.get("name") or SEGMENT_BY_ID.get(segment_id, {}).get("name", segment_id),
        }
        if row.get("speed_kmph") is not None:
            speeds[segment_id] = float(row["speed_kmph"])

    if ui.get("scenario") == "weh_jam":
        _apply_weh_jam(segments_out, speeds)

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
                        "segment_ids": list(corridor.segment_ids),
                    }
                )
                on_route.update(corridor.segment_ids)
            for segment_id in on_route:
                segment = SEGMENT_BY_ID.get(segment_id)
                if segment_id in segments_out:
                    segments_out[segment_id]["width"] = max(float(segments_out[segment_id]["width"]), 3.4)
                    continue
                segments_out[segment_id] = {
                    "band_color": ASSUMED_CLEAR,
                    "band": "no sample",
                    "ratio": None,
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
            for item in corridors_out:
                ids = item.pop("segment_ids")
                item["pieces"] = [
                    {
                        "color": segments_out[segment_id]["band_color"],
                        "band": segments_out[segment_id]["band"],
                        "line": _stitch([segment_id]),
                    }
                    for segment_id in ids
                    if segment_id in segments_out
                ]
                bands: dict[str, float] = {}
                for segment_id in ids:
                    segment = SEGMENT_BY_ID.get(segment_id)
                    info = segments_out.get(segment_id)
                    if segment and info:
                        bands[info["band"]] = bands.get(info["band"], 0.0) + segment["length_km"]
                item["band_km"] = {name: round(km, 1) for name, km in bands.items()}
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
            "scenario": ui.get("scenario"),
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
