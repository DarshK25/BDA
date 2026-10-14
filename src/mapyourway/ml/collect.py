"""Collect real TomTom/Open-Meteo observations for offline training.

The collector stores observed values and explicitly labels any congestion class
derived from the speed-ratio rule.  Such rows are useful for bootstrapping the
pipeline, but ``train.py`` refuses them by default because they are not
independent ground truth.

Example::

    python -m mapyourway.ml.collect --output /data/raw/tomtom_flow.csv --once
"""

from __future__ import annotations

import argparse
import csv
import os
import time
from datetime import datetime, timezone

from mapyourway import config
from mapyourway.network import SEGMENTS
from mapyourway.simulator.real_producer import (
    fetch_and_publish_traffic,
    fetch_open_meteo_weather,
    get_tomtom_api_key,
)


class NullProducer:
    def send(self, topic, key, value):
        return None

    def flush(self):
        return None


def collect_once(output_path: str, api_key: str | None = None) -> int:
    api_key = api_key or get_tomtom_api_key()
    now_ms = int(time.time() * 1000)
    snapshot_cache: dict[str, dict] = {}
    fetch_and_publish_traffic(
        NullProducer(),
        api_key=api_key,
        now_ms=now_ms,
        segments=SEGMENTS,
        snapshot_cache=snapshot_cache,
    )
    try:
        weather = fetch_open_meteo_weather(now_ms=now_ms)
    except Exception:
        weather = {
            "rain_mm_hr": None,
            "visibility_km": None,
            "observed_at_ms": None,
        }

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    rows = []
    observed_at = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat()
    for segment in SEGMENTS:
        snapshot = snapshot_cache.get(segment["segment_id"])
        if snapshot is None:
            continue
        ratio = snapshot["current_speed"] / max(snapshot["free_speed"], 1.0)
        occupancy = max(0.0, min(100.0, (1.0 - ratio) * config.OCCUPANCY_SPEED_FACTOR))
        rows.append(
            {
                "timestamp": observed_at,
                "segment_id": segment["segment_id"],
                "road_name": segment["name"],
                "current_speed_kmph": snapshot["current_speed"],
                "free_flow_kmph": snapshot["free_speed"],
                "speed_ratio": ratio,
                "occupancy_pct": occupancy,
                "veh_per_lane": config.BASE_VEHICLES_PER_LANE
                + config.MAX_ADDITIONAL_VEHICLES_PER_LANE * occupancy / 100.0,
                "rain_mm_hr": weather["rain_mm_hr"],
                "visibility_km": weather["visibility_km"],
                "road_closure": snapshot["road_closure"],
                "source_confidence": snapshot["confidence"],
                "observed_at_ms": snapshot["observed_at_ms"],
                "congestion_level": (
                    0 if ratio >= 0.8 else 1 if ratio >= 0.5 else 2 if ratio >= 0.3 else 3
                ),
                "label_source": "derived_speed_rule",
                "source": "tomtom_flow_segment",
            }
        )
    if not rows:
        return 0
    fieldnames = list(rows[0])
    exists = os.path.exists(output_path)
    with open(output_path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=os.path.join(config.RAW_DIR, "tomtom_flow.csv"))
    parser.add_argument("--interval", type=int, default=config.TOMTOM_POLL_INTERVAL_SECONDS)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--max-polls", type=int, default=0)
    args = parser.parse_args()
    polls = 0
    while True:
        rows = collect_once(args.output)
        polls += 1
        print(f"Collected {rows} real observations into {args.output}", flush=True)
        if args.once or (args.max_polls and polls >= args.max_polls):
            return 0
        time.sleep(max(30, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
