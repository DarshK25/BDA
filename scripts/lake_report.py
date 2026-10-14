"""Batch summary of the Parquet lake that Spark writes every micro-batch.

Run inside the app image:
  docker run --rm -v ${PWD}/data:/data -v ${PWD}/src:/app/src -v ${PWD}/scripts:/app/scripts \
    -e PYTHONPATH=/app/src mapyourway-app python /app/scripts/lake_report.py
"""
from __future__ import annotations

import glob
import json
import os

import pandas as pd

LAKE = os.environ.get("LAKE_DIR", "/data/lake/segment_congestion")
OUT = os.environ.get("REPORT_JSON", "/data/reports/lake_summary.json")


def main() -> None:
    files = sorted(glob.glob(os.path.join(LAKE, "*.parquet")))
    if not files:
        print("no parquet files yet")
        return
    frame = pd.concat((pd.read_parquet(path) for path in files), ignore_index=True)
    frame["time_ist"] = pd.to_datetime(frame["ts_ms"], unit="ms", utc=True).dt.tz_convert("Asia/Kolkata")
    frame["ratio"] = (frame["speed_kmph"] / frame["free_flow_kmph"]).clip(upper=1.0)

    by_road = (
        frame.groupby("name")
        .agg(rows=("segment_id", "size"), segments=("segment_id", "nunique"),
             avg_speed=("speed_kmph", "mean"), avg_ratio=("ratio", "mean"),
             avg_congestion=("congestion", "mean"), max_congestion=("congestion", "max"))
        .sort_values("avg_congestion", ascending=False)
    )
    by_epoch = frame.groupby("epoch").agg(segments=("segment_id", "nunique"),
                                          avg_congestion=("congestion", "mean"))
    by_hour = frame.groupby(frame["time_ist"].dt.strftime("%H:00")).agg(
        rows=("segment_id", "size"), avg_congestion=("congestion", "mean"), avg_ratio=("ratio", "mean")
    )

    summary = {
        "files": len(files),
        "rows": int(len(frame)),
        "distinct_segments": int(frame["segment_id"].nunique()),
        "distinct_roads": int(frame["name"].nunique()),
        "epochs": [int(frame["epoch"].min()), int(frame["epoch"].max())],
        "first_ist": frame["time_ist"].min().isoformat(),
        "last_ist": frame["time_ist"].max().isoformat(),
        "columns": list(frame.columns.drop(["time_ist", "ratio"])),
        "levels": frame["level"].value_counts().to_dict(),
        "speed_source": frame["speed_source"].value_counts().to_dict(),
        "avg_segments_per_epoch": round(float(by_epoch["segments"].mean()), 1),
        "by_hour_ist": by_hour.round(3).reset_index().to_dict(orient="records"),
        "slowest_roads": by_road.head(10).round(3).reset_index().to_dict(orient="records"),
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, default=str)
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
