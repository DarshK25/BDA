"""Measure the pipeline against the simulator's hidden ground truth.

Run (after the pipeline has been running for a few minutes):
    docker compose run --rm --no-deps app python -m cgls.ml.evaluate

Compares what Spark wrote to the Parquet lake with data/ground_truth/gt.csv:
  * NOW accuracy      estimated level vs true level over the same 30 s window
  * 60 s forecast     forecast level vs the true level 60 s later
  * persistence base  "tomorrow = today" guess (forecast = current level), to show whether the
                      trend-based forecast is actually better than doing nothing
  * freshness         emitted_ms - window_end_ms  (how old a result is when it is published)
"""
from __future__ import annotations

import glob
import json
import os
import sys

import numpy as np
import pandas as pd

from cgls import config
from cgls.traffic_model import LEVEL_NAMES, level_from_score


def load(lake_dir: str, gt_path: str):
    files = glob.glob(os.path.join(lake_dir, "**", "*.parquet"), recursive=True)
    if not files:
        raise SystemExit(f"No Parquet files under {lake_dir} - is the Spark job running?")
    lake = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    lake = lake.sort_values("emitted_ms").drop_duplicates(["segment_id", "window_end_ms"], keep="last")
    gt = pd.read_csv(gt_path)
    return lake, gt


def true_level(gt: pd.DataFrame, seg: str, end_ms: int, span_ms: int = 30000):
    g = gt[(gt.segment_id == seg) & (gt.ts_ms > end_ms - span_ms) & (gt.ts_ms <= end_ms)]
    return None if g.empty else level_from_score(float(g["c"].mean()))


def evaluate(lake: pd.DataFrame, gt: pd.DataFrame, horizon_ms: int = 60000) -> dict:
    gt_by_seg = {s: d for s, d in gt.groupby("segment_id")}
    now_true, fc_true = [], []
    for r in lake.itertuples():
        d = gt_by_seg.get(r.segment_id)
        now_true.append(None if d is None else true_level(d.assign(segment_id=r.segment_id), r.segment_id, int(r.window_end_ms)))
        fc_true.append(None if d is None else true_level(d.assign(segment_id=r.segment_id), r.segment_id, int(r.window_end_ms) + horizon_ms))
    lake = lake.assign(true_now=now_true, true_fc=fc_true)

    a = lake.dropna(subset=["true_now"])
    b = lake.dropna(subset=["true_fc"])
    acc_now = float((a["level"] == a["true_now"]).mean()) if len(a) else float("nan")
    # "within one level" is the practical metric: confusing HEAVY with SEVERE is a small error
    near_now = float(((a["level"] - a["true_now"]).abs() <= 1).mean()) if len(a) else float("nan")
    acc_fc = float((b["forecast_level"] == b["true_fc"]).mean()) if len(b) else float("nan")
    acc_persist = float((b["level"] == b["true_fc"]).mean()) if len(b) else float("nan")

    cm = pd.crosstab(a["true_now"].astype(int).map(lambda i: LEVEL_NAMES[i]),
                     a["level"].astype(int).map(lambda i: LEVEL_NAMES[i]))
    fresh = (lake["emitted_ms"] - lake["window_end_ms"]) / 1000.0
    span_s = (lake["window_end_ms"].max() - lake["window_end_ms"].min()) / 1000.0
    expected = int(round(span_s / 5.0)) + 1                       # one window every 5 s per segment
    missing = int(sum(max(0, expected - d["window_end_ms"].nunique()) for _, d in lake.groupby("segment_id")))
    ends = np.sort(lake["window_end_ms"].unique())
    seq_gaps = int(np.sum(np.diff(ends) != 5000)) if len(ends) > 1 else 0        # 0 = no 5-second step is missing anywhere
    return {
        "windows_scored": int(len(a)),
        "accuracy_now": round(acc_now, 4),
        "within_one_level_now": round(near_now, 4),
        "windows_with_forecast_truth": int(len(b)),
        "accuracy_forecast_60s": round(acc_fc, 4),
        "accuracy_persistence_baseline_60s": round(acc_persist, 4),
        "freshness_seconds_p50": round(float(np.percentile(fresh, 50)), 2),
        "freshness_seconds_p95": round(float(np.percentile(fresh, 95)), 2),
        "run_length_seconds": round(float(span_s), 1),
        "window_sequence_gaps": seq_gaps,           # 0 = every 5 s step present (use this after a Spark restart)
        "missing_segment_windows": missing,         # a few at the very start/end are normal (road had no data in that window)
        "confusion_now": json.loads(cm.to_json()),
        "_cm_text": cm.to_string(),
    }


def main() -> int:
    lake, gt = load(config.LAKE_DIR, os.path.join(config.GROUND_TRUTH_DIR, "gt.csv"))
    res = evaluate(lake, gt)
    print("\n=========== PIPELINE EVALUATION (vs simulator ground truth) ===========")
    for k, v in res.items():
        if not k.startswith("_") and k != "confusion_now":
            print(f"{k:38s}: {v}")
    print("\nConfusion matrix (rows = true level, columns = estimated level):")
    print(res["_cm_text"])
    os.makedirs(config.REPORT_DIR, exist_ok=True)
    out = os.path.join(config.REPORT_DIR, "evaluation.json")
    with open(out, "w") as f:
        json.dump({k: v for k, v in res.items() if not k.startswith("_")}, f, indent=2)
    print(f"\nSaved {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
