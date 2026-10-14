"""Create the runnable bootstrap model used on a fresh installation.

A fresh deployment has no labelled historical traffic yet.  This command trains
an actual Spark ML pipeline from a broad, physically calibrated scenario table
so the live stream can start with model inference rather than silently relying
on an unavailable model.  The labels are domain calibration labels derived from
speed-ratio bands; they are *not* an independent accuracy claim.  Replace this
model with labelled municipal/fleet data using ``mapyourway.ml.train`` when it
is available.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import Row

from mapyourway import config
from mapyourway.ml.train import create_spark_session, train_classifier


def build_calibration_rows(n_per_class: int = 1000, seed: int = 42) -> list[dict]:
    """Build varied, bounded traffic scenarios for the startup model."""
    rng = random.Random(seed)
    rows: list[dict] = []
    bands = ((0.82, 0.99), (0.55, 0.79), (0.32, 0.54), (0.08, 0.31))
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    for level, (low, high) in enumerate(bands):
        for index in range(n_per_class):
            ratio = rng.uniform(low, high)
            occupancy = max(0.0, min(100.0, (1.0 - ratio) * 120.0 + rng.gauss(0, 2.5)))
            veh_per_lane = max(
                0.0,
                3.0 + 9.0 * occupancy / 100.0 + rng.gauss(0, 0.25),
            )
            rain = max(0.0, min(30.0, rng.expovariate(1 / 3.0)))
            visibility = max(0.5, min(10.0, rng.gauss(8.0 - rain * 0.08, 1.4)))
            rows.append(
                {
                    "segment_id": f"BOOTSTRAP-{index % 19 + 1:02d}",
                    "speed_ratio": ratio,
                    "occupancy_pct": occupancy,
                    "veh_per_lane": veh_per_lane,
                    "rain_mm_hr": rain,
                    "visibility_km": visibility,
                    "congestion_level": float(level),
                    "label_source": "bootstrap_calibration",
                    "timestamp": start + timedelta(minutes=len(rows)),
                }
            )
    rng.shuffle(rows)
    # The calibration table is scenario-balanced, not a historical timeline.
    # Give it a neutral shuffled timestamp order so chronological splitting does
    # not accidentally put an entire class in the test tail.
    for index, row in enumerate(rows):
        row["timestamp"] = start + timedelta(minutes=index)
    return rows


def ensure_bootstrap_model(
    model_path: str = config.MODEL_PATH,
    metrics_path: str = config.METRICS_JSON,
    force: bool = False,
    n_per_class: int = 1000,
    algorithm: str = "random_forest",
) -> dict:
    """Train once if no model exists and return the recorded metrics."""
    metrics_file = metrics_path
    if os.path.exists(model_path) and not force:
        if os.path.exists(metrics_file):
            with open(metrics_file, "r", encoding="utf-8") as handle:
                return json.load(handle)
        return {"model_path": model_path, "status": "already_exists"}

    config.ensure_directories()
    spark = create_spark_session()
    try:
        rows = build_calibration_rows(n_per_class=n_per_class)
        frame = spark.createDataFrame(rows)
        _, metrics = train_classifier(
            frame,
            model_path,
            metrics_path,
            allow_derived_labels=True,
            algorithm=algorithm,
        )
        metrics["training_data"] = "bootstrap_calibration_scenarios"
        metrics["model_role"] = "runnable_startup_model_not_independent_ground_truth"
        with open(metrics_path, "w", encoding="utf-8") as handle:
            json.dump(metrics, handle, indent=2)
        return metrics
    finally:
        spark.stop()


def ensure_bootstrap_models(
    force: bool = False, n_per_class: int = 1000
) -> dict[str, dict]:
    """Train both supported classifiers in one Spark session.

    The RandomForest is used by the live stream.  The GBT artifact is retained
    for offline comparison and later promotion after independent labels are
    available.
    """
    config.ensure_directories()
    targets = (
        ("random_forest", config.MODEL_PATH, config.METRICS_JSON),
        ("gbt", config.GBT_MODEL_PATH, config.GBT_METRICS_JSON),
    )
    if not force and all(os.path.exists(model) for _, model, _ in targets):
        result = {}
        for algorithm, model, metrics_path in targets:
            if os.path.exists(metrics_path):
                with open(metrics_path, "r", encoding="utf-8") as handle:
                    result[algorithm] = json.load(handle)
            else:
                result[algorithm] = {"model_path": model, "status": "already_exists"}
        return result

    spark = create_spark_session()
    try:
        frame = spark.createDataFrame(build_calibration_rows(n_per_class=n_per_class))
        result: dict[str, dict] = {}
        for algorithm, model_path, metrics_path in targets:
            if not force and os.path.exists(model_path) and os.path.exists(metrics_path):
                with open(metrics_path, "r", encoding="utf-8") as handle:
                    result[algorithm] = json.load(handle)
                continue
            _, metrics = train_classifier(
                frame,
                model_path,
                metrics_path,
                allow_derived_labels=True,
                algorithm=algorithm,
            )
            metrics["training_data"] = "bootstrap_calibration_scenarios"
            metrics["model_role"] = (
                "runnable_startup_model_not_independent_ground_truth"
            )
            with open(metrics_path, "w", encoding="utf-8") as handle:
                json.dump(metrics, handle, indent=2)
            result[algorithm] = metrics
        return result
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-path", default=config.MODEL_PATH)
    parser.add_argument("--metrics-path", default=config.METRICS_JSON)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--rows-per-class", type=int, default=1000)
    parser.add_argument(
        "--algorithm",
        choices=("random_forest", "gbt", "both"),
        default="both",
        help="Train the serving RandomForest, the comparison GBT, or both.",
    )
    args = parser.parse_args()
    force = args.force or os.getenv("FORCE_MODEL_TRAINING", "0").lower() in {
        "1", "true", "yes"
    }
    if args.algorithm == "both" and args.model_path == config.MODEL_PATH:
        metrics = ensure_bootstrap_models(force=force, n_per_class=args.rows_per_class)
    else:
        algorithm = args.algorithm
        metrics = ensure_bootstrap_model(
            args.model_path,
            args.metrics_path,
            force=force,
            n_per_class=args.rows_per_class,
            algorithm=algorithm,
        )
    print(json.dumps(metrics, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
