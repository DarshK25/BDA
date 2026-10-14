"""Learned 60-second-ahead congestion forecaster (Spark MLlib GBTRegressor).

Why: a plain trend extrapolation was no better than "assume nothing changes". Real congestion
is spatio-temporal - a jam grows over time AND spills back onto connected roads - so the
model sees both:
    own history        speed ratio and occupancy now, 15 s, 30 s and 45 s ago
    neighbourhood      average / worst speed ratio of the roads that share a junction
    weather            rain, visibility
and predicts the hidden congestion index c, 60 s ahead.

Training data comes from a fast offline run of the same simulator (cgls.simulator.producer.World)
with random accidents and rain, so no real-time waiting is needed:   python -m cgls.ml.forecaster

(An LSTM could be trained on exactly the same lag table; see README "Extensions". We use gradient
boosted trees because they run inside Spark with no extra deep-learning stack.)
"""
from __future__ import annotations

import json
import os
import random
import sys

import numpy as np
import pandas as pd

from cgls import config
from cgls import traffic_model as tm
from cgls.features import compute_features
from cgls.network import SEGMENTS, neighbours

STEP_S = 5
LAGS = (3, 6, 9)                      # in steps -> 15 s, 30 s, 45 s
HORIZON_STEPS = config.FORECAST_HORIZON_S // STEP_S
FORECAST_FEATURES = (["speed_ratio", "occupancy_pct"]
                     + [f"speed_ratio_l{l * STEP_S}" for l in LAGS]
                     + [f"occupancy_l{l * STEP_S}" for l in LAGS]
                     + ["rain_mm_hr", "visibility_km", "nb_mean_ratio", "nb_min_ratio"])
NEIGHBOURS = {s["segment_id"]: neighbours(s["segment_id"]) for s in SEGMENTS}


# ----------------------------------------------------------------------------- feature table
def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """df needs: segment_id, window_end_ms, speed_ratio, occupancy_pct, rain_mm_hr, visibility_km.
    Adds lag columns and neighbour columns. Rows without a complete history get NaN lags."""
    df = df.sort_values(["segment_id", "window_end_ms"]).reset_index(drop=True)
    key = df.set_index(["segment_id", "window_end_ms"])
    for l in LAGS:
        idx = pd.MultiIndex.from_arrays([df["segment_id"], df["window_end_ms"] - l * STEP_S * 1000])
        df[f"speed_ratio_l{l * STEP_S}"] = key["speed_ratio"].reindex(idx).to_numpy()
        df[f"occupancy_l{l * STEP_S}"] = key["occupancy_pct"].reindex(idx).to_numpy()
    by_time = {}
    for (end, seg), r in df.set_index(["window_end_ms", "segment_id"])["speed_ratio"].items():
        by_time.setdefault(end, {})[seg] = r
    nb_mean, nb_min = [], []
    for seg, end in zip(df["segment_id"], df["window_end_ms"]):
        vals = [by_time[end][n] for n in NEIGHBOURS[seg] if n in by_time.get(end, {})]
        nb_mean.append(float(np.mean(vals)) if vals else np.nan)
        nb_min.append(float(np.min(vals)) if vals else np.nan)
    df["nb_mean_ratio"], df["nb_min_ratio"] = nb_mean, nb_min
    return df


# ----------------------------------------------------------------------------- offline simulation
def simulate_run(seed: int, duration_s: int = 1500) -> pd.DataFrame:
    """One fast (no sleeping) simulated hour-ish with random accidents and rain.
    Returns one row per segment per 5 s window with features and target `c_future`."""
    from cgls.simulator.producer import World, parse_args       # local import: avoids a cycle at module load

    rng = random.Random(seed)
    scenario = rng.choice(["quiet", "normal", "normal", "rush"])
    args = parse_args(["--no-events", "--vehicles", "0", "--scenario", scenario])
    world = World(args, rng)
    world.incidents = [(rng.choice(SEGMENTS)["segment_id"], rng.uniform(40, duration_s - 300), rng.uniform(90, 300))
                       for _ in range(rng.randint(1, 4))]
    world.rains = [(rng.uniform(0, duration_s - 200), rng.uniform(120, 300), rng.uniform(4, 25))
                   for _ in range(rng.randint(0, 2))]

    c_hist = {s["segment_id"]: [] for s in SEGMENTS}
    rain_hist = []
    for t in range(duration_s):
        world.step(t)
        rain_hist.append(world.rain)
        for sid, c in world.c.items():
            c_hist[sid].append(c)

    rows = []
    for end in range(30, duration_s - config.FORECAST_HORIZON_S, STEP_S):
        win = range(end - 30, end)
        r_win = [rain_hist[i] for i in win]
        for s in SEGMENTS:
            sid, hist = s["segment_id"], c_hist[s["segment_id"]]
            n_gps = 0 if rng.random() < 0.02 else rng.randint(20, 80)
            speeds = [tm.sample_speed(s["free_flow_kmph"], hist[rng.choice(win)], rng) for _ in range(n_gps)]
            occ = [tm.sample_occupancy(hist[i], rng) for i in win[::5]]
            cnt = [tm.sample_vehicle_count(s["lanes"], hist[i], rng) for i in win[::5]]
            rn = [max(0.0, r_win[i] + rng.gauss(0, 0.2)) for i in (0, 10, 20)]
            vis = [tm.sample_visibility(r_win[i], rng) for i in (0, 10, 20)]
            f = compute_features(np.mean(speeds) if speeds else None, s["free_flow_kmph"], np.mean(occ),
                                 np.mean(cnt), s["lanes"], np.mean(rn), np.mean(vis))
            rows.append({"run": seed, "segment_id": sid, "window_end_ms": end * 1000,
                         "speed_ratio": f["speed_ratio"], "occupancy_pct": f["occupancy_pct"],
                         "rain_mm_hr": f["rain_mm_hr"], "visibility_km": f["visibility_km"],
                         "c_now": hist[end - 1], "c_future": hist[end - 1 + config.FORECAST_HORIZON_S]})
    return pd.DataFrame(rows)


def build_table(seeds) -> pd.DataFrame:
    parts = []
    for sd in seeds:
        d = simulate_run(sd)
        parts.append(add_lag_features(d))
    return pd.concat(parts, ignore_index=True).dropna(subset=FORECAST_FEATURES)


# ----------------------------------------------------------------------------- train / evaluate
def est_c(speed_ratio: pd.Series) -> pd.Series:
    """Naive current congestion from speed alone (inverse of speed = ff * (1 - 0.8 c))."""
    return ((1.0 - speed_ratio) / 0.8).clip(0, 1)


def train_and_save(spark, n_runs: int = 24, model_dir: str = config.FORECAST_MODEL_DIR,
                   metrics_path: str = config.FORECAST_METRICS_PATH) -> dict:
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import GBTRegressor

    print(f"[forecaster] simulating {n_runs} training runs + 6 held-out runs ...", flush=True)
    train_pdf = build_table(range(1000, 1000 + n_runs))
    test_pdf = build_table(range(5000, 5006))                       # different seeds = unseen worlds
    print(f"[forecaster] {len(train_pdf)} training rows, {len(test_pdf)} held-out rows", flush=True)

    assembler = VectorAssembler(inputCols=FORECAST_FEATURES, outputCol="features", handleInvalid="keep")
    gbt = GBTRegressor(featuresCol="features", labelCol="c_future", maxIter=60, maxDepth=5, stepSize=0.1, seed=3)
    model = Pipeline(stages=[assembler, gbt]).fit(spark.createDataFrame(train_pdf[FORECAST_FEATURES + ["c_future"]]))

    pred = model.transform(spark.createDataFrame(test_pdf[FORECAST_FEATURES + ["c_future"]])).select("prediction").toPandas()["prediction"].to_numpy()
    y = test_pdf["c_future"].to_numpy()
    persist = est_c(test_pdf["speed_ratio"]).to_numpy()
    slope = (est_c(test_pdf["speed_ratio"]) - est_c(test_pdf["speed_ratio_l30"])).to_numpy() / 30.0
    trend = np.clip(persist + config.FORECAST_DAMPING * slope * config.FORECAST_HORIZON_S, 0, 1)
    lv = np.vectorize(tm.level_from_score)

    def scores(p):
        p = np.clip(p, 0, 1)
        return {"rmse": round(float(np.sqrt(np.mean((p - y) ** 2))), 4),
                "level_accuracy": round(float(np.mean(lv(p) == lv(y))), 4),
                "within_one_level": round(float(np.mean(np.abs(lv(p) - lv(y)) <= 1)), 4)}

    imp = dict(zip(FORECAST_FEATURES, [round(float(x), 4) for x in model.stages[-1].featureImportances.toArray()]))
    metrics = {"horizon_s": config.FORECAST_HORIZON_S, "n_train": int(len(train_pdf)), "n_test": int(len(test_pdf)),
               "gbt_forecaster": scores(pred), "baseline_persistence": scores(persist),
               "baseline_linear_trend": scores(trend), "feature_importances": imp,
               "note": "held-out simulated worlds (different random seeds); synthetic data"}
    os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
    model.write().overwrite().save(model_dir)
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    print("\n[forecaster] ====== 60 s AHEAD, held-out worlds ======")
    for k in ("gbt_forecaster", "baseline_persistence", "baseline_linear_trend"):
        print(f"[forecaster] {k:24s} {metrics[k]}")
    print(f"[forecaster] top features: {sorted(imp.items(), key=lambda kv: -kv[1])[:5]}")
    print(f"[forecaster] saved to {model_dir}\n", flush=True)
    return metrics


def main() -> int:
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName("cgls-forecaster").master(os.getenv("SPARK_MASTER", "local[2]"))
             .config("spark.sql.shuffle.partitions", "4").config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    train_and_save(spark, n_runs=int(os.getenv("CGLS_FORECAST_RUNS", "24")))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
