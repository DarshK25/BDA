"""End-to-end: synthetic event files -> real Spark Structured Streaming job -> results.
Uses the file source/sink instead of Kafka, so no broker is needed. Everything else
(parsing, watermark, windows, MLlib model, trend, Parquet) is the production code path."""
import glob
import json
import os
import random
import subprocess
import sys
import time

import pandas as pd
import pytest

from cgls import config
from cgls.network import SEGMENTS
from cgls.simulator import producer

pytestmark = pytest.mark.spark
SECONDS = 100
INCIDENT_FROM, INCIDENT_TO = 40, 100


def make_events(directory, start_ms):
    args = producer.parse_args(["--vehicles", "400", "--no-events", "--dirty-rate", "0.02"])
    args.incident_after, args.incident_duration, args.incident_segment = INCIDENT_FROM, 999, "S05"
    rng = random.Random(3)
    world = producer.World(args, rng)
    sink = producer.FileSink(directory)
    for t in range(SECONDS):
        world.step(t)
        ms = start_ms + t * 1000
        for i in range(t % 5, len(world.vehicles), 5):
            m = world.ping(world.vehicles[i], ms + rng.randint(0, 900), 5)
            if rng.random() < 0.02:
                sink.send(config.TOPIC_GPS, None, b'{"vehicle_id": "x", "speed_kmph": ')   # corrupt JSON
                continue
            sink.send(config.TOPIC_GPS, m["segment_id"], m)
        if t % 5 == 0:
            for s in SEGMENTS:
                sink.send(config.TOPIC_SENSOR, s["segment_id"], world.sensor(s, ms + rng.randint(0, 900)))
            if t % 10 == 0:
                sink.send(config.TOPIC_WEATHER, "WX", world.weather(ms))
        sink.flush()


def test_job_end_to_end(tmp_path):
    ev, out, data, ckpt = (str(tmp_path / n) for n in ("ev", "out", "data", "ckpt"))
    make_events(ev, int(time.time() * 1000) - SECONDS * 1000 - 5000)
    env = dict(os.environ, PYTHONPATH="src", CGLS_SOURCE="files", CGLS_SINK="files", CGLS_FILES_IN=ev,
               CGLS_FILES_OUT=out, CGLS_DATA_DIR=data, CGLS_CHECKPOINT_DIR=ckpt, CGLS_AVAILABLE_NOW="1",
               CGLS_TRAIN_ROWS="8000", CGLS_FORECAST_RUNS="3", SPARK_LOCAL_IP="127.0.0.1")
    res = subprocess.run([sys.executable, "-m", "cgls.streaming.job"], env=env, capture_output=True, text=True, timeout=600)
    assert res.returncode == 0, res.stderr[-3000:]

    rows = [json.loads(l) for f in glob.glob(out + "/part-*") for l in open(f) if l.strip()]
    df = pd.DataFrame(rows)
    assert len(df) > 19 * 8, f"only {len(df)} window rows were emitted"
    assert df["segment_id"].nunique() == 19
    assert not df.duplicated(["segment_id", "window_end_ms"]).any(), "each window must be emitted once"
    assert df["level"].between(0, 3).all() and df["confidence"].between(0, 1).all()
    assert (df["window_end_ms"] - df["window_start_ms"] == 30000).all()

    # the accident on S05 must be detected, and a quiet road must not be flagged
    start = df["window_end_ms"].min()
    s05_late = df[(df.segment_id == "S05") & (df.window_end_ms >= start + (INCIDENT_FROM + 55) * 1000)]
    s01_all = df[df.segment_id == "S01"]
    assert len(s05_late) > 0 and (s05_late["level"] >= 2).mean() > 0.8
    assert (s01_all["level"] <= 1).mean() > 0.8

    # incident detection: S05 is flagged, quiet roads are (almost) never flagged
    assert df.loc[df.segment_id == "S05", "incident_suspected"].any(), "accident on S05 not flagged"
    others = df[(df.segment_id != "S05") & (df.segment_id.isin(["S01", "S02", "S10", "S18"]))]
    assert others["incident_suspected"].mean() < 0.05
    alerts = [json.loads(l) for f in glob.glob(out + "_alerts/part-*") for l in open(f) if l.strip()]
    assert any(a["segment_id"] == "S05" for a in alerts)

    # forecast column exists and is a valid level; GBT is used once 45 s of history exist
    assert df["forecast_level"].between(0, 3).all()
    assert (df["forecast_source"] == "gbt").sum() > 19 * 3

    # flat CSV for R analytics is written
    csv = pd.read_csv(os.path.join(data, "export", "congestion.csv"))
    assert len(csv) >= len(df) and {"segment_id", "level", "incident_suspected"} <= set(csv.columns)

    # R analytics (ggplot2 report) runs on the exported CSV - only when R is installed
    import shutil
    if shutil.which("Rscript"):
        r = subprocess.run(["Rscript", "r/analysis.R"], env=dict(os.environ, CGLS_DATA_DIR=data),
                           capture_output=True, text=True, timeout=300)
        assert r.returncode == 0, r.stderr[-2000:]
        for png in ("01_heatmap.png", "02_speed_vs_occupancy.png", "03_rain_effect.png", "07_freshness.png", "08_road_ranking.png"):
            assert os.path.getsize(os.path.join(data, "reports", "r", png)) > 5000

    # Parquet lake exists and is idempotently laid out (one folder per batch)
    parquet = glob.glob(os.path.join(data, "lake", "segment_congestion", "dt=*", "batch_id=*", "*.parquet"))
    assert parquet
    lake = pd.concat(pd.read_parquet(p) for p in parquet)
    assert len(lake) == len(df)


def test_duplicate_gps_messages_are_counted_once(tmp_path):
    """Every GPS ping is sent twice (a network retry). The 30 s window must still count each once."""
    ev, out, data, ckpt = (str(tmp_path / n) for n in ("ev", "out", "data", "ckpt"))
    sink = producer.FileSink(ev)
    t0 = (int(time.time()) // 5) * 5 - 120
    for sec in range(0, 70):
        ms = (t0 + sec) * 1000 + 100
        ping = {"vehicle_id": "V1", "segment_id": "S01", "lat": 19.2, "lon": 72.85, "speed_kmph": 30.0, "ts_ms": ms}
        sink.send(config.TOPIC_GPS, "S01", ping)
        sink.send(config.TOPIC_GPS, "S01", ping)                       # exact duplicate
        if sec % 5 == 0:
            sink.send(config.TOPIC_SENSOR, "S01", {"sensor_id": "x", "segment_id": "S01", "occupancy_pct": 40.0,
                                                   "vehicle_count": 9, "ts_ms": ms})
        if sec % 10 == 0:
            sink.send(config.TOPIC_WEATHER, "WX", {"station_id": "w", "rain_mm_hr": 0.0, "visibility_km": 10.0,
                                                   "temp_c": 29.0, "humidity_pct": 60.0, "ts_ms": ms})
        sink.flush()
    env = dict(os.environ, PYTHONPATH="src", CGLS_SOURCE="files", CGLS_SINK="files", CGLS_FILES_IN=ev,
               CGLS_FILES_OUT=out, CGLS_DATA_DIR=data, CGLS_CHECKPOINT_DIR=ckpt, CGLS_AVAILABLE_NOW="1",
               CGLS_TRAIN_ROWS="3000", CGLS_FORECASTER="off", SPARK_LOCAL_IP="127.0.0.1")
    res = subprocess.run([sys.executable, "-m", "cgls.streaming.job"], env=env, capture_output=True, text=True, timeout=600)
    assert res.returncode == 0, res.stderr[-3000:]
    df = pd.DataFrame([json.loads(l) for f in glob.glob(out + "/part-*") for l in open(f) if l.strip()])
    full = df[(df.segment_id == "S01") & (df.window_end_ms - df.window_start_ms == 30000)]
    # 1 distinct ping per second * 30 s = 30. Without de-duplication the full windows would show 60.
    assert (full.gps_pings <= 30).all(), full.gps_pings.tolist()
    assert (full.gps_pings == 30).sum() >= 5, full.gps_pings.tolist()
