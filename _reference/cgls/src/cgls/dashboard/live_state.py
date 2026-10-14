"""Thread-safe store of the latest congestion update per segment.

Feeds: a Kafka consumer on topic congestion_updates (normal mode), or a built-in fake feed
(DEMO mode: `CGLS_DEMO=1`) so the dashboard can be developed or shown without Kafka/Spark.
"""
from __future__ import annotations

import json
import random
import threading
import time
from collections import deque

from cgls import config
from cgls import traffic_model as tm
from cgls.network import SEGMENTS


class LiveState:
    def __init__(self):
        self.lock = threading.Lock()
        self.latest: dict[str, dict] = {}
        self.city_history: deque = deque(maxlen=360)     # (window_end_ms, mean score, mean speed ratio)
        self.messages = 0
        self.error: str | None = None
        self.mode = "kafka"
        self._by_window: dict[int, list[dict]] = {}

    def update(self, msg: dict) -> None:
        with self.lock:
            cur = self.latest.get(msg["segment_id"])
            if cur is None or msg["window_end_ms"] >= cur["window_end_ms"]:
                self.latest[msg["segment_id"]] = msg
            self.messages += 1
            w = self._by_window.setdefault(msg["window_end_ms"], [])
            w.append(msg)
            if len(w) == len(SEGMENTS):                  # a full city snapshot is complete
                self.city_history.append((msg["window_end_ms"],
                                          sum(m["score"] for m in w) / len(w),
                                          sum(m["speed_ratio"] for m in w) / len(w)))
            for k in sorted(self._by_window)[:-12]:      # keep memory bounded
                del self._by_window[k]

    def snapshot(self):
        with self.lock:
            return dict(self.latest), list(self.city_history), self.messages, self.error


def start_kafka_consumer(state: LiveState, bootstrap: str) -> threading.Thread:
    def run():
        from kafka import KafkaConsumer
        while True:
            try:
                consumer = KafkaConsumer(config.TOPIC_OUT, bootstrap_servers=bootstrap,
                                         auto_offset_reset="latest", enable_auto_commit=False,
                                         group_id=None, value_deserializer=lambda b: json.loads(b.decode()))
                state.error = None
                for rec in consumer:
                    state.update(rec.value)
            except Exception as exc:  # noqa: BLE001 - keep retrying, show the reason in the UI
                state.error = f"{exc.__class__.__name__}: {exc}"
                time.sleep(3)

    th = threading.Thread(target=run, daemon=True, name="kafka-consumer")
    th.start()
    return th


def fake_message(seg: dict, t: float, rng: random.Random, incident_seg: str = "S05") -> dict:
    """What the Spark job would publish, generated directly from the traffic model (DEMO mode)."""
    incident = 0.5 if (seg["segment_id"] == incident_seg and (t % 240) > 60) else 0.0
    c = tm.congestion_index(tm.city_demand("normal", t), seg["demand_bias"], 0.0, incident, rng)
    speed = tm.speed_from_c(seg["free_flow_kmph"], c)
    lvl = tm.level_from_score(c)
    end_ms = int(t * 1000)
    return {
        "segment_id": seg["segment_id"], "name": seg["name"], "window_start_ms": end_ms - 30000,
        "window_end_ms": end_ms, "avg_speed_kmph": round(speed, 1), "free_flow_kmph": seg["free_flow_kmph"],
        "speed_ratio": round(speed / seg["free_flow_kmph"], 3), "occupancy_pct": round(100 * c, 1),
        "veh_per_lane": round(3 + 9 * c, 2), "rain_mm_hr": 0.0, "visibility_km": 10.0, "gps_pings": 60,
        "vehicles": 30, "level": lvl, "confidence": 0.95, "score": round(c, 3), "forecast_score": round(c, 3),
        "forecast_level": lvl, "level_name": tm.LEVEL_NAMES[lvl], "forecast_level_name": tm.LEVEL_NAMES[lvl],
        "est_speed_kmph": round(speed, 1), "emitted_ms": end_ms,
    }


def start_demo_feed(state: LiveState) -> threading.Thread:
    state.mode = "demo"

    def run():
        rng = random.Random(1)
        t0 = time.time()
        while True:
            now = time.time()
            base_ms = int(now // 5 * 5 * 1000)
            for s in SEGMENTS:
                m = fake_message(s, now - t0, rng)
                m["window_end_ms"] = base_ms
                m["window_start_ms"] = base_ms - 30000
                m["emitted_ms"] = int(now * 1000)
                state.update(m)
            time.sleep(5)

    th = threading.Thread(target=run, daemon=True, name="demo-feed")
    th.start()
    return th
