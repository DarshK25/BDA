"""Smart-city data simulator: GPS trackers + IoT road sensors + weather station.

Run:  python -m cgls.simulator.producer --help

Three message streams are produced (JSON):
  gps_events      one message per vehicle every --ping-interval seconds
  sensor_events   one message per road segment every 5 s   (loop sensor: occupancy, count)
  weather_events  one message every 10 s                   (rain, visibility, temperature)

It also writes the hidden "true" congestion level of every segment to
<data>/ground_truth/gt.csv so that evaluate.py can measure how good the pipeline's
estimates and forecasts really are.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time

from cgls import config
from cgls.network import SEGMENTS, SEGMENT_BY_ID, neighbours, segments_at_node
from cgls import traffic_model as tm


# --------------------------------------------------------------------------- sinks
class KafkaSink:
    def __init__(self, bootstrap: str):
        from kafka import KafkaProducer  # imported here so tests do not need a broker

        last = None
        for attempt in range(30):  # broker may still be starting
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap,
                    linger_ms=20,
                    batch_size=64 * 1024,
                    acks=1,
                    key_serializer=lambda k: k.encode() if k else None,
                    value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode(),
                )
                return
            except Exception as exc:  # noqa: BLE001
                last = exc
                print(f"[simulator] waiting for Kafka at {bootstrap} ({exc.__class__.__name__}) ...", flush=True)
                time.sleep(2)
        raise RuntimeError(f"Kafka not reachable at {bootstrap}: {last}")

    def send(self, topic, key, value):
        self.producer.send(topic, key=key, value=value)

    def flush(self):
        self.producer.flush()


class FileSink:
    """Writes JSON-lines files into <dir>/<topic>/ - lets the Spark job be tested without Kafka."""

    def __init__(self, directory: str):
        self.dir = directory
        self.buf: dict[str, list[str]] = {}
        self.n = 0
        for t in config.INPUT_TOPICS:
            os.makedirs(os.path.join(directory, t), exist_ok=True)

    def send(self, topic, key, value):
        line = value.decode() if isinstance(value, bytes) else json.dumps(value)
        self.buf.setdefault(topic, []).append(line)

    def flush(self):
        for topic, lines in self.buf.items():
            if not lines:
                continue
            final = os.path.join(self.dir, topic, f"part-{self.n:08d}.json")
            tmp = os.path.join(self.dir, topic, f".tmp-{self.n:08d}")
            with open(tmp, "w") as f:
                f.write("\n".join(lines) + "\n")
            os.replace(tmp, final)  # atomic: Spark never sees half-written files
            self.buf[topic] = []
        self.n += 1


# --------------------------------------------------------------------------- world
RELAX = 0.04          # per second: congestion builds up / clears over about a minute, like a real queue
SPILLBACK = 0.45      # share of the accident road's excess congestion that spills onto connected roads


class World:
    """Holds the hidden true state and generates observations from it.

    Events are plain lists, so the real-time simulator uses one accident + one rain episode
    (from the command line) while the offline forecaster-training data uses many random ones:
        incidents: [(segment_id, start_s, duration_s)]     rains: [(start_s, duration_s, mm_per_hour)]
    """

    def __init__(self, args, rng: random.Random):
        self.args = args
        self.rng = rng
        self.c: dict[str, float] = {s["segment_id"]: 0.3 for s in SEGMENTS}
        self.rain = 0.0
        self.incident_active = False
        self.incidents = [(args.incident_segment, args.incident_after, args.incident_duration)] if args.incident_after >= 0 else []
        self.rains = [(args.rain_after, args.rain_duration, args.rain_mm)] if args.rain_after >= 0 else []
        self.vehicles = [self._new_vehicle(i) for i in range(args.vehicles)]

    # ---- scenario events -------------------------------------------------
    def _rain_at(self, t: float) -> float:
        best = 0.0
        for start, dur, mm in self.rains:
            if start <= t < start + dur:
                best = max(best, mm * min(1.0, (t - start) / 30.0))
        return best

    @staticmethod
    def _incident_intensity(start: float, dur: float, t: float) -> float:
        """0 -> 1 over 15 s after the crash, stays 1, fades 1 -> 0 over 30 s once it is cleared."""
        if t < start or t >= start + dur + 30:
            return 0.0
        if t < start + 15:
            return (t - start) / 15.0
        if t < start + dur:
            return 1.0
        return 1.0 - (t - start - dur) / 30.0

    def active_incidents(self, t: float) -> dict[str, float]:
        out: dict[str, float] = {}
        for seg, start, dur in self.incidents:
            k = self._incident_intensity(start, dur, t)
            if k > 0:
                out[seg] = max(out.get(seg, 0.0), k)
        return out

    def step(self, t: float) -> None:
        """Advance the hidden state by one second to time t (seconds since start)."""
        a = self.args
        self.rain = self._rain_at(t)
        active = self.active_incidents(t)
        self.incident_active = bool(active)
        demand = tm.city_demand(a.scenario, t)
        target = {s["segment_id"]: tm.congestion_index(demand, s["demand_bias"], self.rain,
                                                       0.5 * active.get(s["segment_id"], 0.0), self.rng)
                  for s in SEGMENTS}
        for seg, k in active.items():        # a jam spills back onto connected roads, with a delay
            excess = max(0.0, self.c[seg] - 0.35)
            for n in neighbours(seg):
                target[n] = min(1.0, target[n] + SPILLBACK * excess * k)
        for sid, tgt in target.items():
            self.c[sid] += RELAX * (tgt - self.c[sid])

    # ---- vehicles --------------------------------------------------------
    def _new_vehicle(self, i: int) -> dict:
        s = self.rng.choice(SEGMENTS)
        return {"id": f"V{i:05d}", "seg": s["segment_id"], "dir": self.rng.choice((0, 1)),
                "frac": self.rng.random(), "style": self.rng.uniform(0.9, 1.1)}

    def ping(self, v: dict, now_ms: int, interval_s: int) -> dict:
        s = SEGMENT_BY_ID[v["seg"]]
        speed = tm.sample_speed(s["free_flow_kmph"], self.c[v["seg"]], self.rng) * v["style"]
        # move the vehicle along its road
        v["frac"] += (speed * interval_s / 3600.0) / s["length_km"]
        while v["frac"] >= 1.0:
            end_node = s["node_b"] if v["dir"] == 0 else s["node_a"]
            options = [x for x in segments_at_node(end_node) if x != s["segment_id"]] or [s["segment_id"]]
            nxt = SEGMENT_BY_ID[self.rng.choice(options)]
            v["seg"], s = nxt["segment_id"], nxt
            v["dir"] = 0 if nxt["node_a"] == end_node else 1
            v["frac"] -= 1.0
        f = v["frac"] if v["dir"] == 0 else 1.0 - v["frac"]
        lat = s["lat1"] + (s["lat2"] - s["lat1"]) * f
        lon = s["lon1"] + (s["lon2"] - s["lon1"]) * f
        return {"vehicle_id": v["id"], "segment_id": s["segment_id"], "lat": round(lat, 6),
                "lon": round(lon, 6), "speed_kmph": round(speed, 1), "ts_ms": now_ms}

    def sensor(self, s: dict, ts_ms: int) -> dict:
        c = self.c[s["segment_id"]]
        return {"sensor_id": f"IOT-{s['segment_id']}", "segment_id": s["segment_id"],
                "occupancy_pct": round(tm.sample_occupancy(c, self.rng), 1),
                "vehicle_count": tm.sample_vehicle_count(s["lanes"], c, self.rng), "ts_ms": ts_ms}

    def weather(self, ts_ms: int) -> dict:
        return {"station_id": "WX-01", "rain_mm_hr": round(max(0.0, self.rain + self.rng.gauss(0, 0.2)), 2),
                "visibility_km": round(tm.sample_visibility(self.rain, self.rng), 2),
                "temp_c": tm.sample_temperature(self.rng),
                "humidity_pct": round(tm.clip(60 + 2.2 * self.rain + self.rng.gauss(0, 2), 30, 100), 1),
                "ts_ms": ts_ms}


# --------------------------------------------------------------------------- main
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Smart-city traffic data simulator")
    p.add_argument("--sink", choices=["kafka", "files"], default="kafka")
    p.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    p.add_argument("--out-dir", default="/tmp/cgls_events", help="for --sink files")
    p.add_argument("--vehicles", type=int, default=300, help="GPS-tracked vehicles")
    p.add_argument("--ping-interval", type=int, default=5, help="seconds between GPS pings of one vehicle")
    p.add_argument("--scenario", choices=list(tm.SCENARIO_DEMAND), default="normal")
    p.add_argument("--duration", type=int, default=0, help="seconds to run, 0 = forever")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dirty-rate", type=float, default=0.01, help="fraction of corrupt GPS messages")
    p.add_argument("--duplicate-rate", type=float, default=0.01, help="fraction of GPS messages sent twice")
    p.add_argument("--incident-after", type=int, default=90, help="seconds until accident, -1 = never")
    p.add_argument("--incident-duration", type=int, default=240)
    p.add_argument("--incident-segment", default="S05")
    p.add_argument("--rain-after", type=int, default=150, help="seconds until rain starts, -1 = never")
    p.add_argument("--rain-duration", type=int, default=240)
    p.add_argument("--rain-mm", type=float, default=12.0)
    p.add_argument("--no-events", action="store_true", help="disable accident and rain (steady load test)")
    p.add_argument("--no-ground-truth", action="store_true")
    a = p.parse_args(argv)
    if a.no_events:
        a.incident_after = a.rain_after = -1
    if a.incident_segment not in SEGMENT_BY_ID:
        p.error(f"unknown segment {a.incident_segment}")
    return a


def main(argv=None) -> int:
    a = parse_args(argv)
    rng = random.Random(a.seed)
    world = World(a, rng)
    sink = KafkaSink(a.bootstrap) if a.sink == "kafka" else FileSink(a.out_dir)

    gt_file = None
    gt_writer = None
    if not a.no_ground_truth:
        os.makedirs(config.GROUND_TRUTH_DIR, exist_ok=True)
        path = os.path.join(config.GROUND_TRUTH_DIR, "gt.csv")
        new = not os.path.exists(path)
        gt_file = open(path, "a", newline="")
        gt_writer = csv.writer(gt_file)
        if new:
            gt_writer.writerow(["ts_ms", "segment_id", "c", "level"])

    print(f"[simulator] sink={a.sink} vehicles={a.vehicles} scenario={a.scenario} "
          f"=> about {a.vehicles / a.ping_interval:.0f} GPS msgs/s + {len(SEGMENTS) / 5:.1f} sensor msgs/s", flush=True)
    if a.incident_after >= 0:
        print(f"[simulator] ACCIDENT planned on {a.incident_segment} at t+{a.incident_after}s "
              f"for {a.incident_duration}s", flush=True)
    if a.rain_after >= 0:
        print(f"[simulator] RAIN planned at t+{a.rain_after}s for {a.rain_duration}s ({a.rain_mm} mm/h)", flush=True)

    start = time.time()
    tick = 0
    sent = 0
    last_report = start
    last_boundary = -1
    try:
        while True:
            now = time.time()
            t = now - start
            if a.duration and t >= a.duration:
                break
            world.step(t)
            now_ms = int(now * 1000)

            # GPS: vehicles are split into `ping_interval` groups, one group pings per second
            for i in range(tick % a.ping_interval, len(world.vehicles), a.ping_interval):
                msg = world.ping(world.vehicles[i], now_ms - rng.randint(0, 900), a.ping_interval)
                if rng.random() < a.dirty_rate:
                    bad = rng.choice(("speed", "json", "nosegment"))
                    if bad == "speed":
                        msg["speed_kmph"] = rng.choice((-5.0, 999.0))
                    elif bad == "nosegment":
                        msg["segment_id"] = None
                    else:
                        sink.send(config.TOPIC_GPS, None, b'{"vehicle_id": "V0000", "speed_kmph": ')
                        continue
                sink.send(config.TOPIC_GPS, msg["segment_id"], msg)
                sent += 1
                if rng.random() < a.duplicate_rate:          # network retry: exact same message again
                    sink.send(config.TOPIC_GPS, msg["segment_id"], msg)
                    sent += 1

            # Sensors every 5 s, weather every 10 s, aligned to wall-clock boundaries
            sec = int(now)
            if sec // 5 != last_boundary:
                last_boundary = sec // 5
                boundary_ms = last_boundary * 5000
                for s in SEGMENTS:
                    m = world.sensor(s, boundary_ms + rng.randint(0, 900))
                    sink.send(config.TOPIC_SENSOR, s["segment_id"], m)
                    sent += 1
                    if gt_writer:
                        c = world.c[s["segment_id"]]
                        gt_writer.writerow([boundary_ms, s["segment_id"], round(c, 4), tm.level_from_score(c)])
                if last_boundary % 2 == 0:
                    sink.send(config.TOPIC_WEATHER, "WX-01", world.weather(boundary_ms + rng.randint(0, 900)))
                    sent += 1
                if gt_file:
                    gt_file.flush()

            sink.flush()
            tick += 1
            if now - last_report >= 10:
                flags = []
                if world.incident_active:
                    flags.append(f"ACCIDENT@{a.incident_segment}")
                if world.rain > 0:
                    flags.append(f"RAIN {world.rain:.1f}mm/h")
                print(f"[simulator] t={t:5.0f}s sent={sent:>8} ({sent / max(t, 1):.0f} msg/s) {' '.join(flags)}", flush=True)
                last_report = now
            time.sleep(max(0.0, (start + tick) - time.time()))
    except KeyboardInterrupt:
        pass
    finally:
        sink.flush()
        if gt_file:
            gt_file.close()
    print(f"[simulator] finished, {sent} messages", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
