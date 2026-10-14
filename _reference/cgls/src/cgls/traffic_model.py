"""The simulated 'real world'.

One hidden number per road segment, the congestion index c in [0, 1], drives everything
that sensors can observe (speed, occupancy, vehicle count). The simulator and the
model-training script both import this file, so the training data and the live data come
from the same physical story. The Spark job never sees c - it must *infer* it from noisy
GPS, sensor and weather readings. That inference is the "prediction" part of the project.
"""
from __future__ import annotations

import math
import random

LEVEL_NAMES = ["FREE", "MODERATE", "HEAVY", "SEVERE"]
THRESHOLDS = (0.25, 0.50, 0.75)
LEVEL_MIDPOINTS = (0.125, 0.375, 0.625, 0.875)

SCENARIO_DEMAND = {"quiet": 0.15, "normal": 0.30, "rush": 0.55}


def clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def level_from_score(c: float) -> int:
    """0=FREE, 1=MODERATE, 2=HEAVY, 3=SEVERE"""
    if c < THRESHOLDS[0]:
        return 0
    if c < THRESHOLDS[1]:
        return 1
    if c < THRESHOLDS[2]:
        return 2
    return 3


def city_demand(scenario: str, t_seconds: float) -> float:
    """Base demand with a slow wave (period 5 min) so the map keeps changing."""
    base = SCENARIO_DEMAND[scenario]
    return base * (1.0 + 0.15 * math.sin(2 * math.pi * t_seconds / 300.0))


def congestion_index(demand: float, bias: float, rain_mm_hr: float,
                     incident_boost: float, rng: random.Random) -> float:
    c = demand * bias + 0.15 * min(rain_mm_hr, 20.0) / 20.0 + incident_boost + rng.gauss(0, 0.03)
    return clip(c, 0.0, 1.0)


def speed_from_c(free_flow: float, c: float) -> float:
    """Noise-free speed implied by congestion index c."""
    return free_flow * (1.0 - 0.8 * c)


def sample_speed(free_flow: float, c: float, rng: random.Random) -> float:
    return max(2.0, speed_from_c(free_flow, c) * (1.0 + rng.gauss(0, 0.08)))


def sample_occupancy(c: float, rng: random.Random) -> float:
    return clip(100.0 * c ** 0.9 + rng.gauss(0, 5.0), 0.0, 100.0)


def sample_vehicle_count(lanes: int, c: float, rng: random.Random) -> int:
    """Vehicles counted by a loop sensor in one 5 s interval."""
    return max(0, round(lanes * (3.0 + 9.0 * c) + rng.gauss(0, 1.5)))


def sample_visibility(rain_mm_hr: float, rng: random.Random) -> float:
    return clip(10.0 - 0.35 * rain_mm_hr + rng.gauss(0, 0.4), 0.5, 10.0)


def sample_temperature(rng: random.Random) -> float:
    return round(29.0 + rng.gauss(0, 0.3), 1)
