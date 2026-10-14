"""Feature engineering in plain Python (used by training).

The Spark job implements the *same* formulas with Spark SQL expressions
(streaming/job.py::add_features). tests/test_features.py checks the two agree, because a
mismatch between training and serving is one of the most common real-world ML bugs.
"""
from __future__ import annotations


def compute_features(avg_speed, free_flow, avg_occ, avg_vehicle_count, lanes, avg_rain, avg_vis) -> dict:
    """Any avg_* argument may be None (no event of that type arrived in the window)."""
    if avg_speed is not None:
        speed_ratio = avg_speed / free_flow
    elif avg_occ is not None:
        speed_ratio = 1.0 - 0.8 * avg_occ / 100.0
    else:
        speed_ratio = 1.0
    speed_ratio = min(max(speed_ratio, 0.05), 1.3)

    occupancy = avg_occ if avg_occ is not None else max(0.0, (1.0 - speed_ratio) / 0.8 * 100.0)
    veh_per_lane = (avg_vehicle_count / lanes) if avg_vehicle_count is not None else 3.0 + 9.0 * occupancy / 100.0
    return {
        "speed_ratio": speed_ratio,
        "occupancy_pct": occupancy,
        "veh_per_lane": veh_per_lane,
        "rain_mm_hr": avg_rain if avg_rain is not None else 0.0,
        "visibility_km": avg_vis if avg_vis is not None else 10.0,
    }
