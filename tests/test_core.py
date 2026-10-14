"""Pure-Python regression tests for the active MapYourWay contracts."""

from datetime import datetime, timezone

import pandas as pd

from mapyourway import config
from mapyourway.features import create_model_features
from mapyourway.ml.bootstrap import build_calibration_rows
from mapyourway.network import SEGMENTS
from mapyourway.routing import suggest_live_route
from mapyourway.streaming.job import _merge_export_frame


def _live_messages(now_ms: int) -> dict[str, dict]:
    messages = {}
    for segment in SEGMENTS:
        messages[segment["segment_id"]] = {
            "segment_id": segment["segment_id"],
            "timestamp": datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat(),
            "current_speed_kmph": segment["free_flow_kmph"],
            "free_flow_kmph": segment["free_flow_kmph"],
            "source_observed_at_ms": now_ms,
            "source_confidence": 0.95,
            "congestion_level": 0,
        }
    return messages


def test_config_uses_one_live_feature_contract_and_model_paths():
    assert config.FEATURE_COLUMNS == [
        "speed_ratio",
        "occupancy_pct",
        "veh_per_lane",
        "rain_mm_hr",
        "visibility_km",
    ]
    assert config.MODEL_PATH.endswith("congestion_model")
    assert config.GBT_MODEL_PATH.endswith("congestion_gbt_model")


def test_pandas_feature_engineering_preserves_non_default_index_and_units():
    frame = pd.DataFrame(
        {
            "timestamp": ["2026-10-10T12:00:00Z"],
            "segment_id": ["S01"],
            "current_speed_kmph": [30.0],
            "free_flow_speed_kmph": [60.0],
            "occupancy_pct": [60.0],
            "flow_per_lane": [7.5],
            "visibility": [6500.0],
        },
        index=[9],
    )
    result = create_model_features(frame)
    assert list(result.index) == [9]
    assert result.loc[9, "speed_ratio"] == 0.5
    assert result.loc[9, "occupancy_pct"] == 60.0
    assert result.loc[9, "veh_per_lane"] == 7.5
    assert result.loc[9, "visibility_km"] == 6.5


def test_live_router_excludes_closed_edges_and_uses_forecast_contract():
    now_ms = 1_791_638_400_000
    messages = _live_messages(now_ms)
    messages["S19"]["road_closure"] = True
    route = suggest_live_route("Bandra", "Worli", messages, now_ms=now_ms)
    assert route["mode"] == "current"
    assert "S19" in route["closed_segments"]
    assert "S19" not in route["best"].segment_ids

    for message in messages.values():
        message.update(
            {
                "forecast_speed_kmph": message["current_speed_kmph"],
                "forecast_horizon_minutes": 15,
                "forecast_for_ms": now_ms + 600_000,
                "forecast_source": "persistence_baseline",
                "forecast_model_id": "persistence-v1",
            }
        )
    forecast_route = suggest_live_route(
        "Andheri", "Dadar", messages, now_ms=now_ms, use_forecast=True
    )
    assert forecast_route["mode"] == "forecast"
    assert forecast_route["forecast_available"] is True


def test_bootstrap_scenarios_cover_all_four_classes():
    rows = build_calibration_rows(n_per_class=3, seed=7)
    assert len(rows) == 12
    assert {int(row["congestion_level"]) for row in rows} == {0, 1, 2, 3}
    assert all(row["label_source"] == "bootstrap_calibration" for row in rows)


def test_export_frame_normalizes_csv_and_spark_timestamps_before_sorting():
    existing = pd.DataFrame(
        {"segment_id": ["S01"], "timestamp": ["2026-10-11 12:00:00"]}
    )
    current = pd.DataFrame(
        {"segment_id": ["S02"], "timestamp": [pd.Timestamp("2026-10-11 12:10:00")]}
    )

    result = _merge_export_frame(existing, current)

    assert list(result["segment_id"]) == ["S01", "S02"]
    assert str(result.loc[1, "timestamp"]) == "2026-10-11 12:10:00+00:00"
