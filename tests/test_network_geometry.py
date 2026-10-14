"""Regression tests for the sourced Mumbai network geometry."""

import json
import math
from pathlib import Path

from mapyourway.network import (
    GEOMETRY_FALLBACK_USED,
    NODES,
    SEGMENTS,
    SEGMENT_BY_ID,
    haversine_km,
    load_network_geometry,
    point_along_segment,
)
from mapyourway.simulator.real_producer import (
    _segment_road_sample,
    fetch_and_publish_traffic,
)
from mapyourway.simulator.real_producer import ProbeFleet


def _path_length_km(geometry):
    return sum(
        haversine_km((a[1], a[0]), (b[1], b[0]))
        for a, b in zip(geometry, geometry[1:])
    )


def test_all_segments_use_valid_sourced_geometry_and_real_path_lengths():
    assert len(SEGMENTS) == 19
    assert GEOMETRY_FALLBACK_USED is False
    for segment in SEGMENTS:
        geometry = segment["geometry"]
        assert segment["geometry_source"] == "osrm"
        assert len(geometry) >= 2
        assert all(
            isinstance(point, list)
            and len(point) == 2
            and math.isfinite(point[0])
            and math.isfinite(point[1])
            and -180 <= point[0] <= 180
            and -90 <= point[1] <= 90
            for point in geometry
        )
        assert segment["length_km"] == segment["route_distance_m"] / 1000
        assert segment["length_km"] >= haversine_km(
            NODES[segment["node_a"]], NODES[segment["node_b"]]
        )
        assert segment["source_url"].startswith(
            "https://router.project-osrm.org/route/v1/driving/"
        )


def test_geometry_endpoints_are_near_named_node_anchors():
    for segment in SEGMENTS:
        geometry = segment["geometry"]
        assert haversine_km(
            (geometry[0][1], geometry[0][0]), NODES[segment["node_a"]]
        ) < 0.75
        assert haversine_km(
            (geometry[-1][1], geometry[-1][0]), NODES[segment["node_b"]]
        ) < 0.75


def test_path_midpoint_follows_curved_geometry_not_endpoint_chord():
    segment = SEGMENT_BY_ID["S19"]
    lat, lon = point_along_segment(segment, 0.5)
    chord_lat = (segment["lat1"] + segment["lat2"]) / 2
    chord_lon = (segment["lon1"] + segment["lon2"]) / 2
    assert haversine_km((lat, lon), (chord_lat, chord_lon)) > 0.1
    assert any(
        abs(lon - point[0]) < 1e-6 and abs(lat - point[1]) < 1e-6
        for point in segment["geometry"]
    ) or 72.80 < lon < 72.83


def test_s19_geometry_uses_the_sea_link_bridge_route():
    segment = SEGMENT_BY_ID["S19"]
    assert segment["source_url"].count(";") == 2
    assert min(point[0] for point in segment["geometry"]) < 72.814
    assert max(point[0] for point in segment["geometry"]) > 72.816
    assert any(19.015 < point[1] < 19.032 for point in segment["geometry"])


def test_s18_geometry_is_constrained_to_the_jvlr_corridor():
    segment = SEGMENT_BY_ID["S18"]
    assert "72.874000,19.145000" in segment["source_url"]
    assert any(
        abs(point[0] - 72.874) < 0.002 and 19.14 < point[1] < 19.17
        for point in segment["geometry"]
    )
    assert max(point[0] for point in segment["geometry"]) > 72.9


def test_missing_or_malformed_geometry_asset_selects_explicit_fallback(tmp_path):
    missing_segments, missing_fallback = load_network_geometry(
        tmp_path / "missing.json"
    )
    assert missing_fallback is True
    assert missing_segments == {}

    malformed = tmp_path / "malformed.json"
    malformed.write_text(json.dumps({"segments": []}), encoding="utf-8")
    malformed_segments, malformed_fallback = load_network_geometry(malformed)
    assert malformed_fallback is True
    assert malformed_segments == {}

    asset = json.loads(
        Path(__file__).parents[1]
        .joinpath("src", "mapyourway", "network_geometry.json")
        .read_text(encoding="utf-8")
    )
    asset["segments"][0]["source_url"] = "https://example.invalid/not-osrm"
    bad_source = tmp_path / "bad-source.json"
    bad_source.write_text(json.dumps(asset), encoding="utf-8")
    bad_source_segments, bad_source_fallback = load_network_geometry(bad_source)
    assert bad_source_fallback is True
    assert bad_source_segments == {}


def test_fallback_point_interpolation_uses_the_endpoint_chord():
    fallback_segment = {
        "lat1": 19.0,
        "lon1": 72.8,
        "lat2": 19.2,
        "lon2": 73.0,
    }
    assert point_along_segment(fallback_segment, 0.5) == (19.1, 72.9)


class _FlowResponse:
    status_code = 200

    def json(self):
        return {
            "flowSegmentData": {
                "currentSpeed": 30,
                "freeFlowSpeed": 60,
                "currentTravelTime": 120,
                "freeFlowTravelTime": 60,
                "confidence": 0.9,
            }
        }


class _RecordingSession:
    def __init__(self):
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _FlowResponse()


class _RecordingProducer:
    def __init__(self):
        self.messages = []

    def send(self, topic, key, value):
        self.messages.append((topic, key, value))

    def flush(self):
        pass


def test_tomtom_and_aggregate_gps_use_the_path_midpoint():
    segment = SEGMENT_BY_ID["S19"]
    expected_lat, expected_lon = point_along_segment(segment, 0.5)
    session = _RecordingSession()
    producer = _RecordingProducer()

    assert fetch_and_publish_traffic(
        producer,
        session=session,
        api_key="test-key",
        segments=[segment],
        now_ms=123,
    ) == (1, 0)

    assert session.calls[0][1]["params"]["point"] == (
        f"{expected_lat:.6f},{expected_lon:.6f}"
    )
    gps = producer.messages[1][2]
    assert gps["lat"] == round(expected_lat, 6)
    assert gps["lon"] == round(expected_lon, 6)


def test_producer_sample_uses_path_helper_not_stored_endpoint_or_sample():
    segment = dict(SEGMENT_BY_ID["S19"])
    segment["road_sample"] = [segment["lat1"], segment["lon1"]]
    expected = point_along_segment(segment, 0.5)

    assert _segment_road_sample(segment) == expected


def test_derived_probe_positions_use_path_distance_interpolation():
    segment = SEGMENT_BY_ID["S19"]
    expected_lat, expected_lon = point_along_segment(segment, 0.25)
    event = ProbeFleet(vehicles_per_segment=3).events(
        segment,
        speed_kmph=30.0,
        free_speed_kmph=60.0,
        now_ms=123,
        elapsed_seconds=0.0,
        confidence=0.9,
    )[0]
    assert event["lat"] == round(expected_lat, 6)
    assert event["lon"] == round(expected_lon, 6)
