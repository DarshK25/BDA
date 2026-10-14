"""Rendering contracts for the Streamlit dashboard.

The tests exercise figure data and the Streamlit test harness rather than
asserting on source text.  They deliberately use a cached-feed replacement so
rendering never needs Kafka or the traffic tile service.
"""

from __future__ import annotations

from mapyourway import config
from mapyourway.dashboard import app


NOW_MS = 1_800_000_000_000


def _message(**overrides):
    message = {
        "segment_id": "S01",
        "source_observed_at_ms": NOW_MS,
        "current_speed_kmph": 30.0,
        "free_flow_kmph": 48.0,
        "source_confidence": 0.95,
        "congestion_level": 2,
        "prediction_source": "spark_ml",
    }
    message.update(overrides)
    return message


def test_speed_values_use_valid_free_flow_from_message(sample_segment):
    assert app._speed_values(sample_segment, _message(), NOW_MS) == (30.0, 48.0)


def test_speed_values_keep_unknown_stale_and_closed_states_distinct(sample_segment):
    unknown_speed, unknown_free = app._speed_values(sample_segment, {}, NOW_MS)
    stale_speed, stale_free = app._speed_values(
        sample_segment,
        _message(source_observed_at_ms=NOW_MS - (config.TRAFFIC_SOURCE_TTL_SECONDS + 1) * 1000),
        NOW_MS,
    )
    closed_speed, closed_free = app._speed_values(
        sample_segment,
        _message(road_closure=True, current_speed_kmph=0.0, free_flow_kmph=52.0),
        NOW_MS,
    )

    assert (unknown_speed, unknown_free) == (None, sample_segment["free_flow_kmph"])
    assert (stale_speed, stale_free) == (None, 48.0)
    assert (closed_speed, closed_free) == (0.0, 52.0)


def test_corridor_map_uses_geojson_geometry_and_labels_estimates(sample_segment):
    segment = dict(sample_segment)
    segment["geometry"] = [[72.8567, 19.2307], [72.854, 19.21], [72.8484, 19.1864]]
    segment["geometry_source"] = "osm"

    original_segments = app.SEGMENTS
    original_by_id = app.SEGMENT_BY_ID
    try:
        app.SEGMENTS = [segment]
        app.SEGMENT_BY_ID = {segment["segment_id"]: segment}
        figure = app.create_traffic_map(
            {"S01": _message()},
            traffic_layer="Corridor estimates",
            show_nodes=False,
            now_ms=NOW_MS,
        )
    finally:
        app.SEGMENTS = original_segments
        app.SEGMENT_BY_ID = original_by_id

    assert figure.data[0].uid == "corridor-S01"
    assert list(figure.data[0].lon) == [72.8567, 72.854, 72.8484]
    assert list(figure.data[0].lat) == [19.2307, 19.21, 19.1864]
    assert "Spark ML estimates" in figure.layout.title.text


def test_live_map_paints_live_corridors():
    figure = app.create_traffic_map(
        {},
        traffic_layer="Live road traffic",
        provider_status={"configured": True, "available": True, "error": None},
        show_nodes=False,
        now_ms=NOW_MS,
    )

    # The map draws every corridor coloured by live congestion along the sourced
    # road geometry on an OpenStreetMap base map (MapLibre, no token).
    corridor_uids = [trace.uid for trace in figure.data if str(trace.uid).startswith("corridor-")]
    assert len(corridor_uids) == len(app.SEGMENTS)
    assert "Live road traffic" in figure.layout.title.text
    assert figure.layout.uirevision == "traffic-map"
    assert figure.layout.map.style == "open-street-map"


def test_route_overlays_have_stable_uids():
    figure = app.create_traffic_map(
        {},
        best_route=["S01"],
        usual_route=["S01"],
        best_route_nodes=["Borivali", "Malad"],
        usual_route_nodes=["Borivali", "Malad"],
        now_ms=NOW_MS,
    )

    # Corridors are drawn first; the two route overlays keep stable, ordered uids.
    route_uids = [trace.uid for trace in figure.data if str(trace.uid).startswith("route-")]
    assert route_uids == ["route-usual", "route-best"]


def test_speed_chart_uses_same_free_flow_as_map(sample_segment):
    segment = dict(sample_segment)
    segment["free_flow_kmph"] = 60.0
    original_segments = app.SEGMENTS
    try:
        app.SEGMENTS = [segment]
        figure = app.create_speed_chart({"S01": _message(free_flow_kmph=44.0)}, NOW_MS)
    finally:
        app.SEGMENTS = original_segments

    assert list(figure.data[0].y) == [44.0]
    assert list(figure.data[1].y) == [30.0]


def test_old_delivery_is_reported_as_stalled_without_changing_source_age():
    assert app._pipeline_status(NOW_MS / 1000 - 31, NOW_MS, 19, None) == "Stalled"
    assert app._pipeline_status(None, NOW_MS, 0, None) == "Waiting"


def test_provider_status_is_cached_and_errors_are_sanitized(monkeypatch):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return b'{"configured": true, "available": false, "last_success_ms": 7, "error": "secret URL and token"}'

    calls = []

    def fake_urlopen(request, timeout):
        calls.append((request.full_url, timeout))
        return Response()

    monkeypatch.setenv("TRAFFIC_TILE_INTERNAL_URL", "http://tile-service")
    monkeypatch.setattr(app.urllib.request, "urlopen", fake_urlopen)
    app._PROVIDER_STATUS_CACHE.clear()

    first = app._traffic_provider_status()
    second = app._traffic_provider_status()

    assert first == second == {
        "configured": True,
        "available": False,
        "last_success_ms": 7,
        "error": "traffic tile provider unavailable",
    }
    assert calls == [("http://tile-service/status", 1.0)]
    assert "secret" not in str(first)


def test_app_test_renders_waiting_state_and_preserves_controls():
    from streamlit.testing.v1 import AppTest

    script = """
from mapyourway.dashboard import app

class FakeState:
    def get_snapshot(self):
        return {}, [], 0, None, None

app.get_live_state = lambda: (FakeState(), object())
app._traffic_provider_status = lambda: {
    "configured": True, "available": True, "last_success_ms": None, "error": None,
}
app.main()
"""

    at = AppTest.from_string(script).run(timeout=10)
    assert not at.exception
    assert at.radio[0].value == "Live road traffic"
    assert any(item.label == "Monitored corridors" and item.value == "19" for item in at.metric)
    assert any("Waiting" in item.value for item in at.info)

    at.sidebar.checkbox[1].set_value(True)
    at.run(timeout=10)
    assert at.sidebar.checkbox[1].value is True
