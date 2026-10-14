"""Streamlit live traffic, routing, and corridor analytics dashboard."""

from __future__ import annotations

import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Debug: Print what we loaded (remove after testing)
print(f"🔍 DEBUG - BASEMAPS_API_KEY loaded: {os.getenv('BASEMAPS_API_KEY', 'NOT SET')[:20]}...")
print(f"🔍 DEBUG - OSM_TILE_URL loaded: {os.getenv('OSM_TILE_URL', 'NOT SET')[:60]}...")

import networkx as nx
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

src_path = Path(__file__).parent.parent.parent
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from mapyourway.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_CONGESTION
from mapyourway.contracts import LEVEL_COLORS, LEVEL_NAMES, observation_status, timestamp_ms
from mapyourway.dashboard.live_state import start_live_feed
from mapyourway.network import NODES, SEGMENTS, SEGMENT_BY_ID
from mapyourway.local_streets import ALL_ROUTING_NODES, LOCAL_EDGES, LOCAL_LINKS, LOCAL_STREETS
from mapyourway.routing import forecast_available, suggest_live_route


MONITORED_CORRIDOR_COUNT = len(SEGMENTS)
TRAFFIC_LAYER_LIVE = "Live road traffic"
TRAFFIC_LAYER_ESTIMATES = "Corridor estimates"
TRAFFIC_TILE_PUBLIC_DEFAULT = "http://localhost:8502"
TRAFFIC_TILE_INTERNAL_DEFAULT = "http://localhost:8502"
TRAFFIC_TILE_BUCKET_MS = 120_000

# Get basemap tile URL - prioritize OSM_TILE_URL from .env
BASEMAPS_KEY = os.getenv('BASEMAPS_API_KEY', '')
OSM_TILE_FROM_ENV = os.getenv('OSM_TILE_URL', '')

if OSM_TILE_FROM_ENV:
    # Use the explicit URL from .env (already has API key baked in)
    CARTO_TILE_URL = OSM_TILE_FROM_ENV
    print(f"🗺️  Using basemap from .env: {CARTO_TILE_URL[:60]}...")
elif BASEMAPS_KEY:
    # Construct MapTiler URL with the API key
    CARTO_TILE_URL = f"https://api.maptiler.com/maps/streets/{{z}}/{{x}}/{{y}}.png?key={BASEMAPS_KEY}"
    print(f"🗺️  Using MapTiler with BASEMAPS_API_KEY")
else:
    # Fallback to OpenStreetMap (free, no key)
    CARTO_TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
    print("🗺️  Using OpenStreetMap fallback (no API key found)")

CARTO_ATTRIBUTION = "© MapTiler, © OpenStreetMap contributors" if (OSM_TILE_FROM_ENV or BASEMAPS_KEY) else "© OpenStreetMap contributors"
ROUTABLE_BY_ID = {
    **SEGMENT_BY_ID,
    **{edge["segment_id"]: edge for edge in LOCAL_EDGES},
    **{link["segment_id"]: link for link in LOCAL_LINKS},
}
TRAFFIC_PROVIDER_STATUS_TTL_SECONDS = 5.0
TRAFFIC_PROVIDER_TIMEOUT_SECONDS = 1.0
DELIVERY_STALLED_SECONDS = 30.0

_PROVIDER_STATUS_CACHE: dict[str, tuple[float, dict]] = {}
_SAFE_PROVIDER_ERRORS = {
    "API key not configured",
    "upstream authorization failed",
    "upstream rate limited",
    "upstream timeout",
    "upstream unavailable",
    "last successful tile expired",
    "traffic tile service busy",
}

st.set_page_config(page_title="MapYourWay - Mumbai Traffic", page_icon="🚗", layout="wide")


@st.cache_resource
def get_live_state():
    """Return the process-wide Kafka feed; the timed fragment only reads it."""
    return start_live_feed(os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS), TOPIC_CONGESTION)


def _valid_positive_number(value, maximum: float = 150.0) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and 0 < number <= maximum else None


def _speed_values(segment: dict, message: dict, now_ms: int | None = None) -> tuple[float | None, float]:
    """Return an observed speed and the free-flow value from the same source.

    A missing, stale, low-confidence, or malformed observation is never turned
    into a free-flow observation.  Spark output can carry a sourced
    ``free_flow_kmph`` that differs from the static network fallback, so the
    message value is used whenever it is valid.
    """
    free = _valid_positive_number(message.get("free_flow_kmph"), maximum=150.0)
    if free is None:
        free = float(segment["free_flow_kmph"])
    if observation_status(message, now_ms) not in {"fresh", "closed"}:
        return None, free
    try:
        speed = float(message["current_speed_kmph"])
        return (speed if math.isfinite(speed) and 0 <= speed <= 150 else None), free
    except (KeyError, ValueError, TypeError, OverflowError):
        return None, free


def _segment_geometry(segment: dict) -> tuple[list[list[float]], str]:
    """Get sourced GeoJSON coordinates, falling back to node endpoints.

    The network worker may add ``geometry`` and ``geometry_source`` while this
    dashboard is running against an older network module.  Keeping the
    fallback here makes that interface forwards- and backwards-compatible.
    """
    geometry = segment.get("geometry")
    if isinstance(geometry, dict):
        geometry = geometry.get("coordinates")
    if isinstance(geometry, (list, tuple)) and len(geometry) >= 2:
        points: list[list[float]] = []
        valid = True
        for point in geometry:
            if not isinstance(point, (list, tuple)) or len(point) < 2:
                valid = False
                break
            try:
                lon, lat = float(point[0]), float(point[1])
            except (TypeError, ValueError, OverflowError):
                valid = False
                break
            if not math.isfinite(lon) or not math.isfinite(lat):
                valid = False
                break
            points.append([lon, lat])
        if valid and len(points) >= 2:
            return points, str(segment.get("geometry_source") or "network geometry")

    return [
        [float(segment["lon1"]), float(segment["lat1"])],
        [float(segment["lon2"]), float(segment["lat2"])],
    ], "node endpoints (fallback)"


def _oriented_geometry(segment: dict, from_node: str | None = None, to_node: str | None = None) -> list[list[float]]:
    points, _ = _segment_geometry(segment)
    if from_node == segment.get("node_b") and to_node == segment.get("node_a"):
        return list(reversed(points))
    if from_node == segment.get("node_a") and to_node == segment.get("node_b"):
        return points
    if from_node is None or not points:
        return points

    node = ALL_ROUTING_NODES.get(from_node)
    if node is None:
        return points
    node_lon_lat = (float(node[1]), float(node[0]))
    start_distance = (points[0][0] - node_lon_lat[0]) ** 2 + (points[0][1] - node_lon_lat[1]) ** 2
    end_distance = (points[-1][0] - node_lon_lat[0]) ** 2 + (points[-1][1] - node_lon_lat[1]) ** 2
    return list(reversed(points)) if end_distance < start_distance else points


def _route_geometry(segment_ids: list[str], route_nodes: list[str] | None = None) -> list[list[float]]:
    """Join route segment polylines without replacing them with straight chords."""
    joined: list[list[float]] = []
    for index, segment_id in enumerate(segment_ids):
        segment = ROUTABLE_BY_ID.get(segment_id)
        if segment is None:
            continue
        from_node = route_nodes[index] if route_nodes and index < len(route_nodes) else None
        to_node = route_nodes[index + 1] if route_nodes and index + 1 < len(route_nodes) else None
        points = _oriented_geometry(segment, from_node, to_node)
        if not points:
            continue
        if joined and joined[-1] == points[0]:
            joined.extend(points[1:])
        else:
            joined.extend(points)
    return joined


def _source_age(message: dict, now_ms: int) -> str:
    observed = timestamp_ms(message.get("source_observed_at_ms") or message.get("observed_at_ms"))
    return f"{max(0, (now_ms - observed) // 1000)}s" if observed else "unknown"


def _latest_source_age(data: dict, now_ms: int) -> str:
    ages = []
    for message in data.values():
        observed = timestamp_ms(message.get("source_observed_at_ms") or message.get("observed_at_ms"))
        if observed:
            ages.append(max(0, (now_ms - observed) // 1000))
    return f"{min(ages)}s ago" if ages else "N/A"


def _delivery_age(last_update: float | None, now_ms: int) -> str:
    return f"{max(0, int(now_ms / 1000 - last_update))}s ago" if last_update else "Waiting"


def _pipeline_status(
    last_update: float | None,
    now_ms: int,
    fresh_count: int,
    error: str | None,
) -> str:
    """Describe delivery health separately from source observation freshness."""
    if last_update is None:
        return "Error" if error else "Waiting"
    if now_ms / 1000 - last_update > DELIVERY_STALLED_SECONDS:
        return "Stalled"
    if error:
        return "Error"
    return "Running" if fresh_count == len(SEGMENTS) else "Partial"


def _prediction_label(message: dict) -> str:
    source = message.get("prediction_source")
    if not source:
        return "Unavailable"
    normalized = str(source).lower()
    return "Spark ML estimate" if "spark" in normalized or "ml" in normalized else f"Derived: {source}"


def _segment_color(segment: dict, message: dict, now_ms: int | None = None) -> tuple[str, str]:
    status = observation_status(message, now_ms)
    if status in {"unknown", "stale", "low_confidence"}:
        return "#95a5a6", status
    if status == "closed":
        return "#2c3e50", status
    speed, free = _speed_values(segment, message, now_ms)
    if speed is None:
        return "#95a5a6", "unknown"
    try:
        level = int(message["congestion_level"])
        if level not in range(4) or level != float(message["congestion_level"]):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        ratio = speed / free if free > 0 else 0
        level = 0 if ratio >= .8 else 1 if ratio >= .5 else 2 if ratio >= .3 else 3
    return LEVEL_COLORS[level], LEVEL_NAMES[level]


def _safe_provider_error(value) -> str | None:
    """Keep provider errors useful without echoing URLs, headers, or exceptions."""
    if value is None:
        return None
    text = str(value).strip()
    if text in _SAFE_PROVIDER_ERRORS:
        return text
    normalized = text.lower()
    if "configur" in normalized or "api key" in normalized:
        return "API key not configured"
    if "authoriz" in normalized or "forbidden" in normalized or "401" in normalized or "403" in normalized:
        return "upstream authorization failed"
    if "rate" in normalized or "429" in normalized:
        return "upstream rate limited"
    if "timeout" in normalized or "timed out" in normalized:
        return "upstream timeout"
    if "expired" in normalized:
        return "last successful tile expired"
    if "busy" in normalized:
        return "traffic tile service busy"
    return "traffic tile provider unavailable"


def _traffic_provider_status() -> dict:
    """Read short-lived tile-service health without ever handling a provider key."""
    internal_url = os.getenv("TRAFFIC_TILE_INTERNAL_URL", TRAFFIC_TILE_INTERNAL_DEFAULT).rstrip("/")
    now = time.monotonic()
    cached = _PROVIDER_STATUS_CACHE.get(internal_url)
    if cached is not None and now - cached[0] < TRAFFIC_PROVIDER_STATUS_TTL_SECONDS:
        return dict(cached[1])

    request = urllib.request.Request(f"{internal_url}/status", headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=TRAFFIC_PROVIDER_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("invalid status response")
        last_success_ms = payload.get("last_success_ms")
        if isinstance(last_success_ms, bool) or not isinstance(last_success_ms, int) or last_success_ms < 0:
            last_success_ms = None
        result = {
            "configured": bool(payload.get("configured")),
            "available": bool(payload.get("available")),
            "last_success_ms": last_success_ms,
            "error": _safe_provider_error(payload.get("error")),
        }
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError):
        result = {
            "configured": False,
            "available": False,
            "last_success_ms": None,
            "error": "traffic tile service unavailable",
        }
    _PROVIDER_STATUS_CACHE[internal_url] = (time.monotonic(), result)
    return dict(result)


def _traffic_raster_layer(now_ms: int) -> dict:
    public_url = os.getenv("TRAFFIC_TILE_PUBLIC_URL", TRAFFIC_TILE_PUBLIC_DEFAULT).rstrip("/")
    bucket = now_ms // TRAFFIC_TILE_BUCKET_MS
    return {
        "sourcetype": "raster",
        "source": [f"{public_url}/tiles/{{z}}/{{x}}/{{y}}.png?v={bucket}"],
        "sourceattribution": "© TomTom",
        "below": "traces",
    }


def _carto_basemap_layer() -> dict:
    """Geography-only Mumbai canvas: land/water, almost no drawn streets.

    Plotly ``white-bg`` is blank until this raster is attached.  Using Carto
    light_nolabels avoids stacking OSM streets under our corridor traces.
    """
    return {
        "sourcetype": "raster",
        "source": [CARTO_TILE_URL],
        "sourceattribution": CARTO_ATTRIBUTION,
        "below": "traces",
    }


def _map_layers(*, include_provider: bool = False, now_ms: int = 0, provider_status: dict | None = None) -> list[dict]:
    layers = [_carto_basemap_layer()]
    provider_status = provider_status or {}
    if include_provider and provider_status.get("configured"):
        if provider_status.get("available") or not provider_status.get("error"):
            layers.append(_traffic_raster_layer(now_ms))
    return layers


def _local_street_trace() -> go.Scattermap | None:
    if not LOCAL_EDGES:
        return None
    lons: list[float | None] = []
    lats: list[float | None] = []
    for edge in LOCAL_EDGES:
        geometry = edge.get("geometry") or []
        if len(geometry) < 2:
            continue
        for point in geometry:
            lons.append(float(point[0]))
            lats.append(float(point[1]))
        lons.append(None)
        lats.append(None)
    if not lons:
        return None
    return go.Scattermap(
        lon=lons, lat=lats, mode="lines",
        line=dict(width=1.2, color="#9ca3af"),
        opacity=0.55, name="OSM local streets",
        hoverinfo="skip", showlegend=True, uid="local-streets",
    )


def create_traffic_map(
    data: dict,
    best_route: list[str] | None = None,
    usual_route: list[str] | None = None,
    now_ms: int | None = None,
    *,
    traffic_layer: str = TRAFFIC_LAYER_ESTIMATES,
    show_nodes: bool = False,
    provider_status: dict | None = None,
    best_route_nodes: list[str] | None = None,
    usual_route_nodes: list[str] | None = None,
    include_provider: bool = False,
) -> go.Figure:
    """Our corridors and OSM local streets on a street-free Carto canvas.

    TomTom flow tiles are never mixed into this figure.  Use
    ``create_provider_validation_map`` for external validation.
    """
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    del include_provider, provider_status, traffic_layer
    fig = go.Figure()
    local_trace = _local_street_trace()
    if local_trace is not None:
        fig.add_trace(local_trace)

    for segment in SEGMENTS:
        message = data.get(segment["segment_id"], {})
        speed, free = _speed_values(segment, message, now_ms)
        color, status = _segment_color(segment, message, now_ms)
        geometry, geometry_source = _segment_geometry(segment)
        speed_text = f"{speed:.1f} km/h" if speed is not None else "unavailable"
        hover = (
            f"<b>{segment['name']}</b><br>Status: {status}"
            f"<br>Current speed: {speed_text}"
            f"<br>Free-flow limit: {free:.0f} km/h"
            f"<br>Source observation age: {_source_age(message, now_ms)}"
            f"<br>Derived prediction: {_prediction_label(message)}"
            f"<br>Geometry source: {geometry_source}"
        )
        fig.add_trace(go.Scattermap(
            lon=[point[0] for point in geometry], lat=[point[1] for point in geometry],
            mode="lines", line=dict(width=max(3, 1 + segment["lanes"]), color=color),
            hovertext=hover, hoverinfo="text", showlegend=False,
            uid=f"corridor-{segment['segment_id']}",
        ))

    for name, route, route_nodes, color, width, opacity, uid in (
        ("Usual route", usual_route, usual_route_nodes, "#8e44ad", 5, 0.55, "route-usual"),
        ("Best route", best_route, best_route_nodes, "#1d4ed8", 7, 0.95, "route-best"),
    ):
        if not route:
            continue
        geometry = _route_geometry(route, route_nodes)
        if not geometry:
            continue
        fig.add_trace(go.Scattermap(
            lon=[point[0] for point in geometry], lat=[point[1] for point in geometry],
            mode="lines", line=dict(width=width, color=color),
            opacity=opacity, name=name, legendgroup=name, showlegend=True,
            hovertext=f"{name} · sourced road geometry", hoverinfo="text",
            uid=uid,
        ))

    fig.add_trace(go.Scattermap(
        lon=[pos[1] for pos in NODES.values()], lat=[pos[0] for pos in NODES.values()],
        mode="markers+text" if show_nodes else "markers",
        marker=dict(size=8, color="#111827"),
        text=list(NODES) if show_nodes else None,
        textposition="top center", textfont=dict(size=11),
        hoverinfo="text", hovertext=list(NODES),
        showlegend=False, uid="nodes",
    ))

    fig.update_layout(
        title="Spark corridor estimates on Carto geography (no street basemap, no TomTom overlay)",
        map=dict(
            style="white-bg",
            center=dict(lon=72.87, lat=19.11),
            zoom=9.9,
            layers=_map_layers(include_provider=False),
            uirevision="traffic-map",
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=34, b=0), height=600,
        legend=dict(orientation="h", yanchor="bottom", y=0.01, xanchor="left", x=0.01),
        uirevision="traffic-map", hovermode="closest",
    )
    return fig


def create_provider_validation_map(now_ms: int | None = None, provider_status: dict | None = None) -> go.Figure:
    """TomTom flow tiles alone, for external validation — not mixed with our traces."""
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    provider_status = provider_status or {}
    fig = go.Figure()
    fig.add_trace(go.Scattermap(
        lon=[72.87], lat=[19.11], mode="markers",
        marker=dict(size=1, color="rgba(0,0,0,0)"),
        hoverinfo="skip", showlegend=False, uid="provider-anchor",
    ))
    fig.update_layout(
        title="TomTom live traffic tiles (validation only — not MapYourWay corridors)",
        map=dict(
            style="white-bg",
            center=dict(lon=72.87, lat=19.11),
            zoom=9.9,
            layers=_map_layers(
                include_provider=True, now_ms=now_ms, provider_status=provider_status
            ),
            uirevision="provider-map",
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=34, b=0), height=600,
        uirevision="provider-map",
    )
    return fig


def create_speed_chart(data: dict, now_ms: int | None = None) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=[s["name"] for s in SEGMENTS],
        y=[_speed_values(s, data.get(s["segment_id"], {}), now_ms)[1] for s in SEGMENTS],
        name="Free-flow limit", marker_color="lightblue", uid="free-flow-speeds",
    ))
    fig.add_trace(go.Bar(
        x=[s["name"] for s in SEGMENTS],
        y=[_speed_values(s, data.get(s["segment_id"], {}), now_ms)[0] for s in SEGMENTS],
        name="Observed current speed", marker_color="darkblue", uid="observed-speeds",
    ))
    fig.update_layout(
        title=f"Observed vs Free-flow Speeds — {MONITORED_CORRIDOR_COUNT} monitored corridors",
        xaxis_title="Monitored corridor", yaxis_title="Speed (km/h)",
        barmode="group", height=400, xaxis_tickangle=-45, uirevision="speed-chart",
    )
    return fig


def _table_rows(data: dict, now_ms: int, delivery_age: str = "N/A") -> list[dict]:
    rows = []
    for segment in SEGMENTS:
        message = data.get(segment["segment_id"], {})
        speed, free = _speed_values(segment, message, now_ms)
        rows.append({
            "Monitored Corridor": segment["name"],
            "Current Speed": f"{speed:.1f} km/h" if speed is not None else "N/A",
            "Free Flow": f"{free:.1f} km/h",
            "Congestion": f"{max(0, min(1, 1-speed/free))*100:.0f}%" if speed is not None and free else "N/A",
            "Status": observation_status(message, now_ms),
            "Source Observation Age": _source_age(message, now_ms),
            "Delivery Age": delivery_age,
            "Derived Prediction": _prediction_label(message),
            "Forecast Source": message.get("forecast_source") or "Unavailable",
            "Forecast Model ID": message.get("forecast_model_id") or "N/A",
            "Weather": "available" if message.get("weather_available") else "missing",
            "Travel Time": f"{segment['length_km']/max(speed, 1)*3600:.0f} s" if speed is not None else "N/A",
        })
    return rows


def _route_rows(route: dict | None) -> list[dict]:
    if not route:
        return []
    rows = [{
        "Route": "Best route",
        "Junctions": route["best"].label(),
        "Travel time": f"{route['best'].minutes:.1f} min",
        "Distance": f"{route['best'].km:.1f} km",
        "Mode": route["mode"],
    }]
    if route.get("usual") is not None:
        rows.append({
            "Route": "Usual route",
            "Junctions": route["usual"].label(),
            "Travel time": f"{route['usual'].minutes:.1f} min",
            "Distance": f"{route['usual'].km:.1f} km",
            "Mode": "free-flow baseline",
        })
    return rows


def _render_provider_context(slot, provider_status: dict, traffic_layer: str) -> None:
    with slot.container():
        if traffic_layer == TRAFFIC_LAYER_LIVE and provider_status.get("available"):
            provider_note = "TomTom live traffic overlay is active."
        elif traffic_layer == TRAFFIC_LAYER_LIVE and provider_status.get("configured"):
            provider_note = "Mumbai road map is active; TomTom traffic overlay is waiting for its first healthy tile."
        elif traffic_layer == TRAFFIC_LAYER_LIVE:
            provider_note = "Mumbai road map is active; live overlay is unavailable, so corridor estimates remain visible."
        else:
            provider_note = "Showing the complete Mumbai road map with Spark corridor estimates."
        st.caption(
            f"{provider_note} Roads are coloured by Kafka/Spark congestion "
            "(green = free-flow → red = severe) on sourced road geometry. "
            "Basemap © OpenStreetMap contributors; corridor geometry © OSRM."
        )


def main():
    """Render the static shell once and refresh only the dynamic placeholders."""
    st.title("🚗 MapYourWay - Mumbai Traffic Analytics")
    st.caption(
        f"19 monitored traffic corridors · live provider traffic, Kafka observations, and derived route analytics"
    )

    state, _consumer = get_live_state()
    st.sidebar.header("⚙️ Settings")
    refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 3, 30, 5, key="refresh-rate")
    show_details = st.sidebar.checkbox("Show Detailed Stats", value=True, key="show-details")
    traffic_layer = st.sidebar.radio(
        "Traffic layer", [TRAFFIC_LAYER_LIVE, TRAFFIC_LAYER_ESTIMATES],
        index=0, key="traffic-layer",
    )
    show_nodes = st.sidebar.checkbox("Show junctions", value=False, key="show-junctions")
    st.sidebar.header("🧭 Route Planner")
    node_names = list(NODES)
    origin = st.sidebar.selectbox("Origin", node_names, index=node_names.index("Andheri"), key="route-origin")
    destination = st.sidebar.selectbox("Destination", node_names, index=node_names.index("Dadar"), key="route-destination")
    prefer_forecast = st.sidebar.checkbox(
        "Prefer forecast speeds", value=True, key="prefer-forecast",
        help="Uses a complete valid forecast when available, otherwise falls back to current live speeds.",
    )

    metric_slot = st.empty()
    provider_slot = st.empty()
    route_slot = st.sidebar.empty()
    delivery_slot = st.sidebar.empty()

    tab_map, tab_stats, tab_table = st.tabs(["🗺️ Live Map", "📊 Analytics", "📋 Data Table"])
    with tab_map:
        map_slot = st.empty()
        map_context_slot = st.empty()
    with tab_stats:
        stats_slot = st.empty()
    with tab_table:
        table_slot = st.empty()

    @st.fragment(run_every=refresh_rate)
    def render_dynamic():
        # Exactly one coherent state snapshot feeds every dynamic view.
        data, history, count, last_update, error = state.get_snapshot()
        del history
        now_ms = int(time.time() * 1000)
        provider_status = _traffic_provider_status()
        fresh = sum(
            observation_status(data.get(segment["segment_id"], {}), now_ms) == "fresh"
            for segment in SEGMENTS
        )
        pipeline_status = _pipeline_status(last_update, now_ms, fresh, error)
        delivery_age = _delivery_age(last_update, now_ms)
        has_forecast = forecast_available(data, now_ms)
        route = None

        if origin == destination:
            with route_slot.container():
                st.warning("Choose two different junctions")
        else:
            try:
                route = suggest_live_route(
                    origin, destination, data, now_ms=now_ms, use_forecast=prefer_forecast,
                )
                with route_slot.container():
                    st.caption(f"Best route: {route['best'].label()} — {route['best'].minutes:.1f} min")
                    if route["usual"] is None:
                        st.warning("Usual route unavailable; savings cannot be calculated.")
                    elif route["reroute"]:
                        st.success(f"Take alternate route; saves {route['saving_min']:.1f} min")
                    else:
                        st.info("Usual route remains the fastest option")
                    st.caption(f"Mode: {route['mode']}; fresh observation coverage: {route['live_coverage']:.0%}")
                    if route.get("blended"):
                        st.info(
                            f"Live coverage {route['live_coverage']:.0%}; "
                            "remaining roads use free-flow speeds."
                        )
                    if route.get("forecast_fallback"):
                        st.caption("Forecast preference enabled; no complete valid forecast, so current speeds are used.")
                    elif not has_forecast:
                        st.caption("Forecast unavailable; current speeds are used.")
                    st.caption("Alternatives: " + "; ".join(
                        f"{candidate.label()} ({candidate.minutes:.1f} min)"
                        for candidate in route["alternatives"]
                    ))
                    st.caption(f"A*: {route['expanded_astar']} junctions; Dijkstra: {route['expanded_dijkstra']}")
            except (nx.NetworkXException, ValueError, KeyError) as exc:
                with route_slot.container():
                    st.warning(f"Route unavailable: {exc}")

        with metric_slot.container():
            a, b, c, d, e = st.columns(5)
            a.metric("Fresh observations", f"{fresh}/{MONITORED_CORRIDOR_COUNT}")
            b.metric("Monitored corridors", str(MONITORED_CORRIDOR_COUNT))
            c.metric("Source observation age", _latest_source_age(data, now_ms))
            d.metric("Delivery age", _delivery_age(last_update, now_ms))
            e.metric("Pipeline status", pipeline_status)
            if error:
                st.error(error)

        _render_provider_context(provider_slot, provider_status, traffic_layer)
        with delivery_slot.container():
            last_delivery = datetime.fromtimestamp(last_update).strftime("%H:%M:%S") if last_update else "waiting"
            st.caption(f"Last Kafka delivery: {last_delivery} · source observation age is independent of delivery age.")

        with map_slot.container():
            st.plotly_chart(
                create_traffic_map(
                    data,
                    route["best"].segment_ids if route else None,
                    route["usual"].segment_ids if route and route["usual"] else None,
                    now_ms,
                    traffic_layer=traffic_layer,
                    show_nodes=show_nodes,
                    provider_status=provider_status,
                    best_route_nodes=route["best"].nodes if route else None,
                    usual_route_nodes=route["usual"].nodes if route and route["usual"] else None,
                ),
                use_container_width=True, key="traffic-map-chart",
            )
        with map_context_slot.container():
            if not data:
                st.info(
                    "Waiting for Kafka observations. The complete Mumbai road map "
                    "is already visible; corridor colours update when predictions arrive."
                )
            st.caption(
                "Green→red colouring is live congestion per corridor. The solid blue line is the best route now; "
                "the dotted purple line is the usual free-flow route."
            )

        with stats_slot.container():
            st.plotly_chart(create_speed_chart(data, now_ms), use_container_width=True, key="speed-chart")
            if show_details:
                congestion = [
                    max(0, min(1, 1 - speed / free))
                    for segment in SEGMENTS
                    for speed, free in [_speed_values(segment, data.get(segment["segment_id"], {}), now_ms)]
                    if speed is not None and observation_status(data.get(segment["segment_id"], {}), now_ms) == "fresh"
                ]
                x, y, z = st.columns(3)
                x.metric("Average Congestion", f"{sum(congestion) / len(congestion) * 100:.1f}%" if congestion else "N/A")
                y.metric("High/Severe Corridors", str(sum(value > .6 for value in congestion)))
                z.metric("Free-Flow Corridors", str(sum(value < .3 for value in congestion)))
            route_rows = _route_rows(route)
            if route_rows:
                st.subheader("Route analytics")
                st.dataframe(pd.DataFrame(route_rows), use_container_width=True, hide_index=True, key="route-analytics")
            else:
                st.caption("Route analytics appear when the selected junctions have a usable live route.")

        with table_slot.container():
            st.dataframe(
                pd.DataFrame(_table_rows(data, now_ms, delivery_age)),
                use_container_width=True, hide_index=True, key="corridor-data-table",
            )

    render_dynamic()


if __name__ == "__main__":
    main()
