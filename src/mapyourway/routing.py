"""Dynamic route suggestion.

The road network is a weighted graph. The weight of each segment is its *current* travel
time, recomputed every 5 s from the Spark output. Two weight models are available:

  "speed"    minutes = length / live_speed                       (physical, default)
  "penalty"  minutes = free_flow_minutes * (1 + a*congestion + b*weather_penalty)
             (the formula often used in the literature; a = 2.5, b = 0.4 here)

The best route is found with A* (informed search using straight-line distance divided by the
fastest speed in the city as an admissible heuristic) and cross-checked against Dijkstra.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
import math
import time

import heapq

import networkx as nx

from mapyourway.network import NODES, SEGMENTS, haversine_km
from mapyourway.local_streets import (
    ALL_ROUTING_NODES,
    LOCAL_EDGES,
    LOCAL_LINKS,
    LOCAL_NODES,
    LOCAL_STREETS_AVAILABLE,
    inherit_local_speeds,
)
from mapyourway.contracts import observation_status

MIN_SPEED_KMPH = 5.0       # a fully jammed road is slow, not infinitely slow
REROUTE_MIN_SAVING_MIN = 0.5
MAX_FREE_FLOW_KMPH = max(
    [s["free_flow_kmph"] for s in SEGMENTS]
    + [s["free_flow_kmph"] for s in LOCAL_EDGES]
)
ALPHA, BETA = 2.5, 0.4        # penalty-model weights


@dataclass
class Route:
    nodes: list[str]
    segment_ids: list[str]
    minutes: float
    km: float

    def label(self) -> str:
        return " -> ".join(self.nodes)


def build_graph(
    speeds: dict[str, float] | None = None,
    closed_segments: set[str] | None = None,
    *,
    include_local: bool = True,
) -> nx.Graph:
    """speeds: segment_id -> km/h. Missing monitored segments use free-flow.

    Local OSM streets (when present) inherit a congestion ratio from the
    nearest TomTom-monitored corridor.  That is interpolation, not a second
    live observation.
    """
    g = nx.Graph()
    g.add_nodes_from(NODES)
    if include_local and LOCAL_STREETS_AVAILABLE:
        g.add_nodes_from(LOCAL_NODES)
    for s in SEGMENTS:
        if closed_segments and s["segment_id"] in closed_segments:
            continue
        v = s["free_flow_kmph"]
        if speeds and s["segment_id"] in speeds and speeds[s["segment_id"]] is not None:
            v = min(max(float(speeds[s["segment_id"]]), MIN_SPEED_KMPH), s["free_flow_kmph"])
        g.add_edge(s["node_a"], s["node_b"], segment_id=s["segment_id"], km=s["length_km"],
                   minutes=s["length_km"] / v * 60.0)
    if include_local and LOCAL_STREETS_AVAILABLE:
        inherited = inherit_local_speeds(speeds)
        for s in LOCAL_EDGES:
            if closed_segments and s["segment_id"] in closed_segments:
                continue
            v = s["free_flow_kmph"]
            if s["segment_id"] in inherited:
                v = min(max(float(inherited[s["segment_id"]]), MIN_SPEED_KMPH), s["free_flow_kmph"])
            g.add_edge(
                s["node_a"], s["node_b"],
                segment_id=s["segment_id"], km=s["length_km"],
                minutes=s["length_km"] / v * 60.0,
            )
        for link in LOCAL_LINKS:
            g.add_edge(
                link["node_a"], link["node_b"],
                segment_id=link["segment_id"], km=link["length_km"],
                minutes=link["length_km"] / link["free_flow_kmph"] * 60.0,
            )
    return g


def weather_penalty(rain_mm_hr: float, visibility_km: float) -> float:
    """0 (clear) .. 1 (storm, fog): heavy rain and poor visibility slow every driver."""
    return max(0.0, min(1.0, 0.7 * min(rain_mm_hr, 25.0) / 25.0 + 0.3 * (10.0 - min(visibility_km, 10.0)) / 10.0))


def build_graph_penalty(scores: dict[str, float], wx_penalty: float = 0.0,
                        alpha: float = ALPHA, beta: float = BETA) -> nx.Graph:
    """Penalty model: free-flow minutes * (1 + alpha*congestion_score + beta*weather_penalty)."""
    g = nx.Graph()
    g.add_nodes_from(NODES)
    if LOCAL_STREETS_AVAILABLE:
        g.add_nodes_from(LOCAL_NODES)
    for s in list(SEGMENTS) + list(LOCAL_EDGES):
        free = s["length_km"] / s["free_flow_kmph"] * 60.0
        score = scores.get(s["segment_id"], 0.0) if scores else 0.0
        g.add_edge(s["node_a"], s["node_b"], segment_id=s["segment_id"], km=s["length_km"],
                   minutes=free * (1.0 + alpha * score + beta * wx_penalty))
    for link in LOCAL_LINKS:
        free = link["length_km"] / link["free_flow_kmph"] * 60.0
        g.add_edge(
            link["node_a"], link["node_b"],
            segment_id=link["segment_id"], km=link["length_km"], minutes=free,
        )
    return g


def _heuristic(a: str, b: str) -> float:
    """Lower bound on travel minutes: straight line at the city's top speed (never overestimates)."""
    return haversine_km(ALL_ROUTING_NODES[a], ALL_ROUTING_NODES[b]) / MAX_FREE_FLOW_KMPH * 60.0


def search(g: nx.Graph, origin: str, dest: str, use_heuristic: bool = True):
    """A* (use_heuristic=True) or Dijkstra (False). Returns (Route, number_of_nodes_expanded)."""
    if origin not in g or dest not in g:
        raise nx.NodeNotFound(f"Unknown junction: {origin if origin not in g else dest}")
    h = _heuristic if use_heuristic else (lambda a, b: 0.0)
    best = {origin: 0.0}
    parent: dict[str, str] = {}
    heap = [(h(origin, dest), 0.0, origin)]
    done: set[str] = set()
    while heap:
        _, cost, u = heapq.heappop(heap)
        if u in done:
            continue
        done.add(u)
        if u == dest:
            path = [u]
            while path[-1] in parent:
                path.append(parent[path[-1]])
            return _as_route(g, path[::-1]), len(done)
        for v in g[u]:
            nc = cost + g[u][v]["minutes"]
            if nc < best.get(v, float("inf")):
                best[v], parent[v] = nc, u
                heapq.heappush(heap, (nc + h(v, dest), nc, v))
    raise nx.NetworkXNoPath(f"{origin} -> {dest}")


def _as_route(g: nx.Graph, nodes: list[str]) -> Route:
    segs = [g[u][v]["segment_id"] for u, v in zip(nodes, nodes[1:])]
    return Route(nodes, segs, sum(g[u][v]["minutes"] for u, v in zip(nodes, nodes[1:])),
                 sum(g[u][v]["km"] for u, v in zip(nodes, nodes[1:])))


def alternatives(
    origin: str,
    dest: str,
    speeds: dict[str, float] | None,
    k: int = 3,
    closed_segments: set[str] | None = None,
) -> list[Route]:
    """The k fastest simple routes under the given speeds, fastest first."""
    g = build_graph(speeds, closed_segments)
    try:
        return [_as_route(g, p) for p in islice(nx.shortest_simple_paths(g, origin, dest, weight="minutes"), k)]
    except nx.NetworkXNoPath:
        return []


def compare_graph(
    origin: str,
    dest: str,
    live: nx.Graph,
    closed_segments: set[str] | None = None,
    baseline: nx.Graph | None = None,
) -> dict:
    """Usual route (fastest when roads are empty) vs. best route right now, on a ready-made live graph.

    ``baseline`` is an optional graph used *only* to price the usual route and
    savings.  It should contain every non-closed road (live speed where fresh,
    free-flow otherwise) so the usual-route travel time stays available even
    when a segment on it currently lacks a fresh observation.  Without it the
    usual route is priced on ``live`` and may be unavailable.
    """
    if origin == dest:
        raise ValueError("origin and destination are the same")
    # The usual route is the fixed free-flow baseline; closures must not
    # redefine it. Price its current cost on the blended baseline when given.
    pricing = baseline if baseline is not None else live
    usual_nodes = nx.shortest_path(build_graph(), origin, dest, weight="minutes")
    usual_now = _as_route(pricing, usual_nodes) if nx.is_path(pricing, usual_nodes) else None
    best, expanded_astar = search(live, origin, dest, use_heuristic=True)
    dijkstra, expanded_dijkstra = search(live, origin, dest, use_heuristic=False)
    if not math.isclose(best.minutes, dijkstra.minutes, rel_tol=1e-10, abs_tol=1e-10):
        raise AssertionError("A* and Dijkstra disagree on route cost")
    saving = usual_now.minutes - best.minutes if usual_now else None
    alts = [_as_route(live, p) for p in islice(nx.shortest_simple_paths(live, origin, dest, weight="minutes"), 3)]
    return {
        "usual": usual_now, "usual_nodes": usual_nodes, "best": best,
        "alternatives": alts, "saving_min": saving,
        "reroute": saving is not None and best.nodes != usual_nodes and saving >= REROUTE_MIN_SAVING_MIN,
        "expanded_astar": expanded_astar, "expanded_dijkstra": expanded_dijkstra,
    }


def compare(
    origin: str,
    dest: str,
    live_speeds: dict[str, float],
    closed_segments: set[str] | None = None,
) -> dict:
    """Same as compare_graph with the physical 'speed' weight model."""
    return compare_graph(origin, dest, build_graph(live_speeds, closed_segments), closed_segments)


def live_inputs(messages: dict[str, dict], now_ms: int | None = None) -> tuple[dict[str, float], set[str], dict[str, str]]:
    """Use only fresh, confident observations; never infer free flow from absence."""
    speeds: dict[str, float] = {}
    closed: set[str] = set()
    statuses: dict[str, str] = {}
    for segment in SEGMENTS:
        segment_id = segment["segment_id"]
        message = messages.get(segment_id, {})
        status = observation_status(message, now_ms)
        statuses[segment_id] = status
        if status == "closed":
            closed.add(segment_id)
        elif status == "fresh":
            speeds[segment_id] = float(message["current_speed_kmph"])
    return speeds, closed, statuses


def _forecast_speed(message: dict, now_ms: int) -> float | None:
    """A forecast is usable only during its declared future prediction window."""
    try:
        speed = float(message["forecast_speed_kmph"])
        horizon = float(message["forecast_horizon_minutes"])
        for_ms = int(message["forecast_for_ms"])
        if (not math.isfinite(speed) or not 0 < speed <= 150
                or not math.isfinite(horizon) or horizon <= 0
                or not now_ms <= for_ms <= now_ms + horizon * 60000
                or not message.get("forecast_source")):
            return None
        return speed
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def forecast_available(messages: dict[str, dict], now_ms: int | None = None) -> bool:
    """Avoid mixing forecast and current weights in one advertised forecast route."""
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    speeds, _, _ = live_inputs(messages, now_ms)
    return bool(speeds) and all(_forecast_speed(messages[sid], now_ms) is not None for sid in speeds)


def suggest_live_route(
    origin: str,
    dest: str,
    messages: dict[str, dict],
    *,
    now_ms: int | None = None,
    use_forecast: bool = False,
) -> dict:
    """Pure Kafka-serving boundary: prefer fresh (or valid-forecast) edges, then
    fall back to a free-flow blend so a connected pair always gets a route.

    Precise mode treats only fresh observations as passable.  When that leaves
    origin and destination disconnected (empty or partial live coverage), the
    unobserved roads are refilled with their free-flow speed; only genuinely
    closed roads stay impassable.  A pair that is unreachable even then (its
    only paths are closed) is a legitimate no-path and still raises.
    """
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    speeds, closed, statuses = live_inputs(messages, now_ms)
    available = forecast_available(messages, now_ms)
    use_forecast_now = use_forecast and available
    # Forecast speeds replace current speeds only when a complete valid forecast
    # exists for every fresh segment; otherwise current live speeds are used.
    live_speeds = (
        {sid: _forecast_speed(messages[sid], now_ms) for sid in speeds}
        if use_forecast_now
        else dict(speeds)
    )

    # Unknown, stale and low-confidence observations are impassable in precise
    # mode so the advertised route reflects real live coverage.
    excluded = {sid for sid, status in statuses.items() if status != "fresh"}
    # Blended baseline: every non-closed road priced at its live speed when
    # fresh, otherwise free-flow.  Used to keep the usual-route cost (and the
    # savings figure) available even when a road on it lacks a fresh reading.
    baseline_graph = build_graph(live_speeds, set(closed))
    blended = False
    try:
        result = compare_graph(
            origin, dest, build_graph(live_speeds, excluded), baseline=baseline_graph
        )
        mode = "forecast" if use_forecast_now else "current"
    except nx.NetworkXNoPath:
        # Fresh-only edges do not connect the pair.  Blend free-flow speeds in
        # for every unobserved road (closures remain impassable) so routing
        # degrades gracefully instead of hard-failing.
        blended = True
        excluded = set(closed)
        result = compare_graph(
            origin, dest, build_graph(live_speeds, excluded), baseline=baseline_graph
        )
        mode = "live+free-flow blend" if live_speeds else "free-flow baseline"

    result.update(
        live_coverage=len(speeds) / len(SEGMENTS),
        closed_segments=sorted(closed), statuses=statuses,
        excluded_segments=sorted(excluded),
        mode=mode, blended=blended,
        forecast_available=available,
        forecast_fallback=use_forecast and not available,
    )
    return result
