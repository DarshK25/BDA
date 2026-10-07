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

import heapq

import networkx as nx

from cgls.network import NODES, SEGMENTS, haversine_km

MIN_SPEED_KMPH = 5.0       # a fully jammed road is slow, not infinitely slow
REROUTE_MIN_SAVING_MIN = 0.5
MAX_FREE_FLOW_KMPH = max(s["free_flow_kmph"] for s in SEGMENTS)
ALPHA, BETA = 2.5, 0.4        # penalty-model weights


@dataclass
class Route:
    nodes: list[str]
    segment_ids: list[str]
    minutes: float
    km: float

    def label(self) -> str:
        return " -> ".join(self.nodes)


def build_graph(speeds: dict[str, float] | None = None) -> nx.Graph:
    """speeds: segment_id -> km/h. Missing segments use their free-flow speed."""
    g = nx.Graph()
    for s in SEGMENTS:
        v = s["free_flow_kmph"]
        if speeds and s["segment_id"] in speeds and speeds[s["segment_id"]] is not None:
            v = min(max(float(speeds[s["segment_id"]]), MIN_SPEED_KMPH), s["free_flow_kmph"])
        g.add_edge(s["node_a"], s["node_b"], segment_id=s["segment_id"], km=s["length_km"],
                   minutes=s["length_km"] / v * 60.0)
    return g


def weather_penalty(rain_mm_hr: float, visibility_km: float) -> float:
    """0 (clear) .. 1 (storm, fog): heavy rain and poor visibility slow every driver."""
    return max(0.0, min(1.0, 0.7 * min(rain_mm_hr, 25.0) / 25.0 + 0.3 * (10.0 - min(visibility_km, 10.0)) / 10.0))


def build_graph_penalty(scores: dict[str, float], wx_penalty: float = 0.0,
                        alpha: float = ALPHA, beta: float = BETA) -> nx.Graph:
    """Penalty model: free-flow minutes * (1 + alpha*congestion_score + beta*weather_penalty)."""
    g = nx.Graph()
    for s in SEGMENTS:
        free = s["length_km"] / s["free_flow_kmph"] * 60.0
        score = scores.get(s["segment_id"], 0.0) if scores else 0.0
        g.add_edge(s["node_a"], s["node_b"], segment_id=s["segment_id"], km=s["length_km"],
                   minutes=free * (1.0 + alpha * score + beta * wx_penalty))
    return g


def _heuristic(a: str, b: str) -> float:
    """Lower bound on travel minutes: straight line at the city's top speed (never overestimates)."""
    return haversine_km(NODES[a], NODES[b]) / MAX_FREE_FLOW_KMPH * 60.0


def search(g: nx.Graph, origin: str, dest: str, use_heuristic: bool = True):
    """A* (use_heuristic=True) or Dijkstra (False). Returns (Route, number_of_nodes_expanded)."""
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


def alternatives(origin: str, dest: str, speeds: dict[str, float] | None, k: int = 3) -> list[Route]:
    """The k fastest simple routes under the given speeds, fastest first."""
    g = build_graph(speeds)
    return [_as_route(g, p) for p in islice(nx.shortest_simple_paths(g, origin, dest, weight="minutes"), k)]


def compare_graph(origin: str, dest: str, live: nx.Graph) -> dict:
    """Usual route (fastest when roads are empty) vs. best route right now, on a ready-made live graph."""
    if origin == dest:
        raise ValueError("origin and destination are the same")
    usual_nodes = nx.shortest_path(build_graph(None), origin, dest, weight="minutes")
    usual_now = _as_route(live, usual_nodes)                   # usual route, priced at today's speeds
    best, expanded_astar = search(live, origin, dest, use_heuristic=True)
    _, expanded_dijkstra = search(live, origin, dest, use_heuristic=False)
    saving = usual_now.minutes - best.minutes
    alts = [_as_route(live, p) for p in islice(nx.shortest_simple_paths(live, origin, dest, weight="minutes"), 3)]
    return {
        "usual": usual_now, "best": best, "alternatives": alts, "saving_min": saving,
        "reroute": best.nodes != usual_nodes and saving >= REROUTE_MIN_SAVING_MIN,
        "expanded_astar": expanded_astar, "expanded_dijkstra": expanded_dijkstra,
    }


def compare(origin: str, dest: str, live_speeds: dict[str, float]) -> dict:
    """Same as compare_graph with the physical 'speed' weight model."""
    return compare_graph(origin, dest, build_graph(live_speeds))
