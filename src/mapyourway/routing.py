"""Dynamic route suggestion on the Mumbai network.

Each road's weight is its current travel time: length / live speed, plus a small
junction delay so a path will not dive through dozens of tiny local streets unless
the highway is actually worse.

  usual route   fastest path when every road is at its free-flow speed
  best route    fastest path at the speeds Spark just published

A* uses straight-line time at the fastest speed in the city as its heuristic.
That guess never overestimates, so the first time the search reaches the
destination it has found the real fastest path. Dijkstra is the same search
with a zero heuristic; we run it only to report how many extra junctions A*
avoids.
"""
from __future__ import annotations

from dataclasses import dataclass
import threading

import heapq

import networkx as nx

from mapyourway.network import (
    JUNCTIONS,
    PLACE_BY_NAME,
    SEGMENT_BY_ID,
    SEGMENTS,
    haversine_km,
)

MIN_SPEED_KMPH = 5.0
REROUTE_MIN_SAVING_MIN = 1.0
MAX_FREE_FLOW_KMPH = max((s["free_flow_kmph"] for s in SEGMENTS), default=60.0)


@dataclass
class Route:
    nodes: list[str]
    segment_ids: list[str]
    minutes: float
    km: float

    def roads(self) -> list[str]:
        names: list[str] = []
        for segment_id in self.segment_ids:
            name = SEGMENT_BY_ID[segment_id]["name"]
            if not names or names[-1] != name:
                names.append(name)
        return names

    def label(self) -> str:
        roads = self.roads()
        if len(roads) > 8:
            return " → ".join(roads[:4] + ["…"] + roads[-3:])
        return " → ".join(roads)


def resolve(place_or_node: str) -> str:
    if place_or_node in JUNCTIONS:
        return place_or_node
    place = PLACE_BY_NAME.get(place_or_node)
    if not place:
        raise KeyError(f"unknown place {place_or_node}")
    return place["node_id"]


def _add_edge(graph: nx.DiGraph, origin: str, dest: str, segment: dict, minutes: float) -> None:
    existing = graph.get_edge_data(origin, dest)
    if existing is not None and existing["minutes"] <= minutes:
        return
    graph.add_edge(
        origin,
        dest,
        segment_id=segment["segment_id"],
        km=segment["length_km"],
        minutes=minutes,
    )


_graph_lock = threading.Lock()
_free_graph: nx.DiGraph | None = None
_live_cache: dict = {"token": None, "graph": None}


def _build(speeds: dict[str, float] | None) -> nx.DiGraph:
    graph = nx.DiGraph()
    for segment in SEGMENTS:
        speed = segment["free_flow_kmph"]
        if speeds and speeds.get(segment["segment_id"]) is not None:
            speed = float(speeds[segment["segment_id"]])
        speed = max(speed, MIN_SPEED_KMPH)
        minutes = segment["length_km"] / speed * 60.0 + float(segment.get("delay_s", 0.0)) / 60.0
        _add_edge(graph, segment["node_a"], segment["node_b"], segment, minutes)
        # Motorways and trunks stay one-way: they are drawn as separate carriageways.
        # Smaller one-way streets are opened in both directions. Their opposite
        # carriageway is often a different OpenStreetMap way that never shares a
        # junction, and without this a trip from Borivali to Colaba has no path.
        divided = segment.get("road_class") in {"motorway", "motorway_link", "trunk", "trunk_link"}
        if not segment.get("oneway", False) or not divided:
            _add_edge(graph, segment["node_b"], segment["node_a"], segment, minutes)
    return graph


def build_graph(speeds: dict[str, float] | None = None) -> nx.DiGraph:
    """speeds: segment_id -> km/h. Missing segments use their free-flow speed."""
    global _free_graph
    with _graph_lock:
        if not speeds:
            if _free_graph is None:
                _free_graph = _build(None)
            return _free_graph
        token = tuple(sorted((key, round(float(value), 1)) for key, value in speeds.items()))
        if _live_cache["token"] == token:
            return _live_cache["graph"]
        graph = _build(speeds)
        _live_cache["token"] = token
        _live_cache["graph"] = graph
        return graph


def _heuristic(origin: str, dest: str) -> float:
    """Lower bound on minutes: straight line at the city's top speed."""
    if origin not in JUNCTIONS or dest not in JUNCTIONS:
        return 0.0
    return haversine_km(JUNCTIONS[origin], JUNCTIONS[dest]) / MAX_FREE_FLOW_KMPH * 60.0


def _as_route(graph: nx.DiGraph, nodes: list[str]) -> Route:
    segment_ids = []
    minutes = 0.0
    km = 0.0
    for origin, dest in zip(nodes, nodes[1:]):
        edge = graph[origin][dest]
        segment_ids.append(edge["segment_id"])
        minutes += edge["minutes"]
        km += edge["km"]
    return Route(nodes, segment_ids, minutes, km)


def search(graph: nx.DiGraph, origin: str, dest: str, use_heuristic: bool = True):
    """A* when use_heuristic is true, Dijkstra otherwise."""
    heuristic = _heuristic if use_heuristic else (lambda a, b: 0.0)
    best = {origin: 0.0}
    parent: dict[str, str] = {}
    heap = [(heuristic(origin, dest), 0.0, origin)]
    done: set[str] = set()
    while heap:
        _, cost, node = heapq.heappop(heap)
        if node in done:
            continue
        done.add(node)
        if node == dest:
            path = [node]
            while path[-1] in parent:
                path.append(parent[path[-1]])
            return _as_route(graph, path[::-1]), len(done)
        for nxt in graph[node]:
            next_cost = cost + graph[node][nxt]["minutes"]
            if next_cost < best.get(nxt, float("inf")):
                best[nxt] = next_cost
                parent[nxt] = node
                heapq.heappush(heap, (next_cost + heuristic(nxt, dest), next_cost, nxt))
    raise nx.NetworkXNoPath(f"{origin} -> {dest}")


def _overlap(left: list[str], right: list[str]) -> float:
    if not left:
        return 0.0
    return len(set(left) & set(right)) / len(left)


def corridors(
    start: str,
    goal: str,
    live_speeds: dict[str, float],
    live_graph: nx.DiGraph,
    fastest: Route,
    limit: int = 4,
) -> list[Route]:
    """Several different drives, each timed at the live speeds.

    ``fastest`` is the quickest path at the speeds Spark published. Each later
    search pretends the roads already chosen are much slower, so it has to
    leave that corridor. The reported time is always the real live time.
    """
    chosen: list[Route] = [fastest]
    penalty_graph = live_graph.copy()

    def penalize(segment_ids: list[str], factor: float) -> None:
        selected = set(segment_ids)
        for _, _, edge in penalty_graph.edges(data=True):
            if edge["segment_id"] in selected:
                edge["minutes"] *= factor

    penalize(fastest.segment_ids, 4.0)
    attempts = 0
    while len(chosen) < limit and attempts < limit + 3:
        attempts += 1
        try:
            found, _ = search(penalty_graph, start, goal, use_heuristic=True)
        except nx.NetworkXNoPath:
            break
        priced = _as_route(live_graph, found.nodes)
        duplicate = any(_overlap(priced.segment_ids, earlier.segment_ids) >= 0.8 for earlier in chosen)
        if not duplicate:
            chosen.append(priced)
        penalize(priced.segment_ids, 10.0 if duplicate else 4.0)
    chosen.sort(key=lambda route: route.minutes)
    return chosen


def compare(origin: str, dest: str, live_speeds: dict[str, float], corridor_count: int = 2) -> dict:
    """Usual free-flow route versus the fastest route at the speeds given now."""
    start, goal = resolve(origin), resolve(dest)
    if start == goal:
        raise ValueError("origin and destination are the same place")
    free_graph = build_graph(None)
    live_graph = build_graph(live_speeds)
    usual_nodes = nx.shortest_path(free_graph, start, goal, weight="minutes")
    usual = _as_route(live_graph, usual_nodes)
    best, expanded_astar = search(live_graph, start, goal, use_heuristic=True)
    _, expanded_dijkstra = search(live_graph, start, goal, use_heuristic=False)
    saving = usual.minutes - best.minutes
    options = corridors(start, goal, live_speeds, live_graph, best, limit=max(1, corridor_count))
    if all(_overlap(usual.segment_ids, option.segment_ids) < 0.8 for option in options):
        options.append(usual)
        options.sort(key=lambda route: route.minutes)
    return {
        "usual": usual,
        "best": best,
        "alternatives": options,
        "corridors": options,
        "saving_min": saving,
        "reroute": best.segment_ids != usual.segment_ids and saving >= REROUTE_MIN_SAVING_MIN,
        "expanded_astar": expanded_astar,
        "expanded_dijkstra": expanded_dijkstra,
    }
