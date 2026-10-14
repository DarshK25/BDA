from cgls import routing
from cgls.network import SEGMENT_BY_ID


def free_flow():
    return {sid: s["free_flow_kmph"] for sid, s in SEGMENT_BY_ID.items()}


def test_no_reroute_when_roads_are_empty():
    r = routing.compare("Andheri", "Dadar", free_flow())
    assert not r["reroute"] and abs(r["saving_min"]) < 1e-9
    assert r["usual"].nodes == r["best"].nodes


def test_reroute_when_usual_route_jams():
    usual = routing.compare("Andheri", "Dadar", free_flow())["usual"]
    speeds = free_flow()
    jammed = usual.segment_ids[1]                    # jam a road in the middle of the usual route
    speeds[jammed] = 4.0
    r = routing.compare("Andheri", "Dadar", speeds)
    assert r["reroute"]
    assert jammed not in r["best"].segment_ids
    assert r["best"].minutes < r["usual"].minutes and r["saving_min"] > 1.0


def test_alternatives_sorted_and_distinct():
    alts = routing.alternatives("Borivali", "Worli", free_flow(), k=3)
    assert len(alts) == 3
    assert [a.minutes for a in alts] == sorted(a.minutes for a in alts)
    assert len({tuple(a.nodes) for a in alts}) == 3


def test_missing_speeds_default_to_free_flow():
    g1, g2 = routing.build_graph(None), routing.build_graph({})
    assert all(g1[u][v]["minutes"] == g2[u][v]["minutes"] for u, v in g1.edges)


def test_astar_matches_dijkstra_everywhere_and_never_expands_more():
    import random

    from cgls.network import NODES
    rng = random.Random(4)
    for _ in range(40):
        speeds = {sid: s["free_flow_kmph"] * rng.uniform(0.15, 1.0) for sid, s in SEGMENT_BY_ID.items()}
        g = routing.build_graph(speeds)
        o, d = rng.sample(sorted(NODES), 2)
        a, na = routing.search(g, o, d, use_heuristic=True)
        b, nb = routing.search(g, o, d, use_heuristic=False)
        assert abs(a.minutes - b.minutes) < 1e-9, (o, d)
        assert na <= nb + 1          # informed search should not explore more than uninformed


def test_heuristic_is_admissible():
    import random

    from cgls.network import NODES
    rng = random.Random(5)
    speeds = {sid: s["free_flow_kmph"] for sid, s in SEGMENT_BY_ID.items()}
    g = routing.build_graph(speeds)
    for o in NODES:
        for d in NODES:
            if o != d:
                assert routing._heuristic(o, d) <= routing.search(g, o, d, use_heuristic=False)[0].minutes + 1e-9


def test_penalty_model_reroutes_around_a_jam_and_weather_slows_everything():
    scores = {sid: 0.2 for sid in SEGMENT_BY_ID}
    usual = routing.compare("Andheri", "Dadar", free_flow())["usual"]
    scores[usual.segment_ids[1]] = 1.0
    r = routing.compare_graph("Andheri", "Dadar", routing.build_graph_penalty(scores, 0.0))
    assert r["reroute"] and usual.segment_ids[1] not in r["best"].segment_ids
    dry = routing.build_graph_penalty(scores, 0.0)
    wet = routing.build_graph_penalty(scores, routing.weather_penalty(20.0, 3.0))
    assert all(wet[u][v]["minutes"] > dry[u][v]["minutes"] for u, v in dry.edges)
    assert routing.weather_penalty(0, 10) == 0.0 and 0 < routing.weather_penalty(10, 8) < 1
