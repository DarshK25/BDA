import random

import networkx as nx

from cgls import traffic_model as tm
from cgls.network import SEGMENTS, SEGMENT_BY_ID, neighbours
from cgls.routing import build_graph


def test_segment_table_is_sane():
    assert len(SEGMENTS) == len(SEGMENT_BY_ID) == 19
    for s in SEGMENTS:
        assert s["length_km"] > 0.5
        assert s["free_flow_kmph"] > 20 and s["lanes"] >= 2


def test_city_is_connected():
    assert nx.is_connected(build_graph())


def test_neighbours_share_a_junction():
    assert "S04" in neighbours("S05") and "S05" not in neighbours("S05")


def test_level_thresholds():
    assert [tm.level_from_score(x) for x in (0.0, 0.24, 0.25, 0.49, 0.5, 0.74, 0.75, 1.0)] == [0, 0, 1, 1, 2, 2, 3, 3]


def test_more_congestion_means_slower_and_busier():
    rng = random.Random(0)
    low = [tm.sample_speed(50, 0.1, rng) for _ in range(500)]
    high = [tm.sample_speed(50, 0.9, rng) for _ in range(500)]
    assert sum(low) / 500 > 2 * sum(high) / 500
    occ_low = sum(tm.sample_occupancy(0.1, rng) for _ in range(500)) / 500
    occ_high = sum(tm.sample_occupancy(0.9, rng) for _ in range(500)) / 500
    assert occ_high > occ_low + 50


def test_rain_and_incident_raise_congestion():
    rng = random.Random(1)
    dry = sum(tm.congestion_index(0.3, 1.0, 0.0, 0.0, rng) for _ in range(300)) / 300
    wet = sum(tm.congestion_index(0.3, 1.0, 20.0, 0.0, rng) for _ in range(300)) / 300
    jam = sum(tm.congestion_index(0.3, 1.0, 0.0, 0.5, rng) for _ in range(300)) / 300
    assert wet > dry + 0.1 and jam > dry + 0.4
