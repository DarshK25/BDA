import pytest

from cgls.features import compute_features
from cgls.streaming.job import add_features

pytestmark = pytest.mark.spark

CASES = [
    # avg_speed, avg_occ, avg_vc, avg_rain, avg_vis
    (30.0, 40.0, 6.0, 0.0, 10.0),
    (None, 70.0, 9.0, 5.0, 8.0),       # no GPS in the window -> fall back to occupancy
    (12.0, None, None, None, None),    # no sensor, no weather
    (None, None, None, None, None),    # empty window
    (80.0, 5.0, 2.0, 0.0, 10.0),       # faster than free-flow -> clipped
]


def test_spark_and_python_features_agree(spark):
    from pyspark.sql import Row

    rows = [Row(segment_id="S04", window=Row(start=None, end=None), avg_speed_kmph=c[0], gps_pings=1, vehicles=1,
                avg_occ=c[1], avg_vc=c[2], rain_mm_hr_raw=c[3], vis_raw=c[4]) for c in CASES]
    df = spark.createDataFrame(
        rows, "segment_id string, window struct<start:timestamp,end:timestamp>, avg_speed_kmph double, gps_pings long,"
              " vehicles long, avg_occ double, avg_vc double, rain_mm_hr_raw double, vis_raw double")
    got = add_features(df, spark).collect()
    got = sorted(got, key=lambda r: CASES.index((r.avg_speed_kmph, r.avg_occ, r.avg_vc, r.rain_mm_hr_raw, r.vis_raw)))
    from cgls.network import SEGMENT_BY_ID
    seg = SEGMENT_BY_ID["S04"]
    for case, row in zip(CASES, got):
        exp = compute_features(case[0], seg["free_flow_kmph"], case[1], case[2], seg["lanes"], case[3], case[4])
        for k, v in exp.items():
            assert row[k] == pytest.approx(v, rel=1e-9, abs=1e-9), (case, k)
