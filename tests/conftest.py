"""Shared fixtures for the MapYourWay test suite."""

import sys
from pathlib import Path

import pytest

src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


@pytest.fixture
def sample_segment():
    """A real segment from the shared Mumbai network definition."""
    from mapyourway.network import SEGMENT_BY_ID

    return SEGMENT_BY_ID["S01"]


@pytest.fixture
def mock_congestion_message():
    """Message matching the Spark congestion output contract."""
    return {
        "segment_id": "S01",
        "road_name": "Borivali-Malad",
        "timestamp": "2026-10-08T12:00:00Z",
        "congestion_level": 1,
        "confidence": 0.8,
        "speed_ratio": 0.65,
        "occupancy_pct": 42.0,
        "current_speed_kmph": 39.0,
        "free_flow_kmph": 60.0,
    }


@pytest.fixture(scope="session")
def spark_session():
    """Create one local Spark session for tests that opt into Spark."""
    pytest.importorskip("pyspark")
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("MapYourWay-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
