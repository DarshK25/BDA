"""
Pytest configuration and fixtures for the CGLS test suite.
"""

import pytest
import sys
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


@pytest.fixture
def sample_junction():
    """Sample junction data for testing."""
    from cgls.network import Junction
    return Junction("J1", "Test Junction", 40.7580, -73.9855, True)


@pytest.fixture
def sample_road():
    """Sample road data for testing."""
    from cgls.network import Road
    return Road("R1", "Test Road", "J1", "J2", 1.5, 3, 50, 180)


@pytest.fixture
def mock_kafka_message():
    """Mock Kafka message for testing."""
    return {
        "road_id": "R1",
        "timestamp": "2024-01-01T12:00:00",
        "avg_speed": 45.0,
        "avg_occupancy": 0.6,
        "vehicle_count": 25
    }


@pytest.fixture
def sample_congestion_data():
    """Sample congestion data for ML testing."""
    import pandas as pd
    return pd.DataFrame({
        "avg_speed": [50, 40, 30, 20, 45],
        "avg_occupancy": [0.3, 0.5, 0.7, 0.9, 0.4],
        "vehicle_count": [10, 20, 30, 40, 15],
        "time_of_day_sin": [0.5, 0.7, 0.9, 0.3, 0.6],
        "time_of_day_cos": [0.8, 0.6, 0.4, 0.9, 0.7],
        "congestion_level": [0.2, 0.4, 0.6, 0.8, 0.3]
    })


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    
    spark = (SparkSession.builder
             .master("local[2]")
             .appName("CGLS-Tests")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    
    yield spark
    
    spark.stop()
