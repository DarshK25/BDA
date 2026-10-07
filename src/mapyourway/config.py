"""
Configuration and constants for the MapYourWay system (Mumbai Traffic Analytics).
Defines Kafka topics, data folders, window sizes, and system parameters.
"""

import os
from typing import Dict, Any

# ============================================================================
# KAFKA CONFIGURATION
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topic names for MapYourWay Mumbai
TOPIC_GPS = "mapyourway.gps"
TOPIC_SENSOR = "mapyourway.sensor"
TOPIC_WEATHER = "mapyourway.weather"
TOPIC_CONGESTION = "mapyourway.congestion"
TOPIC_ROUTES = "mapyourway.routes"

ALL_INPUT_TOPICS = [TOPIC_GPS, TOPIC_SENSOR, TOPIC_WEATHER]
ALL_OUTPUT_TOPICS = [TOPIC_CONGESTION, TOPIC_ROUTES]

# ============================================================================
# DATA PATHS
# ============================================================================

BASE_DATA_DIR = os.getenv("DATA_DIR", "/app/data")

# Subdirectories
MODEL_DIR = os.path.join(BASE_DATA_DIR, "models")
LAKE_DIR = os.path.join(BASE_DATA_DIR, "lake")
EXPORT_DIR = os.path.join(BASE_DATA_DIR, "export")
GROUND_TRUTH_DIR = os.path.join(BASE_DATA_DIR, "ground_truth")
REPORTS_DIR = os.path.join(BASE_DATA_DIR, "reports")
CHECKPOINT_DIR = os.path.join(BASE_DATA_DIR, "checkpoints")

# Specific paths
CONGESTION_LAKE = os.path.join(LAKE_DIR, "segment_congestion")
CONGESTION_CSV = os.path.join(EXPORT_DIR, "congestion.csv")
GROUND_TRUTH_CSV = os.path.join(GROUND_TRUTH_DIR, "gt.csv")
METRICS_JSON = os.path.join(MODEL_DIR, "metrics.json")

# ============================================================================
# STREAMING PARAMETERS
# ============================================================================

# Micro-batch window sizes
WINDOW_DURATION = "30 seconds"  # Aggregation window
SLIDE_DURATION = "10 seconds"   # Update frequency
WATERMARK_DELAY = "1 minute"    # Late data handling

# Batch configuration
TRIGGER_PROCESSING_TIME = "10 seconds"

# ============================================================================
# NETWORK CONFIGURATION
# ============================================================================

# Network topology
NUM_JUNCTIONS = 13
NUM_ROADS = 19

# Traffic model parameters
CONGESTION_THRESHOLDS = {
    "low": 0.3,      # Below 30% occupancy
    "medium": 0.6,   # 30-60% occupancy
    "high": 0.8,     # 60-80% occupancy
    "severe": 1.0    # Above 80% occupancy
}

# Speed parameters (km/h)
MIN_SPEED = 5.0
MAX_SPEED = 60.0
FREE_FLOW_SPEED = 50.0

# ============================================================================
# SIMULATOR CONFIGURATION
# ============================================================================

SIMULATOR_CONFIG = {
    "gps_interval": 5,        # seconds between GPS updates
    "sensor_interval": 10,    # seconds between sensor readings
    "weather_interval": 60,   # seconds between weather updates
    "num_vehicles": 100,      # number of simulated vehicles
    "simulation_speed": 1.0,  # real-time multiplier
}

# ============================================================================
# MACHINE LEARNING CONFIGURATION
# ============================================================================

ML_CONFIG = {
    "test_size": 0.2,
    "random_state": 42,
    "cv_folds": 5,
    
    # RandomForest parameters
    "rf_n_estimators": 100,
    "rf_max_depth": 15,
    "rf_min_samples_split": 10,
    
    # GBT parameters
    "gbt_n_estimators": 100,
    "gbt_max_depth": 5,
    "gbt_learning_rate": 0.1,
}

# Feature names for model inputs
FEATURE_COLUMNS = [
    "avg_speed",
    "avg_occupancy",
    "vehicle_count",
    "time_of_day_sin",
    "time_of_day_cos"
]

TARGET_COLUMN = "congestion_level"

# ============================================================================
# ROUTING CONFIGURATION
# ============================================================================

ROUTING_CONFIG = {
    "algorithm": "astar",  # or "dijkstra"
    "cost_model": "time",  # "time", "distance", or "hybrid"
    "congestion_weight": 0.7,  # How much to weight congestion vs distance
    "avoid_severe_threshold": 0.9,  # Avoid roads above this congestion level
}

# ============================================================================
# DASHBOARD CONFIGURATION
# ============================================================================

DASHBOARD_CONFIG = {
    "refresh_interval": 5,  # seconds
    "map_center": [40.7128, -74.0060],  # NYC coordinates (example)
    "map_zoom": 12,
    "max_history_points": 1000,
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def ensure_directories() -> None:
    """Create all required data directories if they don't exist."""
    directories = [
        BASE_DATA_DIR,
        MODEL_DIR,
        LAKE_DIR,
        EXPORT_DIR,
        GROUND_TRUTH_DIR,
        REPORTS_DIR,
        CHECKPOINT_DIR,
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)


def get_config_summary() -> Dict[str, Any]:
    """Return a summary of the current configuration."""
    return {
        "kafka_servers": KAFKA_BOOTSTRAP_SERVERS,
        "topics": {
            "input": ALL_INPUT_TOPICS,
            "output": ALL_OUTPUT_TOPICS,
        },
        "data_dir": BASE_DATA_DIR,
        "streaming": {
            "window": WINDOW_DURATION,
            "slide": SLIDE_DURATION,
            "watermark": WATERMARK_DELAY,
        },
        "network": {
            "junctions": NUM_JUNCTIONS,
            "roads": NUM_ROADS,
        },
        "simulator": SIMULATOR_CONFIG,
        "ml": ML_CONFIG,
        "routing": ROUTING_CONFIG,
    }


if __name__ == "__main__":
    import json
    print(json.dumps(get_config_summary(), indent=2))
