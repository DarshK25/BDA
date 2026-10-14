"""
Configuration and constants for the MapYourWay system (Mumbai Traffic Analytics).
Defines Kafka topics, data folders, window sizes, and system parameters.
"""

import os
from typing import Any, Dict

# ============================================================================
# KAFKA CONFIGURATION
# ============================================================================

# Docker uses KAFKA_BOOTSTRAP_SERVERS; KAFKA_BOOTSTRAP is retained as a
# compatibility alias for local runs and older compose files.
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"),
)

# Kafka metric names treat dots and underscores as equivalent. Use one
# underscore-based spelling everywhere so a broker cannot contain a legacy
# underscore topic and a colliding dotted topic at the same time.
TOPIC_GPS = os.getenv("MAPYOURWAY_TOPIC_GPS", "mapyourway.gps_events")
TOPIC_SENSOR = os.getenv("MAPYOURWAY_TOPIC_SENSOR", "mapyourway.sensor_events")
TOPIC_WEATHER = os.getenv("MAPYOURWAY_TOPIC_WEATHER", "mapyourway.weather_events")
TOPIC_CONGESTION = os.getenv(
    "MAPYOURWAY_TOPIC_CONGESTION", "mapyourway.congestion_predictions"
)
TOPIC_ROUTES = os.getenv("MAPYOURWAY_TOPIC_ROUTES", "mapyourway.routes")

ALL_INPUT_TOPICS = [TOPIC_GPS, TOPIC_SENSOR, TOPIC_WEATHER]
ALL_OUTPUT_TOPICS = [TOPIC_CONGESTION, TOPIC_ROUTES]

# ============================================================================
# DATA PATHS
# ============================================================================

BASE_DATA_DIR = os.getenv("DATA_DIR", os.getenv("CGLS_DATA_DIR", "data"))

# Subdirectories
MODEL_DIR = os.path.join(BASE_DATA_DIR, "models")
LAKE_DIR = os.path.join(BASE_DATA_DIR, "lake")
EXPORT_DIR = os.path.join(BASE_DATA_DIR, "export")
GROUND_TRUTH_DIR = os.path.join(BASE_DATA_DIR, "ground_truth")
REPORTS_DIR = os.path.join(BASE_DATA_DIR, "reports")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", os.path.join(BASE_DATA_DIR, "checkpoints"))
RAW_DIR = os.path.join(BASE_DATA_DIR, "raw")
CHECKPOINT_VERSION = os.getenv("CHECKPOINT_VERSION", "congestion-output-v2")

# Specific paths
CONGESTION_LAKE = os.path.join(LAKE_DIR, "segment_congestion")
CONGESTION_CSV = os.path.join(EXPORT_DIR, "congestion.csv")
GROUND_TRUTH_CSV = os.path.join(GROUND_TRUTH_DIR, "gt.csv")
METRICS_JSON = os.path.join(MODEL_DIR, "metrics.json")
# A PipelineModel is stored in its own directory.  Keeping this separate from
# MODEL_DIR avoids confusing a parent directory with a Spark model path.
MODEL_PATH = os.getenv("MODEL_PATH", os.path.join(MODEL_DIR, "congestion_model"))
GBT_MODEL_PATH = os.getenv(
    "GBT_MODEL_PATH", os.path.join(MODEL_DIR, "congestion_gbt_model")
)
EVAL_METRICS_JSON = os.path.join(MODEL_DIR, "eval_metrics.json")
GBT_METRICS_JSON = os.path.join(MODEL_DIR, "gbt_metrics.json")

# Keep the local lake bounded.  The Spark sink removes the oldest batch
# directories when this limit is exceeded.
LAKE_MAX_GB = float(os.getenv("LAKE_MAX_GB", "1.0"))
EXPORT_MAX_GB = float(os.getenv("EXPORT_MAX_GB", "1.0"))
EXPORT_MAX_ROWS = int(os.getenv("EXPORT_MAX_ROWS", "100000"))

# ============================================================================
# STREAMING PARAMETERS
# ============================================================================

# Micro-batch window sizes
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "30 seconds")
SLIDE_DURATION = os.getenv("SLIDE_DURATION", "10 seconds")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "1 minute")
WEATHER_WINDOW_DURATION = os.getenv("WEATHER_WINDOW_DURATION", "5 minutes")

# Batch configuration
TRIGGER_PROCESSING_TIME = os.getenv("TRIGGER_PROCESSING_TIME", "10 seconds")

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
    "gps_interval": 5,
    "sensor_interval": 10,
    "weather_interval": 300,
    "probe_vehicles_per_segment": 3,
    "simulation_speed": 1.0,
}

# The TomTom endpoint is an aggregate speed observation.  These constants make
# the derived sensor contract explicit instead of hiding calibration values in
# the producer implementation.
OCCUPANCY_SPEED_FACTOR = float(os.getenv("OCCUPANCY_SPEED_FACTOR", "120.0"))
BASE_VEHICLES_PER_LANE = float(os.getenv("BASE_VEHICLES_PER_LANE", "3.0"))
MAX_ADDITIONAL_VEHICLES_PER_LANE = float(
    os.getenv("MAX_ADDITIONAL_VEHICLES_PER_LANE", "9.0")
)
TRAFFIC_SOURCE_TTL_SECONDS = max(
    30, int(os.getenv("TRAFFIC_SOURCE_TTL_SECONDS", "1800"))
)
WEATHER_SOURCE_TTL_SECONDS = max(
    60, int(os.getenv("WEATHER_SOURCE_TTL_SECONDS", "1800"))
)
MIN_SOURCE_CONFIDENCE = float(os.getenv("MIN_SOURCE_CONFIDENCE", "0.5"))
# A TomTom roadClosure flag is only honoured when the observed speed shows
# effectively no movement.  Above this speed the flag is treated as a false
# positive (TomTom sometimes flags fragments that still carry normal flow).
CLOSURE_MAX_SPEED_KMPH = float(os.getenv("CLOSURE_MAX_SPEED_KMPH", "5.0"))
MODEL_REFRESH_SECONDS = max(30, int(os.getenv("MODEL_REFRESH_SECONDS", "300")))
FORECAST_PATH = os.path.join(MODEL_DIR, "speed_forecast")
FORECAST_HORIZON_MINUTES = max(
    1, int(os.getenv("FORECAST_HORIZON_MINUTES", "15"))
)
FORECAST_SOURCE = os.getenv("FORECAST_SOURCE", "persistence_baseline")
FORECAST_MODEL_ID = os.getenv("FORECAST_MODEL_ID", "persistence-v1")

# TomTom's flow-segment endpoint is queried once for each configured road
# segment. The default is deliberately conservative for a 2,500-request/day
# allowance: 19 segments every 15 minutes is about 1,824 requests/day.
TOMTOM_REQUEST_BUDGET_DAY = max(
    1, int(os.getenv("TOMTOM_REQUEST_BUDGET_DAY", "2500"))
)
_DEFAULT_TOMTOM_INTERVAL = max(
    900, int((NUM_ROADS * 86400) / TOMTOM_REQUEST_BUDGET_DAY) + 60
)
TOMTOM_POLL_INTERVAL_SECONDS = max(
    60, int(os.getenv("TOMTOM_POLL_INTERVAL_SECONDS", str(_DEFAULT_TOMTOM_INTERVAL)))
)
TRAFFIC_EMIT_INTERVAL_SECONDS = max(
    5, int(os.getenv("TRAFFIC_EMIT_INTERVAL_SECONDS", "10"))
)

# Mumbai weather point used by Open-Meteo.  Open-Meteo needs no API key for
# non-commercial use under its published daily request limits.
OPEN_METEO_URL = os.getenv(
    "OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast"
)
OPEN_METEO_LATITUDE = float(os.getenv("OPEN_METEO_LATITUDE", "19.0760"))
OPEN_METEO_LONGITUDE = float(os.getenv("OPEN_METEO_LONGITUDE", "72.8777"))
OPEN_METEO_TIMEZONE = os.getenv("OPEN_METEO_TIMEZONE", "UTC")

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
    "speed_ratio",
    "occupancy_pct",
    "veh_per_lane",
    "rain_mm_hr",
    "visibility_km",
]
# Backwards-compatible name used by the first forecasting wrapper.
FEATURES = FEATURE_COLUMNS

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
    "map_center": [19.12, 72.87],  # Mumbai
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
        RAW_DIR,
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
            "weather_window": WEATHER_WINDOW_DURATION,
            "trigger": TRIGGER_PROCESSING_TIME,
        },
        "network": {
            "junctions": NUM_JUNCTIONS,
            "roads": NUM_ROADS,
        },
        "simulator": SIMULATOR_CONFIG,
        "sources": {
            "tomtom_poll_seconds": TOMTOM_POLL_INTERVAL_SECONDS,
            "tomtom_daily_budget": TOMTOM_REQUEST_BUDGET_DAY,
            "traffic_emit_seconds": TRAFFIC_EMIT_INTERVAL_SECONDS,
            "open_meteo_poll_seconds": int(
                os.getenv("OPEN_METEO_POLL_INTERVAL_SECONDS", "300")
            ),
        },
        "lake": {"path": CONGESTION_LAKE, "max_gb": LAKE_MAX_GB},
        "features": FEATURE_COLUMNS,
        "model": {
            "path": MODEL_PATH,
            "gbt_path": GBT_MODEL_PATH,
            "refresh_seconds": MODEL_REFRESH_SECONDS,
            "forecast_horizon_minutes": FORECAST_HORIZON_MINUTES,
        },
        "ml": ML_CONFIG,
        "routing": ROUTING_CONFIG,
    }


if __name__ == "__main__":
    import json
    print(json.dumps(get_config_summary(), indent=2))


# ============================================================================
# BASEMAPS / TILES CONFIGURATION
# ============================================================================

# Basemaps API Key (MapTiler, Mapbox, etc.)
BASEMAPS_API_KEY = os.getenv("BASEMAPS_API_KEY", "")

# Tile URL templates
# MapTiler with API key (when BASEMAPS_API_KEY is set)
OSM_TILE_URL = os.getenv(
    "OSM_TILE_URL",
    f"https://api.maptiler.com/maps/streets/{{z}}/{{x}}/{{y}}.png?key={BASEMAPS_API_KEY}" 
    if BASEMAPS_API_KEY else "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
)

# Traffic tile proxy URLs
TRAFFIC_TILE_INTERNAL_URL = os.getenv(
    "TRAFFIC_TILE_INTERNAL_URL", 
    "http://tiles:8502"
)
TRAFFIC_TILE_PUBLIC_URL = os.getenv(
    "TRAFFIC_TILE_PUBLIC_URL",
    "http://localhost:8502"
)

# Available MapTiler styles (when using MapTiler):
# - streets: Standard street map
# - satellite: Satellite imagery
# - hybrid: Satellite with labels
# - basic: Minimalist style
# - outdoor: Hiking/outdoor style
# - winter: Winter theme
# - topo: Topographic map
MAPTILER_STYLE = os.getenv("MAPTILER_STYLE", "streets")
