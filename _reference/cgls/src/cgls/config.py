"""Central configuration. Every value can be overridden with an environment variable
(see docker-compose.yml), so the same code runs in Docker, in tests and on a laptop."""
import os

# ---- Kafka -----------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_GPS = "gps_events"
TOPIC_SENSOR = "sensor_events"
TOPIC_WEATHER = "weather_events"
TOPIC_OUT = "congestion_updates"
TOPIC_ALERTS = "incident_alerts"
INPUT_TOPICS = [TOPIC_GPS, TOPIC_SENSOR, TOPIC_WEATHER]

# ---- Paths (inside the container; ./data on your laptop is mounted at /data) --
DATA_DIR = os.getenv("CGLS_DATA_DIR", "/data")
CHECKPOINT_DIR = os.getenv("CGLS_CHECKPOINT_DIR", "/checkpoints")
LAKE_DIR = os.path.join(DATA_DIR, "lake", "segment_congestion")      # Parquet "data lake"
MODEL_DIR = os.path.join(DATA_DIR, "models", "congestion_rf")
METRICS_PATH = os.path.join(DATA_DIR, "models", "metrics.json")
FORECAST_MODEL_DIR = os.path.join(DATA_DIR, "models", "forecaster_gbt")
FORECAST_METRICS_PATH = os.path.join(DATA_DIR, "models", "forecaster_metrics.json")
EXPORT_DIR = os.path.join(DATA_DIR, "export")                          # CSV for the R analytics
GROUND_TRUTH_DIR = os.path.join(DATA_DIR, "ground_truth")
REPORT_DIR = os.path.join(DATA_DIR, "reports")

# ---- Streaming parameters ---------------------------------------------------
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "30 seconds")   # how much history one estimate uses
SLIDE_DURATION = os.getenv("SLIDE_DURATION", "5 seconds")      # a new estimate every 5 s
WATERMARK = os.getenv("WATERMARK", "2 seconds")                # how long we wait for late events
TRIGGER_SECONDS = int(os.getenv("TRIGGER_SECONDS", "5"))       # micro-batch interval
FORECAST_HORIZON_S = int(os.getenv("FORECAST_HORIZON_S", "60"))
FORECAST_DAMPING = float(os.getenv("FORECAST_DAMPING", "0.3"))
FORECAST_DEADBAND = float(os.getenv("FORECAST_DEADBAND", "0.05"))

# ---- Model contract: feature names must match between training and streaming ---
FEATURES = ["speed_ratio", "occupancy_pct", "veh_per_lane", "rain_mm_hr", "visibility_km"]
