# MapYourWay - Mumbai Smart Traffic Analytics

## Real-Time Traffic Intelligence for Mumbai's Western & Central Lines

A comprehensive real-time data processing system specifically designed for Mumbai's transportation network. Analyzes GPS and sensor data from Western and Central railway lines to provide traffic congestion updates and route optimization for sustainable urban transportation.

## Problem Statement

Design and implement a real-time analytics pipeline for a Smart City Transportation System that processes live data from multiple sources such as GPS trackers, IoT sensors, and weather data to predict traffic congestion and suggest alternate routes dynamically.

## Sustainability Context

- **Reduces fuel consumption and air pollution** by minimizing idle traffic time
- **Supports SDG Goal 11** – Sustainable Cities & Communities
- **Enables energy-efficient** and data-driven public transportation management

## 🚂 Mumbai's Transportation Network

The system covers Mumbai's major arterial routes:
- **Western Line**: Borivali → Malad → Goregaon → Andheri → Santacruz → Bandra → Mahim → Dadar → Worli
- **Central Line**: Ghatkopar → Kurla → Sion → Dadar
- **Cross-connections**: Powai, multiple inter-line connections

### Road Network:
- **13 Major Junctions**: From Borivali (North) to Worli (South)
- **19 Road Segments**: Covering approximately 97 km of modeled road links across Mumbai's busiest corridors
- **Real GPS Coordinates**: Actual Mumbai locations for accurate simulation

The system consists of:
- **Kafka**: Message broker for real-time data streams
- **TomTom Traffic API**: Live traffic data source (replacing synthetic simulator)
- **Open-Meteo**: Live Mumbai weather data without an API key
- **Spark Streaming**: Processes real-time data and generates predictions
   - **Machine Learning**: a compatible RandomForest startup model and comparison GBT model are trained from explicit calibration scenarios; independent labelled data is still required for a production accuracy claim
- **Dashboard**: Live visualization of traffic conditions on a complete Mumbai
  OpenStreetMap road basemap, with optional TomTom flow tiles and Spark/Kafka
  corridor predictions overlaid
- **R Analytics**: Statistical analysis and reporting

## Quick Start

Create or edit `.env` in the project root. The existing variable name is valid
for this project:

```dotenv
TOM_TOM_API_KEY=your_tomtom_key_here
```

The key is ignored by Git and is passed only to the producer container. A
TomTom Traffic API key provides traffic-flow data. Weather comes separately
from Open-Meteo and requires no key.

Open-Meteo is used under its free non-commercial request limits. Its data is
published under CC BY 4.0, so reports should credit Open-Meteo and the
underlying weather-model providers. The configured five-minute weather poll
uses only a small fraction of the documented daily limit.

Optional local settings:

```dotenv
# Safe default for 19 segments under a 2,500-request/day allowance
TOMTOM_POLL_INTERVAL_SECONDS=900
# Keep the Kafka stream active between TomTom refreshes
TRAFFIC_EMIT_INTERVAL_SECONDS=10
OPEN_METEO_POLL_INTERVAL_SECONDS=300
LAKE_MAX_GB=1.0
```

```bash
# Build and start all services
docker compose up --build

# View logs
docker compose logs -f

# Stop all services
docker compose down
```

The first run should start `kafka`, `kafka-init`, `model-init`, `simulator`,
`spark`, `tiles`, and `dashboard`. `kafka-init` and `model-init` exit successfully after
creating topics and training the startup models. Open
the dashboard at <http://localhost:8501> and the Spark UI at
<http://localhost:4040>.

The map always requests an explicit OpenStreetMap raster layer, in addition to
Plotly's built-in map style. This is deliberate: it keeps Mumbai streets,
highways, coastline, neighbourhoods, and labels visible when a browser cannot
load the built-in MapLibre style metadata. In `Live road traffic` mode, the
server-side TomTom tile proxy is added above that basemap when its API key and
upstream are healthy. In `Corridor estimates` mode, the basemap remains full
city coverage while the 19 monitored corridors show the live Spark prediction
colours.

## Current implementation status

The current `mapyourway` implementation is the working application. The
`_reference/cgls` directory is retained as design material only; it is not a
runtime package and is not imported by the application.

| Area | Current state |
|---|---|
| Ingestion | `real_producer.py` calls TomTom Flow Segment Data for the midpoint of each of the 19 network segments. It publishes a small, explicitly labelled derived probe fleet and derived sensor observation from each aggregate segment response. |
| Kafka | Topics are defined in `src/mapyourway/config.py` and created by Compose with the same names. |
| Spark | Structured Streaming parses the three topics, aggregates 30-second event-time windows sliding every 10 seconds, joins network metadata, and publishes congestion predictions. |
| Prediction | The live stream loads the RandomForest startup model and falls back to the transparent speed-ratio rule if the model is unavailable or incompatible. The trainer and live stream share `speed_ratio`, `occupancy_pct`, `veh_per_lane`, `rain_mm_hr`, and `visibility_km`. A persistence speed forecast is exposed for routing and is clearly labelled until future-labelled data is available. |
| Dashboard | Reads `mapyourway.congestion_predictions` and uses the actual Spark field names (`current_speed_kmph`, `free_flow_kmph`). |
| Routing | A* and Dijkstra consume fresh, confident Spark predictions in the dashboard. Origin/destination controls, usual-vs-best comparison, alternatives, closures, forecast mode, and map overlays are available. |
| Weather | Open-Meteo current conditions are published to `mapyourway.weather_events` every five minutes by default. Rain, visibility, temperature, humidity, wind, and WMO weather code are included. |
| Data lake | Every Spark output micro-batch is written to `/data/lake/segment_congestion/dt=YYYY-MM-DD/batch_id=...` as Parquet and appended to `/data/export/congestion.csv`. The lake is bounded by `LAKE_MAX_GB`. |

### What “GPS and IoT sensors” means with free sources

TomTom Flow Segment Data is an aggregate observation for the road fragment
nearest a coordinate. It does not expose private-vehicle identities or raw
GPS traces. There is no guaranteed free, public, live individual-vehicle GPS
feed for this Mumbai corridor that can be substituted honestly.

The project therefore uses a transparent hybrid:

1. TomTom supplies the real segment speed, travel time, confidence, and closure
   status.
2. MapYourWay creates three stable **derived probe IDs per segment** whose
   positions move along the configured segment at that observed speed.
3. MapYourWay derives occupancy and vehicle-count estimates from the same
   observed speed and segment lane count.
4. Open-Meteo supplies real current weather conditions.

The derived events are useful for exercising a complete GPS/sensor/Kafka/Spark
data layer, but they must be described as derived observations in a report.
Replacing them with an actual municipal, fleet, or roadside-sensor feed later
only requires changing the producer; the Kafka schemas remain the same.

## Recommended first development session

1. Start Docker Desktop, verify the `.env` key, and run `docker compose up --build`.
2. Check the producer log for HTTP status and the estimated TomTom request rate:
   `docker compose logs -f simulator`.
3. Check the Spark log for window output:
   `docker compose logs -f spark`.
4. Inspect one input and one output message:

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server kafka:9092 \
      --topic mapyourway.gps_events --max-messages 1

   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server kafka:9092 \
      --topic mapyourway.congestion_predictions --max-messages 1
   ```

5. Only after this end-to-end path works, collect a historical dataset with
   independent labels and define the shared training schema. The live feature schema is now fixed:
   `speed_ratio`, `occupancy_pct`, `veh_per_lane`, `rain_mm_hr`, and
   `visibility_km`.

Inspect the Parquet lake after Spark has emitted a batch:

```bash
docker compose run --rm --no-deps tools python -c "import pandas as pd; df=pd.read_parquet('/data/lake/segment_congestion'); print(df.tail())"
```

The producer makes one TomTom request per segment per traffic refresh. Between
refreshes it emits fresh timestamps and derived probe/sensor observations from
the last TomTom snapshot, so Spark continues to receive events every few
seconds without exceeding the traffic API quota. The producer prints the
estimated request rate and both intervals at startup.

### Model training

`model-init` automatically creates `/data/models/congestion_model` (the live
RandomForest) and `/data/models/congestion_gbt_model` (a multiclass
One-vs-Rest GBT comparison artifact). These startup models use balanced,
physically calibrated speed-ratio scenarios so a fresh installation is
runnable; their metrics are calibration metrics, not independent validation.

After collecting independently labelled historical data, train a production
candidate with:

```bash
docker compose run --rm --no-deps tools \
  python -m mapyourway.ml.train /data/lake/segment_congestion \
  /data/models/congestion_model --algorithm random_forest
```

Use `--allow-derived-labels` only when intentionally creating a bootstrap
model from rule-derived labels. The GBT comparison model can be trained with
`--algorithm gbt --metrics-path /data/models/gbt_metrics.json`.

## Project Structure

```
.
├── docker-compose.yml        # All containers and network configuration
├── Dockerfile                # Python + Java + Spark image
├── requirements.txt          # Python dependencies
├── pytest.ini               # Test configuration
├── conf/
│   └── log4j2.properties    # Spark logging configuration
├── src/mapyourway/          # Main application package
│   ├── config.py            # Configuration and constants
│   ├── network.py           # Road network topology (13 junctions, 19 roads)
│   ├── traffic_model.py     # Traffic physics simulation
│   ├── features.py          # Feature engineering
│   ├── routing.py           # A* / Dijkstra routing algorithms
│   ├── simulator/           # Data generation
│   │   └── real_producer.py
│   ├── ml/                  # Machine learning pipeline
│   │   ├── train.py
│   │   ├── forecaster.py
│   │   └── evaluate.py
│   ├── streaming/           # Spark streaming job
│   │   └── job.py
│   └── dashboard/           # Real-time dashboard
│       ├── live_state.py
│       └── app.py
├── r/                       # R analytics and Shiny app
│   ├── Dockerfile
│   ├── common.R
│   ├── analysis.R
│   └── app.R
├── scripts/                 # Demo and utility scripts
│   ├── demo_astar.py
│   ├── demo_predict.py
│   └── show_lake.py
├── tests/                   # Pytest test suite
├── docs/                    # Documentation and reports
└── data/                    # Generated at runtime
    ├── models/              # Trained models
    ├── lake/                # Parquet data lake
    ├── export/              # CSV exports
    ├── ground_truth/        # Simulator ground truth
    └── reports/             # Analysis reports
```

## Features

### Real-Time Processing
- Processes GPS, sensor, and weather data every few seconds
- Micro-batch streaming with Spark Structured Streaming
- Kafka-based message queue for reliable data ingestion

### Traffic Prediction
- Machine learning models (RandomForest, Gradient Boosted Trees)
- Predicts congestion levels based on historical patterns
- Dynamic route optimization using A* algorithm

### Visualization
- Live dashboard showing current traffic conditions
- R-based statistical analysis and reporting
- Parquet-based data lake for historical analysis

## Technologies

- **Apache Spark 3.x** - Stream processing and ML
- **Apache Kafka** - Message broker
- **Python 3.9+** - Application logic
- **R & Shiny** - Statistical analysis
- **Docker & Docker Compose** - Containerization
- **PySpark MLlib** - Machine learning

## License

MIT License
