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
- **19 Road Segments**: Covering ~25 km of Mumbai's busiest corridors
- **Real GPS Coordinates**: Actual Mumbai locations for accurate simulation

The system consists of:
- **Kafka**: Message broker for real-time data streams
- **Simulator**: Generates GPS, sensor, and weather data
- **Spark Streaming**: Processes real-time data and generates predictions
- **Machine Learning**: Traffic prediction models (RandomForest, GBT)
- **Dashboard**: Live visualization of traffic conditions
- **R Analytics**: Statistical analysis and reporting

## Quick Start

```bash
# Build and start all services
docker-compose up --build

# View logs
docker-compose logs -f

# Stop all services
docker-compose down
```

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
│   │   └── producer.py
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
