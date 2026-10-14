#!/usr/bin/env python3
"""Live Data Viewer - Monitor real-time data sources for MapYourWay.

This script shows live data from all three sources:
1. TomTom Traffic Flow API (GPS + Sensor events)
2. Open-Meteo Weather API
3. Kafka Topics (if available)

Usage:
    python scripts/view_live_data.py [--source all|tomtom|weather|kafka]
    python scripts/view_live_data.py --source tomtom --limit 5
    python scripts/view_live_data.py --source kafka --topic mapyourway.gps_events
"""
import argparse
import json
import sys
import time
from datetime import datetime
from typing import Optional

import requests

# Add parent directory to path for imports
sys.path.insert(0, "src")

from mapyourway import config
from mapyourway.simulator import real_producer


def print_separator(title: str = ""):
    """Print a visual separator with optional title."""
    if title:
        print(f"\n{'=' * 80}")
        print(f"  {title}")
        print('=' * 80)
    else:
        print('-' * 80)


def view_tomtom_data(limit: int = 10, verbose: bool = False):
    """Fetch and display live TomTom traffic flow data."""
    print_separator("🚗 TomTom Traffic Flow API Data")
    
    # Import SEGMENTS from network module
    from mapyourway.network import SEGMENTS, NODES
    
    # Get API key using the proper function
    try:
        api_key = real_producer.get_tomtom_api_key()
        print(f"API Key: {api_key[:15]}..." if api_key else "❌ NO API KEY")
    except ValueError as e:
        print(f"❌ {e}")
        return
    
    # Try to load ML model for predictions
    ml_model = None
    try:
        from pyspark.ml import PipelineModel
        from pyspark.sql import SparkSession
        import os
        
        model_path = "data/models/congestion_model"
        if os.path.exists(model_path):
            # Initialize Spark (minimal config for local prediction)
            spark = SparkSession.builder \
                .appName("LiveDataViewer") \
                .master("local[1]") \
                .config("spark.sql.shuffle.partitions", "1") \
                .getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")
            
            ml_model = PipelineModel.load(model_path)
            print(f"✅ ML Model loaded from {model_path}")
        else:
            print(f"⚠️  ML Model not found at {model_path} (predictions disabled)")
    except Exception as e:
        print(f"⚠️  Could not load ML model: {e}")
        ml_model = None
    
    print(f"Segments loaded: {len(SEGMENTS)}")
    print(f"Fetching data at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print_separator()
    
    try:
        session = requests.Session()
        fleet = real_producer.ProbeFleet(vehicles_per_segment=3)
        now_ms = int(time.time() * 1000)
        
        # Fetch data for limited segments
        segments_to_fetch = SEGMENTS[:limit]
        
        print(f"\n📡 Fetching from TomTom API for {len(segments_to_fetch)} segments...")
        
        for idx, segment in enumerate(segments_to_fetch, 1):
            lat, lon = real_producer._segment_road_sample(segment)
            
            try:
                response = session.get(
                    "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json",
                    params={"key": api_key, "point": f"{lat},{lon}"},
                    timeout=10,
                )
                
                if response.status_code == 200:
                    data = response.json()
                    flow_data = data.get('flowSegmentData', {})
                    
                    # Calculate derived metrics
                    current_speed = flow_data.get('currentSpeed', 0)
                    free_flow_speed = flow_data.get('freeFlowSpeed', 1)
                    current_travel_time = flow_data.get('currentTravelTime', 0)
                    free_flow_travel_time = flow_data.get('freeFlowTravelTime', 1)
                    confidence = flow_data.get('confidence', 0)
                    
                    # Core derived metrics
                    speed_ratio = current_speed / max(free_flow_speed, 1.0)
                    congestion_score = 100 * (1 - speed_ratio)
                    delay_seconds = current_travel_time - free_flow_travel_time
                    delay_minutes = delay_seconds / 60.0
                    speed_loss = free_flow_speed - current_speed
                    efficiency_pct = speed_ratio * 100
                    
                    # Traffic level classification
                    if speed_ratio >= 0.8:
                        level = "🟢 FREE FLOW"
                        level_numeric = 0
                    elif speed_ratio >= 0.5:
                        level = "🟡 MODERATE"
                        level_numeric = 1
                    elif speed_ratio >= 0.3:
                        level = "🟠 HEAVY"
                        level_numeric = 2
                    else:
                        level = "🔴 SEVERE"
                        level_numeric = 3
                    
                    # Estimated occupancy from speed
                    if speed_ratio >= 0.95:
                        occupancy_pct = 5.0
                    elif speed_ratio >= 0.8:
                        occupancy_pct = 20.0
                    elif speed_ratio >= 0.5:
                        occupancy_pct = 45.0
                    elif speed_ratio >= 0.3:
                        occupancy_pct = 70.0
                    else:
                        occupancy_pct = 90.0
                    
                    # Confidence quality
                    if confidence > 0.9:
                        conf_quality = "Excellent ✅"
                    elif confidence > 0.7:
                        conf_quality = "Good 👍"
                    elif confidence > 0.5:
                        conf_quality = "Fair ⚠️"
                    else:
                        conf_quality = "Poor ❌"
                    
                    # Get place names from segment
                    start_node = segment.get('node_a', 'Unknown')
                    end_node = segment.get('node_b', 'Unknown')
                    segment_name = segment.get('name', f"{start_node}-{end_node}")
                    start_coords = NODES.get(start_node, (0, 0))
                    end_coords = NODES.get(end_node, (0, 0))
                    
                    # Reverse geocode to get area names (optional, uses Nominatim)
                    def get_area_name(lat, lon):
                        try:
                            resp = session.get(
                                "https://nominatim.openstreetmap.org/reverse",
                                params={
                                    "lat": lat,
                                    "lon": lon,
                                    "format": "json",
                                    "addressdetails": 1
                                },
                                headers={"User-Agent": "MapYourWay/1.0"},
                                timeout=5
                            )
                            if resp.status_code == 200:
                                addr = resp.json().get('address', {})
                                # Get most specific available
                                return (addr.get('neighbourhood') or 
                                       addr.get('suburb') or 
                                       addr.get('city_district') or
                                       addr.get('locality') or
                                       'Mumbai')
                            return None
                        except:
                            return None
                    
                    print(f"\n{'='*70}")
                    print(f"📍 Segment {idx}: {segment['segment_id']}")
                    print(f"   🗺️  Route: {start_node} → {end_node}")
                    
                    # Try to get area names (may be slow, so optional)
                    if not verbose:  # Skip geocoding unless verbose
                        print(f"   📌 From: {start_node} ({start_coords[0]:.4f}, {start_coords[1]:.4f})")
                        print(f"   📌 To:   {end_node} ({end_coords[0]:.4f}, {end_coords[1]:.4f})")
                    else:
                        start_area = get_area_name(start_coords[0], start_coords[1])
                        end_area = get_area_name(end_coords[0], end_coords[1])
                        if start_area:
                            print(f"   📌 From: {start_node} ({start_area})")
                        if end_area:
                            print(f"   📌 To:   {end_node} ({end_area})")
                    
                    print(f"{'='*70}")
                    print(f"\n   📊 RAW DATA (from TomTom sensors):")
                    print(f"      Current Speed:        {current_speed} km/h")
                    print(f"      Free Flow Speed:      {free_flow_speed} km/h")
                    print(f"      Current Travel Time:  {current_travel_time}s ({current_travel_time//60}:{current_travel_time%60:02d})")
                    print(f"      Free Flow Time:       {free_flow_travel_time}s ({free_flow_travel_time//60}:{free_flow_travel_time%60:02d})")
                    print(f"      Confidence:           {confidence:.3f} ({conf_quality})")
                    print(f"      Road Closure:         {flow_data.get('roadClosure', False)}")
                    
                    print(f"\n   🔬 DERIVED METRICS (computed by app):")
                    print(f"      Speed Ratio:          {speed_ratio:.3f} ({efficiency_pct:.1f}% efficiency)")
                    print(f"      Speed Loss:           -{speed_loss} km/h slower than normal")
                    print(f"      Delay:                +{delay_seconds}s ({delay_minutes:.1f} min extra)")
                    print(f"      Congestion Score:     {congestion_score:.1f} / 100")
                    print(f"      Traffic Level:        {level}")
                    print(f"      Estimated Occupancy:  {occupancy_pct:.0f}%")
                    
                    # ML Model Prediction
                    if ml_model and spark:
                        try:
                            # Create features DataFrame for prediction
                            from pyspark.sql.types import StructType, StructField, DoubleType
                            
                            schema = StructType([
                                StructField("speed_ratio", DoubleType(), False),
                                StructField("occupancy_pct", DoubleType(), False),
                                StructField("veh_per_lane", DoubleType(), False),
                                StructField("rain_mm_hr", DoubleType(), False),
                                StructField("visibility_km", DoubleType(), False),
                            ])
                            
                            # Prepare features (need weather data)
                            features_data = [(
                                float(speed_ratio),
                                float(occupancy_pct),
                                2.0,  # Estimated vehicles per lane
                                0.0,  # Would need actual weather (assume clear)
                                10.0  # Assume good visibility
                            )]
                            
                            features_df = spark.createDataFrame(features_data, schema)
                            predictions = ml_model.transform(features_df)
                            
                            # Get prediction
                            result = predictions.select("prediction", "probability").collect()[0]
                            pred_level = int(result.prediction)
                            probabilities = result.probability.toArray()
                            
                            level_names = ["🟢 FREE FLOW", "🟡 MODERATE", "🟠 HEAVY", "🔴 SEVERE"]
                            pred_level_name = level_names[pred_level]
                            
                            print(f"\n   🤖 ML MODEL PREDICTION:")
                            print(f"      Predicted Level:      {pred_level_name} ({pred_level})")
                            print(f"      Confidence:           {probabilities[pred_level]*100:.1f}%")
                            print(f"      Probabilities:")
                            for i, prob in enumerate(probabilities):
                                print(f"         {level_names[i]}: {prob*100:.1f}%")
                            
                            # Compare with actual
                            if pred_level == level_numeric:
                                print(f"      ✅ Model matches actual traffic level!")
                            else:
                                print(f"      ⚠️  Model predicted {pred_level_name}, actual is {level}")
                                print(f"          Difference: {abs(pred_level - level_numeric)} level(s)")
                        
                        except Exception as e:
                            print(f"\n   ⚠️  ML prediction failed: {e}")
                    
                    print(f"\n   📍 LOCATION:")
                    coords = flow_data.get('coordinates', {}).get('coordinate', [])
                    if coords and len(coords) >= 2:
                        print(f"      Start: ({coords[0].get('latitude', 0):.4f}, {coords[0].get('longitude', 0):.4f})")
                        print(f"      End:   ({coords[-1].get('latitude', 0):.4f}, {coords[-1].get('longitude', 0):.4f})")
                    print(f"      Segment Length:       {segment.get('length_km', 'N/A')} km")
                    print(f"      Lanes:                {segment.get('lanes', 'N/A')}")
                    print(f"{'='*70}")
                    
                    if verbose:
                        print(f"\n   📄 FULL API RESPONSE:")
                        print(json.dumps(data, indent=2))
                        print(f"{'='*70}")
                else:
                    print(f"\n📍 Segment {idx}: {segment['segment_id']}")
                    print(f"   ❌ Error: HTTP {response.status_code}")
                    
            except Exception as e:
                print(f"\n📍 Segment {idx}: {segment['segment_id']}")
                print(f"   ❌ Error: {e}")
        
        print(f"\n✅ Fetch complete!")
        
    except Exception as e:
        print(f"\n❌ Error fetching TomTom data: {e}")
        import traceback
        traceback.print_exc()


def view_weather_data(verbose: bool = False):
    """Fetch and display live weather data from Open-Meteo."""
    print_separator("🌤️  Open-Meteo Weather API Data")
    
    print(f"Location: Mumbai ({config.OPEN_METEO_LATITUDE}, {config.OPEN_METEO_LONGITUDE})")
    print(f"API URL: {config.OPEN_METEO_URL}")
    print(f"Fetching data at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print_separator()
    
    try:
        session = requests.Session()
        weather = real_producer.fetch_open_meteo_weather(session=session)
        
        print(f"\n✅ Weather Data Received:")
        print(f"   📍 Station: {weather.get('station_id', 'N/A')}")
        print(f"   🌡️  Temperature: {weather['temp_c']}°C")
        print(f"   💧 Humidity: {weather['humidity_pct']}%")
        print(f"   🌧️  Rain: {weather['rain_mm_hr']} mm/hr")
        print(f"   👁️  Visibility: {weather['visibility_km']} km")
        print(f"   💨 Wind Speed: {weather.get('wind_speed_kmph', weather.get('wind_kmph', 0.0))} km/h")
        print(f"   🔢 Weather Code: {weather['weather_code']}")
        print(f"   ⏰ Observed: {datetime.fromtimestamp(weather['observed_at_ms']/1000).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   ⏰ Fetched: {datetime.fromtimestamp(weather['ts_ms']/1000).strftime('%Y-%m-%d %H:%M:%S')}")
        
        if verbose:
            print(f"\n   Full JSON:\n{json.dumps(weather, indent=2)}")
            
        # Interpret weather code
        weather_codes = {
            0: "Clear sky",
            1: "Mainly clear",
            2: "Partly cloudy",
            3: "Overcast",
            45: "Fog",
            48: "Depositing rime fog",
            51: "Light drizzle",
            53: "Moderate drizzle",
            55: "Dense drizzle",
            61: "Slight rain",
            63: "Moderate rain",
            65: "Heavy rain",
            71: "Slight snow",
            73: "Moderate snow",
            75: "Heavy snow",
            95: "Thunderstorm",
        }
        print(f"   🌈 Condition: {weather_codes.get(weather['weather_code'], 'Unknown')}")
        
    except Exception as e:
        print(f"\n❌ Error fetching weather data: {e}")
        import traceback
        traceback.print_exc()


def view_kafka_topic(topic: str, limit: int = 10, timeout: int = 5):
    """View live messages from a Kafka topic."""
    print_separator(f"📨 Kafka Topic: {topic}")
    
    try:
        from kafka import KafkaConsumer
        import json
        
        print(f"Bootstrap servers: {config.KAFKA_BOOTSTRAP_SERVERS}")
        print(f"Connecting to Kafka...")
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',  # Only read new messages
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=timeout * 1000,
        )
        
        print(f"✅ Connected! Waiting for messages (timeout: {timeout}s)...")
        print_separator()
        
        message_count = 0
        for message in consumer:
            message_count += 1
            print(f"\n📬 Message {message_count}:")
            print(f"   Partition: {message.partition}")
            print(f"   Offset: {message.offset}")
            print(f"   Timestamp: {datetime.fromtimestamp(message.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Value: {json.dumps(message.value, indent=2)}")
            print_separator()
            
            if message_count >= limit:
                break
        
        if message_count == 0:
            print("\n⚠️  No messages received within timeout period.")
            print("   Make sure the producer is running: docker compose up simulator")
        
        consumer.close()
        
    except ImportError:
        print("\n❌ kafka-python not installed. Install with: pip install kafka-python")
    except Exception as e:
        print(f"\n❌ Error connecting to Kafka: {e}")
        print("\n💡 Make sure Kafka is running: docker compose up kafka")
        import traceback
        traceback.print_exc()


def continuous_monitoring(interval: int = 60):
    """Continuously monitor all data sources."""
    print_separator("🔄 Continuous Monitoring Mode")
    print(f"Refresh interval: {interval} seconds")
    print("Press Ctrl+C to stop")
    print_separator()
    
    iteration = 0
    try:
        while True:
            iteration += 1
            print(f"\n\n{'#' * 80}")
            print(f"  Iteration {iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print('#' * 80)
            
            # Fetch TomTom data
            view_tomtom_data(limit=3, verbose=False)
            
            # Fetch weather data
            view_weather_data(verbose=False)
            
            print(f"\n\n⏳ Waiting {interval} seconds until next refresh...")
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n\n✋ Monitoring stopped by user")


def main():
    parser = argparse.ArgumentParser(
        description="View live data sources for MapYourWay",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # View all sources
  python scripts/view_live_data.py

  # View only TomTom data (first 5 segments)
  python scripts/view_live_data.py --source tomtom --limit 5

  # View weather data with full JSON
  python scripts/view_live_data.py --source weather --verbose

  # Monitor Kafka topic
  python scripts/view_live_data.py --source kafka --topic mapyourway.gps_events

  # Continuous monitoring (every 60 seconds)
  python scripts/view_live_data.py --continuous --interval 60
        """
    )
    
    parser.add_argument(
        '--source',
        choices=['all', 'tomtom', 'weather', 'kafka'],
        default='all',
        help='Data source to view (default: all)'
    )
    parser.add_argument(
        '--topic',
        default='mapyourway.gps_events',
        help='Kafka topic to view (default: mapyourway.gps_events)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=10,
        help='Number of records to display (default: 10)'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=5,
        help='Kafka consumer timeout in seconds (default: 5)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show full JSON data'
    )
    parser.add_argument(
        '--continuous',
        action='store_true',
        help='Continuously monitor data sources'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=60,
        help='Interval between refreshes in continuous mode (default: 60s)'
    )
    
    args = parser.parse_args()
    
    print("\n" + "=" * 80)
    print("  MapYourWay - Live Data Viewer")
    print("=" * 80)
    
    if args.continuous:
        continuous_monitoring(args.interval)
        return
    
    if args.source in ['all', 'tomtom']:
        view_tomtom_data(limit=args.limit, verbose=args.verbose)
    
    if args.source in ['all', 'weather']:
        view_weather_data(verbose=args.verbose)
    
    if args.source == 'kafka':
        view_kafka_topic(args.topic, limit=args.limit, timeout=args.timeout)
    
    print("\n" + "=" * 80)
    print("  Done!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
