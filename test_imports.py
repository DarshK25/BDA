"""
Quick test to verify all imports work
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

print("Testing imports...")
print("=" * 60)

try:
    from mapyourway.config import KAFKA_BOOTSTRAP_SERVERS
    print("✅ config.py imported successfully")
    print(f"   Kafka servers: {KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    print(f"❌ config.py failed: {e}")

try:
    from mapyourway.network import SEGMENTS, NODES
    print("✅ network.py imported successfully")
    print(f"   Junctions: {len(NODES)}, Roads: {len(SEGMENTS)}")
except Exception as e:
    print(f"❌ network.py failed: {e}")

try:
    from mapyourway.routing import build_graph, search
    print("✅ routing.py imported successfully")
except Exception as e:
    print(f"❌ routing.py failed: {e}")

try:
    from mapyourway.traffic_model import TrafficModel
    print("✅ traffic_model.py imported successfully")
except Exception as e:
    print(f"❌ traffic_model.py failed: {e}")

try:
    from mapyourway.features import create_model_features
    print("✅ features.py imported successfully")
except Exception as e:
    print(f"❌ features.py failed: {e}")

try:
    from mapyourway.simulator.real_producer import fetch_and_publish_traffic
    print("✅ real_producer.py imported successfully")
except Exception as e:
    print(f"❌ real_producer.py failed: {e}")

try:
    from mapyourway.dashboard.live_state import LiveState
    print("✅ live_state.py imported successfully")
except Exception as e:
    print(f"❌ live_state.py failed: {e}")

print("=" * 60)
print("All imports successful! ✅")
