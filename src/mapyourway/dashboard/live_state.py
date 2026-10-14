"""
Live State Manager - Thread-safe store for latest traffic data
Runs a background Kafka consumer that continuously updates the state
"""

import json
import math
import uuid
import threading
import time
from collections import deque
from typing import Dict, Optional
import logging

from mapyourway import config
from mapyourway.contracts import timestamp_ms
from mapyourway.network import SEGMENT_BY_ID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LiveState:
    """
    Thread-safe container for the latest traffic state.
    
    Architecture:
        Background Thread → Kafka Consumer → updates self.data
        Main Thread (Streamlit) → reads self.data
        Lock prevents simultaneous read/write
    """
    
    def __init__(self):
        self.lock = threading.Lock()
        self.data: Dict[str, dict] = {}  # {segment_id: latest_message}
        self.history: deque = deque(maxlen=config.DASHBOARD_CONFIG["max_history_points"])
        self.message_count = 0
        self.last_update = None
        self.error: Optional[str] = None
    
    def update(self, segment_id: str, message: dict) -> bool:
        """Accept a valid newer window or a higher-batch revision of that window."""
        if not self._valid_message(segment_id, message):
            return False
        with self.lock:
            previous = self.data.get(segment_id)
            if previous is not None and self._message_order(message) <= self._message_order(previous):
                return False
            self.data[segment_id] = dict(message)
            self.message_count += 1
            self.last_update = time.time()
            self.history.append({
                "timestamp": message["timestamp"],
                "segment_id": segment_id,
                "speed": message.get("current_speed_kmph", message.get("current_speed")),
                "congestion": self._estimate_congestion(message),
            })
            return True
    
    def get_snapshot(self) -> tuple:
        """
        Get current state snapshot.
        Called by Streamlit main thread.
        
        Returns:
            (data_dict, history_list, message_count, last_update, error)
        """
        with self.lock:
            return (
                {sid: dict(message) for sid, message in self.data.items()},
                list(self.history),
                self.message_count,
                self.last_update,
                self.error
            )
    
    def set_error(self, error: str) -> None:
        """Record error from consumer thread."""
        with self.lock:
            self.error = error
            logger.error(f"LiveState error: {error}")
    
    def clear_error(self) -> None:
        """Clear error state."""
        with self.lock:
            self.error = None
    
    @staticmethod
    def _message_order(message: dict) -> tuple[int, int]:
        """Window event time first; batch_id permits legitimate window revisions."""
        return timestamp_ms(message["timestamp"]), int(message.get("batch_id", -1))

    @staticmethod
    def _valid_message(segment_id: str, message: dict) -> bool:
        if (segment_id not in SEGMENT_BY_ID or not isinstance(message, dict)
                or message.get("segment_id", segment_id) != segment_id
                or timestamp_ms(message.get("timestamp")) <= 0):
            return False
        try:
            batch = message.get("batch_id", -1)
            if isinstance(batch, bool) or int(batch) != float(batch) or int(batch) < -1:
                return False
            for field in ("source_observed_at_ms", "observed_at_ms", "ts_ms"):
                if field in message and message[field] is not None and timestamp_ms(message[field]) <= 0:
                    return False
            if message.get("road_closure") is not True:
                speed = float(message.get("current_speed_kmph", message.get("current_speed")))
                if not math.isfinite(speed) or not 0 <= speed <= 150:
                    return False
            if message.get("congestion_level") is not None:
                level = int(message["congestion_level"])
                if level != float(message["congestion_level"]) or not 0 <= level <= 3:
                    return False
            return True
        except (ValueError, TypeError, OverflowError):
            return False

    @staticmethod
    def _estimate_congestion(message: dict) -> float:
        """
        Estimate congestion level from message data.
        
        Returns:
            0.0 (free) to 1.0 (jammed)
        """
        # The Spark output uses *_kmph names; the aliases keep the dashboard
        # compatible with older raw traffic messages during development.
        if message.get("congestion_level") is not None:
            return float(message["congestion_level"]) / 3.0
        current = message.get("current_speed_kmph", message.get("current_speed"))
        free_flow = message.get("free_flow_kmph", message.get("free_flow_speed"))
        try:
            if current is None or free_flow is None or not math.isfinite(float(free_flow)) or float(free_flow) <= 0:
                return 0.5
            return max(0.0, min(1.0, 1.0 - float(current) / float(free_flow)))
        except (TypeError, ValueError):
            return 0.5


class KafkaConsumerThread:
    """
    Background thread that consumes Kafka messages.
    Runs forever, updating LiveState with each message.
    """
    
    def __init__(self, state: LiveState, bootstrap_servers: str, topic: str = config.TOPIC_CONGESTION):
        self.state = state
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = f"mapyourway-dashboard-{uuid.uuid4().hex}"
        self.running = False
        self.thread = None
        self._consumer = None
    
    def start(self) -> None:
        """Start the consumer thread."""
        if self.running:
            logger.warning("Consumer already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._consume, daemon=True)
        self.thread.start()
        logger.info(f"Started Kafka consumer thread for topic: {self.topic}")
    
    def stop(self) -> None:
        """Stop the consumer thread."""
        self.running = False
        if self._consumer is not None:
            self._consumer.close()
        if self.thread:
            self.thread.join(timeout=5)
    
    def _consume(self) -> None:
        """
        Main consumer loop.
        Runs in background thread.
        """
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError

        retry_delay = 5

        while self.running:
            consumer = None
            try:
                logger.info(f"Connecting to Kafka at {self.bootstrap_servers}...")

                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    group_id=self.group_id,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                )
                self._consumer = consumer

                self.state.clear_error()
                logger.info(f"Connected! Consuming from {self.topic}")

                while self.running:
                    records = consumer.poll(timeout_ms=1000)
                    for messages in records.values():
                        for message in messages:
                            try:
                                data = message.value
                                segment_id = data.get("segment_id")

                                if segment_id:
                                    self.state.update(segment_id, data)

                            except Exception as e:
                                logger.error(f"Error processing message: {e}")
                                continue

            except KafkaError as e:
                error_msg = f"Kafka error: {e}"
                self.state.set_error(error_msg)
                logger.error(error_msg)

            except Exception as e:
                error_msg = f"Consumer error: {e}"
                self.state.set_error(error_msg)
                logger.error(error_msg)

            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        logger.exception("Error closing Kafka consumer")
                if self._consumer is consumer:
                    self._consumer = None

            if self.running:
                time.sleep(retry_delay)


def start_live_feed(
    bootstrap_servers: str,
    topic: str = config.TOPIC_CONGESTION,
) -> tuple[LiveState, KafkaConsumerThread]:
    """
    Initialize and start the live data feed.
    
    Usage:
        state, consumer = start_live_feed("localhost:29092")
        
        # Later in Streamlit:
        data, history, count, last_update, error = state.get_snapshot()
    
    Returns:
        (LiveState, KafkaConsumerThread)
    """
    state = LiveState()
    consumer = KafkaConsumerThread(state, bootstrap_servers, topic)
    consumer.start()
    
    return state, consumer


if __name__ == "__main__":
    # Test the live state
    import sys
    from pathlib import Path
    
    # Add src to path
    src_path = Path(__file__).parent.parent.parent
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    
    if len(sys.argv) < 2:
        print("Usage: python live_state.py <bootstrap_servers>")
        print("Example: python live_state.py localhost:9092")
        sys.exit(1)
    
    bootstrap = sys.argv[1]
    topic = config.TOPIC_CONGESTION
    
    print(f"Starting live feed from {bootstrap}, topic: {topic}")
    state, consumer = start_live_feed(bootstrap, topic)
    
    try:
        while True:
            time.sleep(5)
            data, history, count, last_update, error = state.get_snapshot()
            
            print(f"\n{'='*60}")
            print(f"Messages: {count} | Segments: {len(data)} | Error: {error}")
            
            if data:
                # Show 3 random segments
                import random
                samples = random.sample(list(data.items()), min(3, len(data)))
                for seg_id, msg in samples:
                    speed = msg.get(
                        "current_speed_kmph", msg.get("current_speed", 0.0)
                    )
                    print(f"  {seg_id}: {speed:.1f} km/h")
    
    except KeyboardInterrupt:
        print("\nStopping...")
        consumer.stop()
