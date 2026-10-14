"""Small contract tests for the real-data boundary.

These tests do not contact Kafka or TomTom.  They verify that a TomTom-shaped
response becomes the event shapes consumed by the Spark job and dashboard.
"""

from mapyourway import config
from mapyourway.dashboard.live_state import LiveState
from mapyourway.dashboard.live_state import KafkaConsumerThread
from mapyourway.network import SEGMENTS
from mapyourway.simulator.real_producer import (
    ProbeFleet,
    fetch_and_publish_traffic,
    fetch_open_meteo_weather,
)


class FakeResponse:
    status_code = 200

    def json(self):
        return {
            "flowSegmentData": {
                "currentSpeed": 30,
                "freeFlowSpeed": 60,
                "currentTravelTime": 120,
                "freeFlowTravelTime": 60,
                "confidence": 0.91,
                "roadClosure": False,
            }
        }


class EmptyFlowResponse(FakeResponse):
    def json(self):
        return {}


class FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse()


class EmptyFlowSession(FakeSession):
    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return EmptyFlowResponse()


class FakeWeatherResponse:
    status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return {
            "current": {
                "time": "2026-10-09T12:00",
                "temperature_2m": 29.5,
                "relative_humidity_2m": 70,
                "rain": 1.2,
                "visibility": 6500,
                "wind_speed_10m": 12.0,
                "weather_code": 63,
            }
        }


class FakeWeatherSession:
    def get(self, url, **kwargs):
        return FakeWeatherResponse()


class FakeProducer:
    def __init__(self):
        self.messages = []
        self.flushes = 0

    def send(self, topic, key, value):
        self.messages.append((topic, key, value))

    def flush(self):
        self.flushes += 1


def test_tomtom_flow_is_published_as_gps_and_sensor_events():
    producer = FakeProducer()
    session = FakeSession()

    result = fetch_and_publish_traffic(
        producer,
        session=session,
        api_key="test-key",
        segments=SEGMENTS[:1],
        now_ms=123,
    )

    assert result == (1, 0)
    assert [message[0] for message in producer.messages] == [
        config.TOPIC_SENSOR,
        config.TOPIC_GPS,
    ]
    sensor = producer.messages[0][2]
    gps = producer.messages[1][2]
    assert sensor["segment_id"] == "S01"
    assert sensor["occupancy_pct"] == 60.0
    assert gps["speed_kmph"] == 30.0
    assert gps["current_travel_time_s"] == 120
    assert gps["confidence"] == 0.91
    assert session.calls[0][1]["params"]["key"] == "test-key"
    assert producer.flushes == 1


def test_live_state_reads_spark_speed_field_names():
    state = LiveState()
    state.update(
        "S01",
        {
            "timestamp": "2026-10-08T12:00:00Z",
            "current_speed_kmph": 30.0,
            "free_flow_kmph": 60.0,
        },
    )

    data, history, count, last_update, error = state.get_snapshot()
    assert data["S01"]["current_speed_kmph"] == 30.0
    assert history[-1]["congestion"] == 0.5
    assert count == 1
    assert last_update is not None
    assert error is None


def test_kafka_consumer_closes_failed_consumer_before_retry(monkeypatch):
    """A failed poll must not leave a live member in the dashboard group."""
    import kafka
    from kafka.errors import KafkaError

    class FailingConsumer:
        def __init__(self, *args, **kwargs):
            self.closed = False

        def poll(self, timeout_ms):
            raise KafkaError("poll failed")

        def close(self):
            self.closed = True

    consumer = FailingConsumer()
    monkeypatch.setattr(kafka, "KafkaConsumer", lambda *args, **kwargs: consumer)

    state = LiveState()
    thread = KafkaConsumerThread(state, "kafka:9092")
    thread.running = True

    def stop_after_retry(_delay):
        thread.running = False

    monkeypatch.setattr("mapyourway.dashboard.live_state.time.sleep", stop_after_retry)
    thread._consume()

    assert consumer.closed is True
    assert thread._consumer is None


def test_empty_tomtom_flow_is_not_counted_as_success():
    producer = FakeProducer()
    result = fetch_and_publish_traffic(
        producer,
        session=EmptyFlowSession(),
        api_key="test-key",
        segments=SEGMENTS[:1],
        now_ms=123,
    )
    assert result == (0, 1)
    assert producer.messages == []


def test_open_meteo_weather_is_normalized_to_event_contract():
    weather = fetch_open_meteo_weather(FakeWeatherSession(), now_ms=123)

    assert weather["source"] == "open_meteo_current"
    assert weather["rain_mm_hr"] == 1.2
    assert weather["visibility_km"] == 6.5
    assert weather["temp_c"] == 29.5
    assert weather["ts_ms"] == 123


def test_probe_fleet_produces_multiple_explicitly_derived_gps_events():
    fleet = ProbeFleet(vehicles_per_segment=3)
    events = fleet.events(
        SEGMENTS[0],
        speed_kmph=30.0,
        free_speed_kmph=60.0,
        now_ms=123,
        elapsed_seconds=10.0,
        confidence=0.9,
    )

    assert len(events) == 3
    assert len({event["vehicle_id"] for event in events}) == 3
    assert all(event["source"] == "derived_probe_from_tomtom_flow" for event in events)
