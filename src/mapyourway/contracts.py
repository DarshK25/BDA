"""Shared serving semantics. Observation age is distinct from delivery age."""
import math
import time
from datetime import datetime, timezone

from mapyourway import config

LEVEL_NAMES = ("free_flow", "moderate", "heavy", "severe")
LEVEL_COLORS = ("#2ecc71", "#f39c12", "#e67e22", "#e74c3c")


def congestion_level(ratio: float) -> int:
    if not math.isfinite(float(ratio)):
        raise ValueError("speed ratio must be finite")
    return 0 if ratio >= 0.8 else 1 if ratio >= 0.5 else 2 if ratio >= 0.3 else 3


def timestamp_ms(value) -> int:
    """Convert a supported event timestamp to milliseconds, safely.

    Kafka payloads are external input.  A malformed timestamp must not kill the
    dashboard consumer; it is treated as missing and the record will be shown
    as unknown rather than silently appearing fresh.
    """
    if isinstance(value, (int, float)):
        return int(value)
    if not value:
        return 0
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError):
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def observation_status(message: dict, now_ms=None) -> str:
    if not message:
        return "unknown"
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    observed = message.get("source_observed_at_ms") or message.get("observed_at_ms")
    observed_ms = timestamp_ms(observed)
    if not observed_ms:
        return "unknown"
    age = now_ms - observed_ms
    if age < -60000 or age > config.TRAFFIC_SOURCE_TTL_SECONDS * 1000:
        return "stale"
    if message.get("road_closure") is True:
        return "closed"
    speed = message.get("current_speed_kmph")
    try:
        valid_speed = speed is not None and math.isfinite(float(speed)) and 0 <= float(speed) <= 150
    except (TypeError, ValueError):
        valid_speed = False
    if not valid_speed:
        return "unknown"
    quality = message.get("source_confidence")
    try:
        valid_quality = quality is not None and float(quality) >= config.MIN_SOURCE_CONFIDENCE
    except (TypeError, ValueError):
        valid_quality = False
    return "fresh" if valid_quality else "low_confidence"
