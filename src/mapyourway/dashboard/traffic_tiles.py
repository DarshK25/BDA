"""Small server-side proxy for TomTom traffic flow raster tiles.

The browser only talks to this process.  The TomTom API key is read at
runtime and is never accepted from, or returned to, a browser request.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlsplit

import requests


UPSTREAM_TILE_URL = (
    "https://api.tomtom.com/traffic/map/4/tile/flow/relative0/"
    "{z}/{x}/{y}.png"
)
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8502
MIN_ZOOM = 8
MAX_ZOOM = 18
TILE_SIZE = 256
MAX_TILE_BYTES = 4 * 1024 * 1024
MAX_REQUEST_TARGET_BYTES = 512
MAX_IN_FLIGHT_REQUESTS = 8
TILE_CACHE_SECONDS = 120.0
STATUS_TTL_SECONDS = 300.0
UPSTREAM_TIMEOUT = (3.0, 5.0)
FAILURE_COOLDOWN_SECONDS = 15.0
MAX_FAILURE_COOLDOWN_SECONDS = 120.0
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
CORS_ORIGIN = os.getenv("TRAFFIC_TILE_CORS_ORIGIN", "http://localhost:8501")

ERROR_NOT_CONFIGURED = "API key not configured"
ERROR_AUTHORIZATION = "upstream authorization failed"
ERROR_RATE_LIMITED = "upstream rate limited"
ERROR_TIMEOUT = "upstream timeout"
ERROR_UNAVAILABLE = "upstream unavailable"
ERROR_EXPIRED = "last successful tile expired"
ERROR_BUSY = "traffic tile service busy"
GENERIC_TILE_ERROR = "traffic tile unavailable\n"

_TILE_PATH = re.compile(r"^/tiles/([0-9]+)/([0-9]+)/([0-9]+)\.png$")


@dataclass(frozen=True)
class _TileResult:
    status: int
    body: bytes
    headers: dict[str, str]
    error: str | None = None


@dataclass
class _CachedTile:
    result: _TileResult
    expires_at: float


@dataclass
class _InFlight:
    event: threading.Event
    result: _TileResult | None = None


def parse_tile_path(path: str) -> tuple[int, int, int] | None:
    """Return a legal tile coordinate, or ``None`` for an invalid path."""

    match = _TILE_PATH.fullmatch(path)
    if match is None:
        return None
    z, x, y = (int(value) for value in match.groups())
    if not MIN_ZOOM <= z <= MAX_ZOOM:
        return None
    max_coordinate = (1 << z) - 1
    if x > max_coordinate or y > max_coordinate:
        return None
    return z, x, y


def _cache_seconds(cache_control: str | None) -> float:
    """Return a bounded in-memory cache lifetime permitted by Cache-Control."""

    if not cache_control:
        return TILE_CACHE_SECONDS
    directives = {
        part.strip().lower().split("=", 1)[0]
        for part in cache_control.split(",")
        if part.strip()
    }
    if directives.intersection({"no-store", "no-cache", "private"}):
        return 0.0
    for part in cache_control.split(","):
        name, separator, value = part.strip().lower().partition("=")
        if name == "max-age" and separator:
            try:
                return max(0.0, min(TILE_CACHE_SECONDS, float(value)))
            except ValueError:
                return 0.0
    return TILE_CACHE_SECONDS


class TrafficTileService:
    """Fetch, validate, and briefly cache TomTom raster flow tiles."""

    def __init__(
        self,
        api_key: str | None,
        *,
        http_get: Callable[..., Any] | None = None,
        timeout: float | tuple[float, float] = UPSTREAM_TIMEOUT,
        status_ttl_seconds: float = STATUS_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.configured = bool(self.api_key)
        self._http_get = http_get or requests.get
        self._timeout = timeout
        self._status_ttl_seconds = max(0.0, float(status_ttl_seconds))
        self._clock = clock
        self._wall_clock = wall_clock
        self._lock = threading.RLock()
        self._slots = threading.Semaphore(MAX_IN_FLIGHT_REQUESTS)
        self._cache: dict[tuple[int, int, int], _CachedTile] = {}
        self._in_flight: dict[tuple[int, int, int], _InFlight] = {}
        self._last_success_at: float | None = None
        self._last_success_ms: int | None = None
        self._available = False
        self._error = None if self.configured else ERROR_NOT_CONFIGURED
        self._cooldown_until = 0.0
        self._failure_count = 0

    @classmethod
    def from_environment(cls) -> "TrafficTileService":
        """Load optional dotenv configuration, then read the server-only key."""

        try:
            from dotenv import load_dotenv
        except ImportError:
            pass
        else:
            load_dotenv()
        return cls(os.getenv("TOM_TOM_API_KEY") or os.getenv("TOMTOM_API_KEY"))

    def status(self) -> dict[str, Any]:
        """Return the stable status interface without exposing provider details."""

        with self._lock:
            error = self._error
            available = self._available
            if (
                self._last_success_at is not None
                and self._clock() - self._last_success_at > self._status_ttl_seconds
            ):
                available = False
                if self.configured:
                    error = ERROR_EXPIRED
            return {
                "configured": self.configured,
                "available": bool(available),
                "last_success_ms": self._last_success_ms,
                "error": error,
            }

    def get_tile(self, z: int, x: int, y: int) -> _TileResult:
        """Return one tile result, coalescing concurrent requests for its key."""

        key = (z, x, y)
        now = self._clock()
        with self._lock:
            cached = self._cache.get(key)
            if cached is not None:
                if cached.expires_at > now:
                    return cached.result
                del self._cache[key]
            if self._cooldown_until > now:
                return self._failure_result(self._error or ERROR_UNAVAILABLE)
            in_flight = self._in_flight.get(key)
            if in_flight is None:
                in_flight = _InFlight(threading.Event())
                self._in_flight[key] = in_flight
                owner = True
            else:
                owner = False

        if not owner:
            wait_seconds = self._timeout_seconds() + 1.0
            if not in_flight.event.wait(wait_seconds):
                return self._failure_result(ERROR_UNAVAILABLE)
            return in_flight.result or self._failure_result(ERROR_UNAVAILABLE)

        result: _TileResult
        if not self.configured:
            result = self._failure_result(ERROR_NOT_CONFIGURED)
        elif not self._slots.acquire(blocking=False):
            result = self._failure_result(ERROR_BUSY)
        else:
            try:
                result = self._fetch_tile(z, x, y, key)
            finally:
                self._slots.release()
        with self._lock:
            in_flight.result = result
            self._in_flight.pop(key, None)
            in_flight.event.set()
        return result

    def _fetch_tile(
        self, z: int, x: int, y: int, key: tuple[int, int, int]
    ) -> _TileResult:
        url = UPSTREAM_TILE_URL.format(z=z, x=x, y=y)
        response = None
        try:
            response = self._http_get(
                url,
                params={"key": self.api_key, "tileSize": str(TILE_SIZE)},
                timeout=self._timeout,
                stream=True,
                allow_redirects=False,
            )
        except requests.Timeout:
            self._set_failure(ERROR_TIMEOUT)
            return self._failure_result(ERROR_TIMEOUT, status=503)
        except requests.RequestException:
            self._set_failure(ERROR_UNAVAILABLE)
            return self._failure_result(ERROR_UNAVAILABLE, status=503)
        except Exception:
            # The proxy must never turn a transport/library traceback into a
            # response, and this deliberately avoids echoing a URL or key.
            self._set_failure(ERROR_UNAVAILABLE)
            return self._failure_result(ERROR_UNAVAILABLE, status=503)

        try:
            upstream_status = int(response.status_code)
            if upstream_status != 200:
                error = {
                    403: ERROR_AUTHORIZATION,
                    429: ERROR_RATE_LIMITED,
                }.get(upstream_status, ERROR_UNAVAILABLE)
                if upstream_status in {403, 429}:
                    self._set_failure(error, cooldown=True)
                else:
                    self._set_failure(error)
                return self._failure_result(error, status=503 if upstream_status == 429 else 502)

            content_type = str(response.headers.get("Content-Type", "")).lower()
            if not content_type.startswith("image/png"):
                self._set_failure(ERROR_UNAVAILABLE)
                return self._failure_result(ERROR_UNAVAILABLE, status=502)
            content_length = response.headers.get("Content-Length")
            if content_length is not None:
                try:
                    if int(content_length) > MAX_TILE_BYTES:
                        self._set_failure(ERROR_UNAVAILABLE)
                        return self._failure_result(ERROR_UNAVAILABLE, status=502)
                except (TypeError, ValueError):
                    pass
            body = self._read_body(response)
            if body is None or not body.startswith(PNG_SIGNATURE):
                self._set_failure(ERROR_UNAVAILABLE)
                return self._failure_result(ERROR_UNAVAILABLE, status=502)

            cache_control = response.headers.get("Cache-Control")
            result = _TileResult(
                status=200,
                body=body,
                headers={
                    "Content-Type": "image/png",
                    **({"Cache-Control": cache_control} if cache_control else {}),
                },
            )
            cache_seconds = _cache_seconds(cache_control)
            with self._lock:
                if cache_seconds > 0:
                    self._cache[key] = _CachedTile(result, self._clock() + cache_seconds)
            self._set_success()
            return result
        except Exception:
            self._set_failure(ERROR_UNAVAILABLE)
            return self._failure_result(ERROR_UNAVAILABLE, status=502)
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass

    @staticmethod
    def _read_body(response: Any) -> bytes | None:
        try:
            chunks = response.iter_content(chunk_size=64 * 1024)
            body = bytearray()
            for chunk in chunks:
                if not chunk:
                    continue
                body.extend(chunk)
                if len(body) > MAX_TILE_BYTES:
                    return None
            return bytes(body)
        except (AttributeError, OSError, requests.RequestException, TypeError, ValueError):
            return None

    def _set_success(self) -> None:
        with self._lock:
            self._last_success_at = self._clock()
            self._last_success_ms = int(self._wall_clock() * 1000)
            self._available = True
            self._error = None
            self._failure_count = 0
            self._cooldown_until = 0.0

    def _set_failure(self, error: str, *, cooldown: bool = False) -> None:
        with self._lock:
            self._available = False
            self._error = error
            if cooldown:
                self._failure_count += 1
                delay = min(
                    FAILURE_COOLDOWN_SECONDS * (2 ** (self._failure_count - 1)),
                    MAX_FAILURE_COOLDOWN_SECONDS,
                )
                self._cooldown_until = self._clock() + delay

    @staticmethod
    def _failure_result(error: str, *, status: int = 503) -> _TileResult:
        return _TileResult(status=status, body=GENERIC_TILE_ERROR.encode(), headers={}, error=error)

    def _timeout_seconds(self) -> float:
        timeout = self._timeout
        if isinstance(timeout, tuple):
            return max(float(value) for value in timeout)
        return float(timeout)


def _send_json(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _send_text(handler: BaseHTTPRequestHandler, status: int, body: str) -> None:
    encoded = body.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/plain; charset=utf-8")
    handler.send_header("Content-Length", str(len(encoded)))
    handler.end_headers()
    handler.wfile.write(encoded)


def make_handler(service: TrafficTileService) -> type[BaseHTTPRequestHandler]:
    """Build a handler bound to one process-wide tile service."""

    class TrafficTileHandler(BaseHTTPRequestHandler):
        server_version = "MapYourWayTrafficTiles/1"

        def end_headers(self):  # noqa: N802 - required by BaseHTTPRequestHandler
            self.send_header("Access-Control-Allow-Origin", CORS_ORIGIN)
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            super().end_headers()

        def do_OPTIONS(self):  # noqa: N802 - required by BaseHTTPRequestHandler
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):  # noqa: N802 - required by BaseHTTPRequestHandler
            if len(self.path.encode("utf-8", errors="ignore")) > MAX_REQUEST_TARGET_BYTES:
                _send_text(self, 414, "request target too long\n")
                return
            parsed = urlsplit(self.path)
            if parsed.path == "/health":
                _send_text(self, 200, "ok\n")
                return
            if parsed.path == "/status":
                _send_json(self, 200, service.status())
                return
            coordinates = parse_tile_path(parsed.path)
            if coordinates is None:
                _send_text(self, 400, "invalid tile request\n")
                return
            result = service.get_tile(*coordinates)
            if result.status != 200:
                _send_text(self, result.status, GENERIC_TILE_ERROR)
                return
            self.send_response(200)
            for name, value in result.headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(result.body)))
            self.end_headers()
            self.wfile.write(result.body)

        def do_HEAD(self):  # noqa: N802 - required by BaseHTTPRequestHandler
            _send_text(self, 405, "method not allowed\n")

        def log_message(self, *_args):
            # Avoid writing request targets or provider details to container logs.
            return

    return TrafficTileHandler


def _port_from_environment() -> int:
    value = os.getenv("TILE_PORT", str(DEFAULT_PORT))
    try:
        port = int(value)
    except ValueError as exc:
        raise ValueError("TILE_PORT must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ValueError("TILE_PORT must be between 1 and 65535")
    return port


def create_server(
    service: TrafficTileService | None = None,
    *,
    host: str = DEFAULT_HOST,
    port: int | None = None,
) -> ThreadingHTTPServer:
    """Create the service server without starting its serving loop."""

    if service is None:
        service = TrafficTileService.from_environment()
    if port is None:
        port = _port_from_environment()
    server = ThreadingHTTPServer((host, port), make_handler(service))
    server.daemon_threads = True
    return server


def main() -> None:
    server = create_server()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
