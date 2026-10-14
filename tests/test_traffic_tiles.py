"""HTTP contract tests for the server-side TomTom traffic tile proxy."""

from __future__ import annotations

import json
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from mapyourway.dashboard.traffic_tiles import (
    STATUS_TTL_SECONDS,
    TrafficTileService,
    make_handler,
)


PNG = b"\x89PNG\r\n\x1a\ntraffic-tile"


class StubState:
    def __init__(self, status=200, body=PNG, headers=None, delay=0):
        self.status = status
        self.body = body
        self.headers = headers or {"Content-Type": "image/png", "Cache-Control": "max-age=60"}
        self.delay = delay
        self.requests = []


def _stub_handler(state):
    class StubHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - required by BaseHTTPRequestHandler
            state.requests.append((self.path, dict(self.headers)))
            if state.delay:
                time.sleep(state.delay)
            self.send_response(state.status)
            for name, value in state.headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(state.body)))
            self.end_headers()
            try:
                self.wfile.write(state.body)
            except OSError:
                # The proxy may close the timed-out upstream connection first.
                pass

        def log_message(self, *_args):
            return

    return StubHandler


@contextmanager
def running_server(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _proxy_get(stub_server, seen_urls):
    def get(url, **kwargs):
        assert kwargs.get("allow_redirects") is False
        seen_urls.append(requests.Request("GET", url, params=kwargs.get("params")).prepare().url)
        parsed = urlsplit(url)
        assert parsed.scheme == "https"
        assert parsed.netloc == "api.tomtom.com"
        return requests.get(
            f"http://127.0.0.1:{stub_server.server_port}{parsed.path}?{parsed.query}",
            **kwargs,
        )

    return get


@contextmanager
def proxy_server(state, **service_kwargs):
    with running_server(_stub_handler(state)) as upstream:
        seen_urls = []
        service = TrafficTileService(
            api_key="server-secret",
            http_get=_proxy_get(upstream, seen_urls),
            **service_kwargs,
        )
        with running_server(make_handler(service)) as proxy:
            yield proxy, upstream, state, seen_urls, service


def get_tile(proxy, path="/tiles/10/123/456.png?v=120"):
    return requests.get(f"http://127.0.0.1:{proxy.server_port}{path}", timeout=2)


def test_valid_tile_is_forwarded_as_png_with_fixed_upstream_contract():
    state = StubState(headers={"Content-Type": "image/png", "Cache-Control": "max-age=60"})
    with proxy_server(state) as (proxy, _upstream, state, seen_urls, _service):
        response = get_tile(proxy)

        assert response.status_code == 200
        assert response.content == PNG
        assert response.headers["Content-Type"].startswith("image/png")
        assert response.headers["Cache-Control"] == "max-age=60"
        assert response.headers["Access-Control-Allow-Origin"] == "http://localhost:8501"
        assert len(state.requests) == 1
        path, _headers = state.requests[0]
        parsed = urlsplit(path)
        assert parsed.path == "/traffic/map/4/tile/flow/relative0/10/123/456.png"
        assert parse_qs(parsed.query) == {"key": ["server-secret"], "tileSize": ["256"]}
        assert len(seen_urls) == 1
        assert "server-secret" in seen_urls[0]
        assert "relative0" in seen_urls[0]


def test_valid_cacheable_tile_is_coalesced_and_cached_for_refresh_window():
    state = StubState()
    with proxy_server(state) as (proxy, _upstream, state, _seen_urls, _service):
        first = get_tile(proxy, "/tiles/10/123/456.png?v=one")
        second = get_tile(proxy, "/tiles/10/123/456.png?v=two")

        assert first.status_code == second.status_code == 200
        assert first.content == second.content == PNG
        assert len(state.requests) == 1


def test_no_store_response_is_forwarded_but_not_cached():
    state = StubState(headers={"Content-Type": "image/png", "Cache-Control": "no-store"})
    with proxy_server(state) as (proxy, _upstream, state, _seen_urls, _service):
        assert get_tile(proxy).status_code == 200
        assert get_tile(proxy).status_code == 200

        assert len(state.requests) == 2


@pytest.mark.parametrize("path", [
    "/tiles/7/0/0.png",
    "/tiles/19/0/0.png",
    "/tiles/10/1024/0.png",
    "/tiles/10/0/1024.png",
    "/tiles/10/not-a-tile/0.png",
    "/tiles/10/0/0.jpg",
])
def test_invalid_tile_path_is_rejected_without_upstream_request(path):
    state = StubState()
    with proxy_server(state) as (proxy, _upstream, state, _seen_urls, _service):
        response = get_tile(proxy, path)

        assert response.status_code == 400
        assert len(state.requests) == 0
        assert b"server-secret" not in response.content


@pytest.mark.parametrize(
    ("upstream_status", "expected_error"),
    [(403, "upstream authorization failed"), (429, "upstream rate limited")],
)
def test_provider_failures_are_generic_and_enter_cooldown(upstream_status, expected_error):
    state = StubState(status=upstream_status, body=b"provider secret details")
    with proxy_server(state) as (proxy, _upstream, state, _seen_urls, service):
        first = get_tile(proxy)
        second = get_tile(proxy)
        status = requests.get(f"http://127.0.0.1:{proxy.server_port}/status", timeout=2)

        assert first.status_code in {502, 503}
        assert second.status_code in {502, 503}
        assert b"server-secret" not in first.content
        assert b"provider secret details" not in first.content
        assert len(state.requests) == 1
        assert status.json() == {
            "configured": True,
            "available": False,
            "last_success_ms": None,
            "error": expected_error,
        }
        assert service.status()["error"] == expected_error


def test_timeout_does_not_block_health_or_leak_exception_details():
    state = StubState(delay=0.2)
    with proxy_server(state, timeout=(0.02, 0.02)) as (proxy, _upstream, state, _seen_urls, _service):
        failed = get_tile(proxy)
        started = time.monotonic()
        health = requests.get(f"http://127.0.0.1:{proxy.server_port}/health", timeout=1)
        elapsed = time.monotonic() - started
        status = requests.get(f"http://127.0.0.1:{proxy.server_port}/status", timeout=1)

        assert failed.status_code == 503
        assert b"server-secret" not in failed.content
        assert b"HTTPConnectionPool" not in failed.content
        assert health.status_code == 200
        assert elapsed < 0.5
        assert status.json()["available"] is False
        assert status.json()["error"] == "upstream timeout"


def test_status_expires_after_last_success():
    state = StubState()
    monotonic = [0.0]

    with proxy_server(
        state,
        status_ttl_seconds=0.03,
        clock=lambda: monotonic[0],
    ) as (proxy, _upstream, _state, _seen_urls, _service):
        assert get_tile(proxy).status_code == 200
        fresh = requests.get(f"http://127.0.0.1:{proxy.server_port}/status", timeout=2).json()
        assert fresh["configured"] is True
        assert fresh["available"] is True
        assert isinstance(fresh["last_success_ms"], int)
        monotonic[0] = 0.04
        expired = requests.get(f"http://127.0.0.1:{proxy.server_port}/status", timeout=2).json()

        assert expired["available"] is False
        assert expired["last_success_ms"] == fresh["last_success_ms"]
        assert expired["error"] == "last successful tile expired"


def test_non_png_success_is_not_forwarded_as_a_tile():
    state = StubState(body=b"not an image", headers={"Content-Type": "text/html"})
    with proxy_server(state) as (proxy, _upstream, _state, _seen_urls, _service):
        response = get_tile(proxy)

        assert response.status_code == 502
        assert response.content == b"traffic tile unavailable\n"
        assert b"server-secret" not in response.content


def test_health_is_stable_when_service_is_not_configured():
    service = TrafficTileService(api_key="", http_get=lambda *_args, **_kwargs: pytest.fail("unexpected upstream"))
    with running_server(make_handler(service)) as proxy:
        health = requests.get(f"http://127.0.0.1:{proxy.server_port}/health", timeout=1)
        status = requests.get(f"http://127.0.0.1:{proxy.server_port}/status", timeout=1)

        assert health.status_code == 200
        assert health.content == b"ok\n"
        assert status.json() == {
            "configured": False,
            "available": False,
            "last_success_ms": None,
            "error": "API key not configured",
        }


def test_status_contract_keys_are_stable():
    service = TrafficTileService(api_key="server-secret")
    assert list(service.status()) == [
        "configured",
        "available",
        "last_success_ms",
        "error",
    ]
    assert STATUS_TTL_SECONDS > 0
