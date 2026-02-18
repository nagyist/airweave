"""Unit tests for HTTP metrics adapter and middleware."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from airweave.adapters.http_metrics import FakeHttpMetrics, PrometheusHttpMetrics


class TestFakeHttpMetrics:
    """Tests for the FakeHttpMetrics test helper."""

    def test_clear_resets_all_state(self):
        """clear() should empty every collection and reset generate_calls."""
        fake = FakeHttpMetrics()
        fake.inc_in_progress("GET")
        fake.observe_request("GET", "/test", "200", 0.01)
        fake.observe_response_size("GET", "/test", 512)
        fake.generate()

        fake.clear()

        assert fake.in_progress == {}
        assert fake.requests == []
        assert fake.response_sizes == []
        assert fake.generate_calls == 0


class TestPrometheusHttpMetrics:
    """Tests for the Prometheus adapter."""

    def test_registry_is_separate_from_default(self):
        """Adapter registry must not be the default global registry."""
        from prometheus_client import REGISTRY

        adapter = PrometheusHttpMetrics()
        assert adapter._registry is not REGISTRY

    def test_generate_returns_bytes(self):
        adapter = PrometheusHttpMetrics()
        result = adapter.generate()
        assert isinstance(result, bytes)

    def test_generate_contains_expected_families(self):
        adapter = PrometheusHttpMetrics()
        adapter.observe_request("GET", "/test", "200", 0.01)

        output = adapter.generate().decode()
        assert "airweave_http_requests_total" in output
        assert "airweave_http_request_duration_seconds" in output
        assert "airweave_http_requests_in_progress" in output
        assert "airweave_http_response_size_bytes" in output

    def test_observe_request_increments_counter(self):
        adapter = PrometheusHttpMetrics()
        adapter.observe_request("POST", "/api/v1/items", "201", 0.05)
        adapter.observe_request("POST", "/api/v1/items", "201", 0.03)

        output = adapter.generate().decode()
        assert 'airweave_http_requests_total{endpoint="/api/v1/items",method="POST",status_code="201"} 2.0' in output

    def test_in_progress_gauge(self):
        adapter = PrometheusHttpMetrics()
        adapter.inc_in_progress("GET")
        adapter.inc_in_progress("GET")
        adapter.dec_in_progress("GET")

        output = adapter.generate().decode()
        assert 'airweave_http_requests_in_progress{method="GET"} 1.0' in output

    def test_content_type_is_prometheus_format(self):
        adapter = PrometheusHttpMetrics()
        assert adapter.content_type == "text/plain; version=0.0.4; charset=utf-8"

    def test_observe_response_size(self):
        adapter = PrometheusHttpMetrics()
        adapter.observe_response_size("GET", "/api/v1/items", 1234)

        output = adapter.generate().decode()
        assert "airweave_http_response_size_bytes" in output


class TestHttpMetricsMiddleware:
    """Tests for http_metrics_middleware using FakeHttpMetrics."""

    @pytest.fixture
    def fake_metrics(self):
        return FakeHttpMetrics()

    @pytest.fixture
    def _make_request(self, fake_metrics):
        """Factory for mock Starlette Request objects with fake metrics."""

        def factory(path: str = "/api/v1/sources", method: str = "GET"):
            request = MagicMock()
            request.url.path = path
            request.method = method
            request.app.state.http_metrics = fake_metrics
            route = MagicMock()
            route.path = path
            request.scope = {"route": route}
            return request

        return factory

    @pytest.mark.asyncio
    async def test_skips_health_endpoint(self, _make_request, fake_metrics):
        """Middleware should pass through /health without recording metrics."""
        from airweave.api.middleware import http_metrics_middleware

        request = _make_request(path="/health")
        sentinel = MagicMock()
        call_next = AsyncMock(return_value=sentinel)

        response = await http_metrics_middleware(request, call_next)

        assert response is sentinel
        call_next.assert_awaited_once_with(request)
        assert len(fake_metrics.requests) == 0

    @pytest.mark.asyncio
    async def test_records_metrics_for_normal_request(self, _make_request, fake_metrics):
        """Middleware should record request via the HttpMetrics protocol."""
        from airweave.api.middleware import http_metrics_middleware

        request = _make_request(path="/api/v1/sources", method="GET")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-length": "1234"}
        call_next = AsyncMock(return_value=mock_response)

        response = await http_metrics_middleware(request, call_next)

        assert response is mock_response
        assert len(fake_metrics.requests) == 1

        rec = fake_metrics.requests[0]
        assert rec.method == "GET"
        assert rec.endpoint == "/api/v1/sources"
        assert rec.status_code == "200"
        assert rec.duration > 0

        assert len(fake_metrics.response_sizes) == 1
        assert fake_metrics.response_sizes[0].size == 1234

    @pytest.mark.asyncio
    async def test_decrements_in_progress_on_exception(self, _make_request, fake_metrics):
        """In-progress gauge must be decremented even when call_next raises."""
        from airweave.api.middleware import http_metrics_middleware

        request = _make_request(path="/api/v1/fail", method="POST")
        call_next = AsyncMock(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError, match="boom"):
            await http_metrics_middleware(request, call_next)

        # inc was called once, dec was called once → net zero
        assert fake_metrics.in_progress.get("POST", 0) == 0

    @pytest.mark.asyncio
    async def test_unmatched_route_uses_fallback(self, fake_metrics):
        """When no route is matched, endpoint label should be 'unmatched'."""
        from airweave.api.middleware import http_metrics_middleware

        request = MagicMock()
        request.url.path = "/random-bot-path"
        request.method = "GET"
        request.scope = {}  # No route matched
        request.app.state.http_metrics = fake_metrics

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.headers = {}
        call_next = AsyncMock(return_value=mock_response)

        await http_metrics_middleware(request, call_next)

        assert len(fake_metrics.requests) == 1
        assert fake_metrics.requests[0].endpoint == "unmatched"
        assert fake_metrics.requests[0].status_code == "404"

    @pytest.mark.asyncio
    async def test_no_response_size_when_header_missing(self, _make_request, fake_metrics):
        """Response size should not be recorded when content-length is absent."""
        from airweave.api.middleware import http_metrics_middleware

        request = _make_request(path="/api/v1/items", method="GET")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        call_next = AsyncMock(return_value=mock_response)

        await http_metrics_middleware(request, call_next)

        assert len(fake_metrics.response_sizes) == 0


class TestApiMetricsServer:
    """Tests for the ApiMetricsServer handler."""

    @pytest.mark.asyncio
    async def test_handle_metrics_returns_fake_body_and_content_type(self):
        """Handler should delegate to HttpMetrics.generate() and content_type."""
        from aiohttp.test_utils import make_mocked_request

        from airweave.api.metrics_server import ApiMetricsServer

        fake = FakeHttpMetrics()
        server = ApiMetricsServer(fake, port=0)

        request = make_mocked_request("GET", "/metrics")
        response = await server._handle_metrics(request)

        assert response.body == b"# fake metrics\n"
        assert response.content_type == "text/plain"
        assert fake.generate_calls == 1

    @pytest.mark.asyncio
    async def test_start_and_stop_serves_metrics(self):
        """A started server should respond with metrics on /metrics."""
        import aiohttp

        from airweave.api.metrics_server import ApiMetricsServer

        fake = FakeHttpMetrics()
        server = ApiMetricsServer(fake, port=0)
        await server.start()

        try:
            # Extract the OS-assigned port from the runner's sites.
            site = list(server._runner.sites)[0]
            sock = site._server.sockets[0]
            port = sock.getsockname()[1]

            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{port}/metrics") as resp:
                    assert resp.status == 200
                    body = await resp.read()
                    assert body == b"# fake metrics\n"
        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_stop_is_safe_when_not_started(self):
        """Calling stop() before start() must not raise."""
        from airweave.api.metrics_server import ApiMetricsServer

        fake = FakeHttpMetrics()
        server = ApiMetricsServer(fake, port=0)
        await server.stop()  # Should be a no-op
