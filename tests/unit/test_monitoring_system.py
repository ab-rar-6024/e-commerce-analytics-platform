"""
Unit tests for ECAP monitoring and APM system.

This module tests all monitoring components including:
- Prometheus metrics collection
- Distributed tracing with Jaeger
- Custom business metrics
- Performance monitoring
- SLA tracking
"""

import contextlib
import time
from unittest.mock import Mock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.middleware.metrics_middleware import (
    PrometheusMetricsMiddleware,
    get_metrics_response,
    record_analytics_job,
    record_cache_hit,
    record_cache_miss,
    record_fraud_alert,
    record_kafka_message_produced,
    record_transaction,
    record_user_registration,
)
from src.monitoring import ECAPTracing, record_exception, start_span


class TestPrometheusMetrics:
    """Test Prometheus metrics collection."""

    def test_metrics_middleware_initialization(self):
        """Test that metrics middleware initializes correctly."""
        app = FastAPI()
        middleware = PrometheusMetricsMiddleware(app)

        assert middleware is not None
        assert hasattr(middleware, "exclude_paths")
        assert "/metrics" in middleware.exclude_paths
        assert "/health" in middleware.exclude_paths

    def test_http_request_metrics_collection(self):
        """Test HTTP request metrics are collected correctly."""
        app = FastAPI()
        app.add_middleware(PrometheusMetricsMiddleware)

        @app.get("/test")
        async def test_endpoint():
            return {"message": "test"}

        client = TestClient(app)

        # Make test requests
        response = client.get("/test")
        assert response.status_code == 200

        # Check metrics are recorded
        metrics_response = get_metrics_response()
        metrics_content = metrics_response.body.decode()

        assert "http_requests_total" in metrics_content
        assert "http_request_duration_seconds" in metrics_content
        assert 'method="GET"' in metrics_content
        assert 'status="200"' in metrics_content

    def test_path_normalization(self):
        """Test that paths are normalized correctly for metrics."""
        app = FastAPI()
        middleware = PrometheusMetricsMiddleware(app)

        # Test UUID normalization
        assert (
            middleware._normalize_path(
                "/api/users/123e4567-e89b-12d3-a456-426614174000"
            )
            == "/api/users/{uuid}"
        )

        # Test numeric ID normalization
        assert middleware._normalize_path("/api/users/12345") == "/api/users/{id}"

        # Test long ID normalization
        assert (
            middleware._normalize_path("/api/sessions/abcdef1234567890abcdef1234567890")
            == "/api/sessions/{id}"
        )

        # Test normal paths remain unchanged
        assert middleware._normalize_path("/api/users") == "/api/users"

    def test_exclude_paths(self):
        """Test that excluded paths don't generate metrics."""
        app = FastAPI()
        app.add_middleware(PrometheusMetricsMiddleware)

        @app.get("/metrics")
        async def metrics():
            return get_metrics_response()

        @app.get("/health")
        async def health():
            return {"status": "healthy"}

        client = TestClient(app)

        # These requests should not generate HTTP metrics
        client.get("/metrics")
        client.get("/health")

        metrics_content = get_metrics_response().body.decode()

        # Should not contain metrics for excluded paths
        assert 'endpoint="/metrics"' not in metrics_content
        assert 'endpoint="/health"' not in metrics_content

    def test_custom_business_metrics(self):
        """Test custom business metrics recording."""
        # Test cache metrics
        record_cache_hit("user_cache")
        record_cache_miss("session_cache")

        # Test Kafka metrics
        record_kafka_message_produced("user-events")
        record_kafka_message_produced("transactions")

        # Test analytics metrics
        record_analytics_job("fraud_detection", "completed", 5.5)
        record_analytics_job("segmentation", "failed", 2.1)

        # Test business metrics
        record_user_registration()
        record_transaction("completed", "credit_card")
        record_fraud_alert("high_amount", "critical")

        metrics_content = get_metrics_response().body.decode()

        # Verify custom metrics are present
        assert "cache_hits_total" in metrics_content
        assert "cache_misses_total" in metrics_content
        assert "kafka_messages_produced_total" in metrics_content
        assert "analytics_jobs_total" in metrics_content
        assert "user_registrations_total" in metrics_content
        assert "transactions_total" in metrics_content
        assert "fraud_alerts_total" in metrics_content

    def test_metrics_endpoint_response_format(self):
        """Test that metrics endpoint returns properly formatted Prometheus data."""
        response = get_metrics_response()

        assert response.media_type == "text/plain; version=0.0.4; charset=utf-8"

        content = response.body.decode()

        # Should contain Prometheus format elements
        assert "# HELP" in content
        assert "# TYPE" in content


class TestDistributedTracing:
    """Test distributed tracing functionality."""

    def test_tracing_initialization(self):
        """Test tracing system initialization."""
        with patch("src.monitoring.tracing.trace") as mock_trace:
            mock_tracer_provider = Mock()
            mock_trace.set_tracer_provider.return_value = None
            mock_trace.get_tracer_provider.return_value = mock_tracer_provider
            mock_trace.get_tracer.return_value = Mock()

            tracing = ECAPTracing(
                service_name="test-service", service_version="1.0.0", environment="test"
            )
            tracing.initialize_tracing()

            assert tracing.service_name == "test-service"
            assert tracing.service_version == "1.0.0"
            assert tracing.environment == "test"
            mock_trace.set_tracer_provider.assert_called_once()

    def test_fastapi_instrumentation(self):
        """Test FastAPI instrumentation setup."""
        with patch("src.monitoring.tracing.FastAPIInstrumentor") as mock_instrumentor:
            tracing = ECAPTracing()
            app = FastAPI()

            tracing.instrument_fastapi(app)

            mock_instrumentor.instrument_app.assert_called_once()

    def test_database_instrumentation(self):
        """Test database instrumentation setup."""
        with patch("src.monitoring.tracing.Psycopg2Instrumentor") as mock_instrumentor:
            tracing = ECAPTracing()

            tracing.instrument_database()

            mock_instrumentor.return_value.instrument.assert_called_once()

    def test_redis_instrumentation(self):
        """Test Redis instrumentation setup."""
        with patch("src.monitoring.tracing.RedisInstrumentor") as mock_instrumentor:
            tracing = ECAPTracing()

            tracing.instrument_redis()

            mock_instrumentor.return_value.instrument.assert_called_once()

    def test_kafka_instrumentation(self):
        """Test Kafka instrumentation setup."""
        with patch("src.monitoring.tracing.KafkaInstrumentor") as mock_instrumentor:
            tracing = ECAPTracing()

            tracing.instrument_kafka()

            mock_instrumentor.return_value.instrument.assert_called_once()

    @patch("src.monitoring.tracing._tracing_instance")
    def test_manual_span_creation(self, mock_instance):
        """Test manual span creation and attribute addition."""
        mock_tracer = Mock()
        mock_span = Mock()
        mock_span.is_recording.return_value = True
        mock_tracer.start_span.return_value = mock_span
        mock_instance.get_tracer.return_value = mock_tracer

        # Test span creation
        span = start_span("test_operation", {"key": "value"})

        mock_tracer.start_span.assert_called_once_with("test_operation")
        mock_span.set_attribute.assert_called_with("key", "value")

    @patch("src.monitoring.tracing._tracing_instance")
    def test_span_exception_recording(self, mock_instance):
        """Test exception recording in spans."""
        mock_span = Mock()
        mock_span.is_recording.return_value = True

        test_exception = Exception("Test error")

        record_exception(mock_span, test_exception)

        mock_span.record_exception.assert_called_once_with(test_exception)
        mock_span.set_status.assert_called_once()


class TestPerformanceMonitoring:
    """Test performance monitoring capabilities."""

    def test_request_timing(self):
        """Test that request timing is measured correctly."""
        app = FastAPI()
        app.add_middleware(PrometheusMetricsMiddleware)

        @app.get("/slow")
        async def slow_endpoint():
            import asyncio

            await asyncio.sleep(0.1)  # 100ms delay
            return {"message": "slow"}

        client = TestClient(app)

        start_time = time.time()
        response = client.get("/slow")
        duration = time.time() - start_time

        assert response.status_code == 200
        assert duration >= 0.1  # Should take at least 100ms

        # Check that duration metric was recorded
        metrics_content = get_metrics_response().body.decode()
        assert "http_request_duration_seconds" in metrics_content

    def test_concurrent_requests_tracking(self):
        """Test that concurrent requests are tracked correctly."""
        app = FastAPI()
        app.add_middleware(PrometheusMetricsMiddleware)

        @app.get("/test")
        async def test_endpoint():
            return {"message": "test"}

        client = TestClient(app)

        # Make multiple requests
        for _ in range(5):
            response = client.get("/test")
            assert response.status_code == 200

        metrics_content = get_metrics_response().body.decode()
        assert "http_requests_in_progress" in metrics_content


class TestSLAMonitoring:
    """Test SLA monitoring functionality."""

    def test_availability_calculation(self):
        """Test availability SLA calculation."""
        app = FastAPI()
        app.add_middleware(PrometheusMetricsMiddleware)

        @app.get("/success")
        async def success_endpoint():
            return {"status": "success"}

        @app.get("/error")
        async def error_endpoint():
            raise Exception("Test error")

        client = TestClient(app)

        # Generate successful requests
        for _ in range(95):
            client.get("/success")

        # Generate failed requests
        for _ in range(5):
            with contextlib.suppress(Exception):
                client.get("/error")

        metrics_content = get_metrics_response().body.decode()

        # Should have both success and error metrics
        assert "http_requests_total" in metrics_content
        assert 'status="200"' in metrics_content
        assert 'status="500"' in metrics_content

    def test_response_time_percentiles(self):
        """Test response time percentile tracking."""
        app = FastAPI()
        app.add_middleware(PrometheusMetricsMiddleware)

        @app.get("/variable-speed")
        async def variable_speed():
            import asyncio

            # Variable response times
            delay = 0.05 + (hash(time.time()) % 100) / 10000  # 50-150ms
            await asyncio.sleep(delay)
            return {"delay": delay}

        client = TestClient(app)

        # Generate requests with variable timing
        for _ in range(20):
            response = client.get("/variable-speed")
            assert response.status_code == 200

        metrics_content = get_metrics_response().body.decode()

        # Should have histogram buckets for percentile calculation
        assert "http_request_duration_seconds_bucket" in metrics_content
        assert "le=" in metrics_content  # Histogram bucket labels


class TestIntegration:
    """Test integration between monitoring components."""

    def test_metrics_and_tracing_integration(self):
        """Test that metrics and tracing work together."""
        app = FastAPI()
        app.add_middleware(PrometheusMetricsMiddleware)

        with patch("src.monitoring.tracing.initialize_tracing"), patch(
            "src.monitoring.tracing.instrument_all"
        ):

            @app.get("/integrated")
            async def integrated_endpoint():
                # Record custom metrics within traced request
                record_user_registration()
                record_cache_hit("test_cache")
                return {"message": "integrated"}

            client = TestClient(app)
            response = client.get("/integrated")

            assert response.status_code == 200

            metrics_content = get_metrics_response().body.decode()

            # Should have both HTTP and custom metrics
            assert "http_requests_total" in metrics_content
            assert "user_registrations_total" in metrics_content
            assert "cache_hits_total" in metrics_content

    def test_error_handling_in_monitoring(self):
        """Test that monitoring handles errors gracefully."""
        app = FastAPI()
        app.add_middleware(PrometheusMetricsMiddleware)

        @app.get("/error")
        async def error_endpoint():
            raise ValueError("Test error")

        client = TestClient(app)

        # Should handle error gracefully
        response = client.get("/error")
        assert response.status_code == 500

        # Metrics should still be recorded
        metrics_content = get_metrics_response().body.decode()
        assert "http_requests_total" in metrics_content
        assert 'status="500"' in metrics_content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
