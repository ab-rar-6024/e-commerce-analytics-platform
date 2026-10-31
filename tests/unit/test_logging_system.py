"""
Comprehensive test suite for ECAP logging system.

Tests structured logging, correlation tracking, formatters,
handlers, and middleware integration.
"""

import json
import logging
import sys
import tempfile
import time
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from src.custom_logging import (
    AsyncQueueHandler,
    CorrelationContext,
    ECSFormatter,
    ElasticsearchHandler,
    FileRotatingHandler,
    JSONFormatter,
    LogConfig,
    StructuredLogger,
    clear_correlation_id,
    get_correlation_id,
    get_logger,
    set_correlation_id,
    setup_logging,
)


class TestLogConfig:
    """Test LogConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = LogConfig()

        assert config.level == "INFO"
        assert config.format_type == "json"
        assert config.output_destination == "console"
        assert config.service_name == "ecap-service"
        assert config.environment == "development"
        assert config.async_logging is True

    def test_custom_config(self):
        """Test custom configuration values."""
        config = LogConfig(
            level="DEBUG",
            service_name="test-service",
            environment="testing",
            elasticsearch_host="http://test-es:9200",
        )

        assert config.level == "DEBUG"
        assert config.service_name == "test-service"
        assert config.environment == "testing"
        assert config.elasticsearch_host == "http://test-es:9200"


class TestStructuredLogger:
    """Test StructuredLogger functionality."""

    @pytest.fixture
    def temp_log_file(self):
        """Create temporary log file for testing."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".log") as f:
            yield f.name
        # Windows cannot delete a file that a logging handler still has open
        std_logger = logging.getLogger("test.logger")
        for handler in list(std_logger.handlers):
            handler.close()
            std_logger.removeHandler(handler)
        Path(f.name).unlink(missing_ok=True)

    @pytest.fixture
    def test_config(self, temp_log_file):
        """Create test configuration."""
        return LogConfig(
            level="DEBUG",
            format_type="json",
            output_destination="file",
            log_file_path=temp_log_file,
            service_name="test-service",
            environment="testing",
        )

    def test_logger_creation(self, test_config):
        """Test logger creation and basic functionality."""
        logger = StructuredLogger("test.logger", test_config)

        assert logger.name == "test.logger"
        assert logger.config == test_config
        assert logger.logger is not None

    def test_log_levels(self, test_config, temp_log_file):
        """Test different log levels."""
        logger = StructuredLogger("test.logger", test_config)

        # Test all log levels
        logger.debug("Debug message", debug_data="test")
        logger.info("Info message", info_data="test")
        logger.warning("Warning message", warning_data="test")
        logger.error("Error message", error_data="test")
        logger.critical("Critical message", critical_data="test")

        # Read log file and verify entries
        with open(temp_log_file, "r") as f:
            log_entries = [json.loads(line.strip()) for line in f if line.strip()]

        assert len(log_entries) == 5

        levels = [entry["level"] for entry in log_entries]
        assert "DEBUG" in levels
        assert "INFO" in levels
        assert "WARNING" in levels
        assert "ERROR" in levels
        assert "CRITICAL" in levels

    def test_request_logging(self, test_config, temp_log_file):
        """Test HTTP request logging."""
        logger = StructuredLogger("test.logger", test_config)

        logger.log_request(
            method="GET",
            path="/api/users",
            status_code=200,
            duration_ms=125.5,
            user_id="test-user",
        )

        with open(temp_log_file, "r") as f:
            log_entry = json.loads(f.read().strip())

        assert log_entry["level"] == "INFO"
        assert "GET /api/users 200 125.50ms" in log_entry["message"]
        assert log_entry["http"]["method"] == "GET"
        assert log_entry["http"]["status_code"] == 200
        assert log_entry["http"]["duration_ms"] == 125.5
        assert log_entry["user_id"] == "test-user"

    def test_database_logging(self, test_config, temp_log_file):
        """Test database operation logging."""
        logger = StructuredLogger("test.logger", test_config)

        logger.log_database_query(
            query="SELECT * FROM users WHERE active = true",
            duration_ms=45.2,
            rows_affected=10,
            table="users",
        )

        with open(temp_log_file, "r") as f:
            log_entry = json.loads(f.read().strip())

        assert log_entry["level"] == "INFO"
        assert "Database query completed" in log_entry["message"]
        assert log_entry["database"]["duration_ms"] == 45.2
        assert log_entry["database"]["rows_affected"] == 10
        assert log_entry["table"] == "users"

    def test_kafka_logging(self, test_config, temp_log_file):
        """Test Kafka event logging."""
        logger = StructuredLogger("test.logger", test_config)

        logger.log_kafka_event(
            topic="test-topic",
            partition=0,
            offset=12345,
            event_type="producer_send",
            message_size=256,
        )

        with open(temp_log_file, "r") as f:
            log_entry = json.loads(f.read().strip())

        assert log_entry["level"] == "INFO"
        assert "Kafka producer_send: test-topic[0]@12345" in log_entry["message"]
        assert log_entry["kafka"]["topic"] == "test-topic"
        assert log_entry["kafka"]["partition"] == 0
        assert log_entry["kafka"]["offset"] == 12345
        assert log_entry["message_size"] == 256


class TestCorrelationTracking:
    """Test correlation ID and request tracing."""

    def test_correlation_context_manager(self):
        """Test correlation context manager."""
        test_id = "test-correlation-123"

        # Ensure clean state
        clear_correlation_id()
        assert get_correlation_id() is None

        # Test context manager
        with CorrelationContext(test_id):
            assert get_correlation_id() == test_id

        # Should be cleared after exiting context
        assert get_correlation_id() is None

    def test_nested_correlation_contexts(self):
        """Test nested correlation contexts."""
        parent_id = "parent-123"
        child_id = "child-456"

        clear_correlation_id()

        with CorrelationContext(parent_id):
            assert get_correlation_id() == parent_id

            with CorrelationContext(child_id):
                assert get_correlation_id() == child_id

            # Should restore parent context
            assert get_correlation_id() == parent_id

    def test_correlation_inheritance(self):
        """Test correlation ID inheritance in child spans."""
        parent_id = "parent-123"

        with CorrelationContext(parent_id) as parent_ctx:
            child_ctx = parent_ctx.create_child_span("child_operation")

            with child_ctx:
                # Should inherit parent's correlation ID
                assert get_correlation_id() == parent_id
                assert child_ctx.correlation_id == parent_id
                assert child_ctx.operation_name == "child_operation"

    def test_trace_headers(self):
        """Test trace header generation."""
        correlation_id = "test-123"

        with CorrelationContext(correlation_id) as ctx:
            headers = ctx.get_trace_headers()

            assert headers["X-Correlation-ID"] == correlation_id
            assert "X-Trace-ID" in headers
            assert "X-Span-ID" in headers

    def test_duration_tracking(self):
        """Test duration tracking in correlation context."""
        with CorrelationContext() as ctx:
            time.sleep(0.1)  # Small delay
            duration = ctx.get_duration_ms()

            assert duration >= 100  # At least 100ms
            assert duration < 200  # But not too much more


class TestJSONFormatter:
    """Test JSON log formatter."""

    def test_basic_formatting(self):
        """Test basic JSON formatting."""
        formatter = JSONFormatter(
            service_name="test-service", service_version="1.0.0", environment="testing"
        )

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        formatted = formatter.format(record)
        parsed = json.loads(formatted)

        assert parsed["level"] == "INFO"
        assert parsed["message"] == "Test message"
        assert parsed["service"]["name"] == "test-service"
        assert parsed["service"]["version"] == "1.0.0"
        assert parsed["service"]["environment"] == "testing"
        assert parsed["file"]["name"] == "test.py"
        assert parsed["file"]["line"] == 42

    def test_correlation_id_inclusion(self):
        """Test correlation ID inclusion in formatted logs."""
        formatter = JSONFormatter()

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        # Add correlation ID to record
        record.correlation_id = "test-correlation-123"

        formatted = formatter.format(record)
        parsed = json.loads(formatted)

        assert parsed["correlation_id"] == "test-correlation-123"

    def test_exception_formatting(self):
        """Test exception information formatting."""
        formatter = JSONFormatter()

        try:
            raise ValueError("Test exception")
        except ValueError:
            record = logging.LogRecord(
                name="test.logger",
                level=logging.ERROR,
                pathname="test.py",
                lineno=42,
                msg="Error occurred",
                args=(),
                exc_info=sys.exc_info(),  # Capture the exception while it is active
            )

        formatted = formatter.format(record)
        parsed = json.loads(formatted)

        assert parsed["level"] == "ERROR"
        assert "exception" in parsed
        assert parsed["exception"]["type"] == "ValueError"
        assert parsed["exception"]["message"] == "Test exception"
        assert "traceback" in parsed["exception"]


class TestECSFormatter:
    """Test Elastic Common Schema formatter."""

    def test_ecs_structure(self):
        """Test ECS-compliant structure."""
        formatter = ECSFormatter(service_name="test-service", environment="testing")

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        formatted = formatter.format(record)
        parsed = json.loads(formatted)

        # Check ECS required fields
        assert "@timestamp" in parsed
        assert parsed["ecs"]["version"] == "8.6.0"
        assert parsed["log"]["level"] == "info"
        assert parsed["message"] == "Test message"
        assert parsed["service"]["name"] == "test-service"
        assert parsed["service"]["environment"] == "testing"

    def test_http_context_mapping(self):
        """Test HTTP context mapping to ECS."""
        formatter = ECSFormatter()

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="HTTP request",
            args=(),
            exc_info=None,
        )

        # Add HTTP context
        record.http = {
            "method": "GET",
            "path": "/api/users",
            "status_code": 200,
            "duration_ms": 125.5,
        }

        formatted = formatter.format(record)
        parsed = json.loads(formatted)

        assert parsed["http"]["request"]["method"] == "GET"
        assert parsed["http"]["request"]["path"] == "/api/users"
        assert parsed["http"]["response"]["status_code"] == 200
        assert parsed["event"]["duration"] == 125500000  # nanoseconds


class TestLogHandlers:
    """Test custom log handlers."""

    @pytest.fixture
    def mock_elasticsearch(self):
        """Mock Elasticsearch client."""
        with patch("elasticsearch.Elasticsearch") as mock_es:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_client.bulk.return_value = {"errors": False, "items": []}
            mock_es.return_value = mock_client
            yield mock_client

    def test_elasticsearch_handler_creation(self, mock_elasticsearch):
        """Test Elasticsearch handler creation."""
        handler = ElasticsearchHandler(
            host="http://test-es:9200", index_prefix="test-logs"
        )

        assert handler.host == "http://test-es:9200"
        assert handler.index_prefix == "test-logs"
        assert handler.es_client is not None

    def test_elasticsearch_handler_emit(self, mock_elasticsearch):
        """Test Elasticsearch handler log emission."""
        handler = ElasticsearchHandler(
            host="http://test-es:9200", buffer_size=1  # Force immediate flush
        )

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        handler.emit(record)

        # Should have called bulk insert
        assert mock_elasticsearch.bulk.called

    def test_file_rotating_handler(self):
        """Test file rotating handler."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as f:
            temp_path = f.name

        try:
            handler = FileRotatingHandler(
                filename=temp_path, max_bytes=1024, backup_count=3
            )

            # Write some data
            record = logging.LogRecord(
                name="test.logger",
                level=logging.INFO,
                pathname="test.py",
                lineno=42,
                msg="Test message",
                args=(),
                exc_info=None,
            )

            handler.emit(record)
            handler.close()

            # Verify file was created
            assert Path(temp_path).exists()

        finally:
            # Cleanup
            Path(temp_path).unlink(missing_ok=True)

    def test_async_queue_handler(self):
        """Test asynchronous queue handler."""
        # Create a mock handler to wrap
        mock_handler = Mock(spec=logging.Handler)

        async_handler = AsyncQueueHandler(mock_handler, queue_size=10)

        # Emit a log record
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        async_handler.emit(record)

        # Give worker thread time to process
        time.sleep(0.1)

        # Verify the mock handler was called
        mock_handler.emit.assert_called_once_with(record)

        # Cleanup
        async_handler.close()


class TestLoggerIntegration:
    """Test complete logger integration."""

    def test_global_logger_setup(self):
        """Test global logger setup and retrieval."""
        config = LogConfig(service_name="integration-test", environment="testing")

        setup_logging(config)

        # Get logger instances
        logger1 = get_logger("test.module1")
        logger2 = get_logger("test.module2")
        logger1_again = get_logger("test.module1")

        # Should reuse existing logger
        assert logger1 is logger1_again
        assert logger1 is not logger2

        # Both should have the same config
        assert logger1.config.service_name == "integration-test"
        assert logger2.config.service_name == "integration-test"

    def test_configuration_from_settings(self):
        """Test configuration creation from settings dict."""
        from src.custom_logging.structured_logger import configure_from_settings

        settings = {
            "app": {"name": "test-app"},
            "environment": "testing",
            "logging": {
                "level": "DEBUG",
                "format": "ecs",
                "output": "file",
                "file_path": "/tmp/test.log",
                "elasticsearch": {"host": "http://test-es:9200", "index": "test-logs"},
            },
        }

        config = configure_from_settings(settings)

        assert config.level == "DEBUG"
        assert config.format_type == "ecs"
        assert config.output_destination == "file"
        assert config.log_file_path == "/tmp/test.log"
        assert config.service_name == "test-app"
        assert config.environment == "testing"
        assert config.elasticsearch_host == "http://test-es:9200"
        assert config.elasticsearch_index == "test-logs"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
