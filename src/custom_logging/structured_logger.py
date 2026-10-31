"""
Structured Logger Implementation

Provides JSON-structured logging with correlation tracking,
performance metrics, and ELK stack integration.
"""

import json
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Union

from loguru import logger

from .correlation import get_correlation_id
from .formatters import ECSFormatter, JSONFormatter


@dataclass
class LogConfig:
    """Logging configuration with sensible defaults."""

    level: str = "INFO"
    format_type: str = "json"  # json, ecs, text
    output_destination: str = "console"  # console, file, both

    # File logging settings
    log_file_path: Optional[str] = None
    max_file_size: str = "100MB"
    backup_count: int = 5

    # Performance settings
    async_logging: bool = True
    buffer_size: int = 1000

    # Service context
    service_name: str = "ecap-service"
    service_version: str = "1.0.0"
    environment: str = "development"

    # ELK integration
    elasticsearch_host: Optional[str] = None
    elasticsearch_index: str = "ecap-logs"

    # Additional context
    extra_fields: Dict[str, Any] = field(default_factory=dict)


class StructuredLogger:
    """Enhanced logger with structured output and correlation tracking."""

    def __init__(self, name: str, config: LogConfig):
        self.name = name
        self.config = config
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Configure structured logger with appropriate handlers."""
        # Remove default loguru handlers
        logger.remove()

        # Create standard library logger for integration
        std_logger = logging.getLogger(self.name)
        std_logger.setLevel(getattr(logging, self.config.level.upper()))

        # Clear existing handlers
        std_logger.handlers.clear()
        std_logger.propagate = False

        # Configure formatters
        if self.config.format_type == "ecs":
            formatter = ECSFormatter(
                service_name=self.config.service_name,
                service_version=self.config.service_version,
                environment=self.config.environment,
            )
        else:
            formatter = JSONFormatter(
                service_name=self.config.service_name,
                service_version=self.config.service_version,
                environment=self.config.environment,
            )

        # Console handler
        if self.config.output_destination in ["console", "both"]:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            std_logger.addHandler(console_handler)

        # File handler
        if (
            self.config.output_destination in ["file", "both"]
            and self.config.log_file_path
        ):
            from logging.handlers import RotatingFileHandler

            log_path = Path(self.config.log_file_path)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = RotatingFileHandler(
                filename=log_path,
                maxBytes=self._parse_size(self.config.max_file_size),
                backupCount=self.config.backup_count,
            )
            file_handler.setFormatter(formatter)
            std_logger.addHandler(file_handler)

        # Elasticsearch handler (if configured)
        if self.config.elasticsearch_host:
            try:
                from .handlers import ElasticsearchHandler

                es_handler = ElasticsearchHandler(
                    host=self.config.elasticsearch_host,
                    index=self.config.elasticsearch_index,
                )
                es_handler.setFormatter(formatter)
                std_logger.addHandler(es_handler)
            except ImportError:
                std_logger.warning("Elasticsearch dependencies not available")

        return std_logger

    def _parse_size(self, size_str: str) -> int:
        """Parse size string like '100MB' or '100M' to bytes."""
        units = {
            "GB": 1024**3,
            "MB": 1024**2,
            "KB": 1024,
            "G": 1024**3,
            "M": 1024**2,
            "K": 1024,
            "B": 1,
        }
        size_str = size_str.strip().upper()

        # Longest suffixes first so '100MB' is not read as '100M' + 'B'
        for unit, multiplier in units.items():
            if size_str.endswith(unit):
                return int(size_str[: -len(unit)]) * multiplier

        return int(size_str)  # Assume bytes if no unit

    def _enrich_record(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Enrich log record with correlation and context data."""
        enriched = {
            "correlation_id": get_correlation_id(),
            "service_name": self.config.service_name,
            "service_version": self.config.service_version,
            "environment": self.config.environment,
            "timestamp": time.time(),
            **self.config.extra_fields,
        }

        if extra:
            enriched.update(extra)

        return enriched

    def debug(self, message: str, **kwargs):
        """Log debug message with structured context."""
        exc_info = kwargs.pop("exc_info", None)
        extra = self._enrich_record(kwargs)
        self.logger.debug(message, extra=extra, exc_info=exc_info)

    def info(self, message: str, **kwargs):
        """Log info message with structured context."""
        exc_info = kwargs.pop("exc_info", None)
        extra = self._enrich_record(kwargs)
        self.logger.info(message, extra=extra, exc_info=exc_info)

    def warning(self, message: str, **kwargs):
        """Log warning message with structured context."""
        exc_info = kwargs.pop("exc_info", None)
        extra = self._enrich_record(kwargs)
        self.logger.warning(message, extra=extra, exc_info=exc_info)

    def error(self, message: str, **kwargs):
        """Log error message with structured context."""
        exc_info = kwargs.pop("exc_info", None)
        extra = self._enrich_record(kwargs)
        self.logger.error(message, extra=extra, exc_info=exc_info)

    def critical(self, message: str, **kwargs):
        """Log critical message with structured context."""
        exc_info = kwargs.pop("exc_info", None)
        extra = self._enrich_record(kwargs)
        self.logger.critical(message, extra=extra, exc_info=exc_info)

    def exception(self, message: str, **kwargs):
        """Log exception with full traceback."""
        exc_info = kwargs.pop("exc_info", True)
        extra = self._enrich_record(kwargs)
        self.logger.exception(message, extra=extra, exc_info=exc_info)

    def log_request(
        self, method: str, path: str, status_code: int, duration_ms: float, **kwargs
    ):
        """Log HTTP request with structured format."""
        fields = {
            "http": {
                "method": method,
                "path": path,
                "status_code": status_code,
                "duration_ms": duration_ms,
            },
            **kwargs,
        }

        level = "error" if status_code >= 400 else "info"
        getattr(self, level)(
            f"{method} {path} {status_code} {duration_ms:.2f}ms", **fields
        )

    def log_database_query(
        self,
        query: str,
        duration_ms: float,
        rows_affected: Optional[int] = None,
        **kwargs,
    ):
        """Log database query with performance metrics."""
        fields = {
            "database": {
                "query": query[:200] + "..." if len(query) > 200 else query,
                "duration_ms": duration_ms,
                "rows_affected": rows_affected,
            },
            **kwargs,
        }

        level = "warning" if duration_ms > 1000 else "info"
        getattr(self, level)(
            f"Database query completed in {duration_ms:.2f}ms", **fields
        )

    def log_kafka_event(
        self, topic: str, partition: int, offset: int, event_type: str, **kwargs
    ):
        """Log Kafka producer/consumer events."""
        fields = {
            "kafka": {
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "event_type": event_type,
            },
            **kwargs,
        }

        self.info(f"Kafka {event_type}: {topic}[{partition}]@{offset}", **fields)


# Global logger registry
_loggers: Dict[str, StructuredLogger] = {}
_global_config: Optional[LogConfig] = None


def setup_logging(config: LogConfig) -> None:
    """Setup global logging configuration."""
    global _global_config
    _global_config = config


def get_logger(name: str, config: Optional[LogConfig] = None) -> StructuredLogger:
    """Get or create a structured logger instance."""
    if name not in _loggers:
        log_config = config or _global_config or LogConfig()
        _loggers[name] = StructuredLogger(name, log_config)

    return _loggers[name]


def configure_from_settings(settings: Dict[str, Any]) -> LogConfig:
    """Create LogConfig from application settings."""
    logging_settings = settings.get("logging", {})

    return LogConfig(
        level=logging_settings.get("level", "INFO"),
        format_type=logging_settings.get("format", "json"),
        output_destination=logging_settings.get("output", "console"),
        log_file_path=logging_settings.get("file_path"),
        max_file_size=logging_settings.get("max_file_size", "100MB"),
        backup_count=logging_settings.get("backup_count", 5),
        service_name=settings.get("app", {}).get("name", "ecap-service"),
        environment=settings.get("environment", "development"),
        elasticsearch_host=logging_settings.get("elasticsearch", {}).get("host"),
        elasticsearch_index=logging_settings.get("elasticsearch", {}).get(
            "index", "ecap-logs"
        ),
    )
