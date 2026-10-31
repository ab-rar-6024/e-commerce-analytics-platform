"""
Log Formatters

Provides JSON and ECS (Elastic Common Schema) formatters
for structured logging output.
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def __init__(
        self,
        service_name: str = "ecap-service",
        service_version: str = "1.0.0",
        environment: str = "development",
    ):
        super().__init__()
        self.service_name = service_name
        self.service_version = service_version
        self.environment = environment

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        # Base log structure
        log_entry = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": {
                "name": self.service_name,
                "version": self.service_version,
                "environment": self.environment,
            },
            "process": {
                "pid": record.process,
                "name": record.processName if hasattr(record, "processName") else None,
            },
            "thread": {
                "id": record.thread,
                "name": record.threadName if hasattr(record, "threadName") else None,
            },
        }

        # Add correlation ID if available
        if hasattr(record, "correlation_id") and record.correlation_id:
            log_entry["correlation_id"] = record.correlation_id

        # Add trace information if available
        if hasattr(record, "trace_id") and record.trace_id:
            log_entry["trace"] = {
                "id": record.trace_id,
                "span_id": getattr(record, "span_id", None),
                "parent_span_id": getattr(record, "parent_span_id", None),
            }

        # Add file location
        if record.filename and record.lineno:
            log_entry["file"] = {
                "name": record.filename,
                "line": record.lineno,
                "function": record.funcName,
            }

        # Add exception information
        exc_info = record.exc_info
        if exc_info is True:
            # LogRecord built directly (not via Logger._log) keeps the raw flag
            exc_info = sys.exc_info()
        if exc_info and exc_info[0]:
            log_entry["exception"] = {
                "type": exc_info[0].__name__,
                "message": str(exc_info[1]),
                "traceback": self.formatException(exc_info),
            }

        # Add extra fields from record
        extra_fields = self._extract_extra_fields(record)
        if extra_fields:
            log_entry.update(extra_fields)

        return json.dumps(log_entry, default=str, ensure_ascii=False)

    def _extract_extra_fields(self, record: logging.LogRecord) -> Dict[str, Any]:
        """Extract extra fields from log record."""
        # Standard fields to exclude
        standard_fields = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
        }

        extra = {}
        for key, value in record.__dict__.items():
            if key not in standard_fields and not key.startswith("_"):
                extra[key] = value

        return extra


class ECSFormatter(logging.Formatter):
    """Elastic Common Schema (ECS) formatter."""

    def __init__(
        self,
        service_name: str = "ecap-service",
        service_version: str = "1.0.0",
        environment: str = "development",
    ):
        super().__init__()
        self.service_name = service_name
        self.service_version = service_version
        self.environment = environment

    def format(self, record: logging.LogRecord) -> str:
        """Format log record according to ECS specification."""
        # ECS base structure
        ecs_entry = {
            "@timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "ecs": {"version": "8.6.0"},
            "log": {"level": record.levelname.lower(), "logger": record.name},
            "message": record.getMessage(),
            "service": {
                "name": self.service_name,
                "version": self.service_version,
                "environment": self.environment,
            },
            "process": {
                "pid": record.process,
                "name": getattr(record, "processName", None),
            },
        }

        # Add labels for correlation
        labels = {}
        if hasattr(record, "correlation_id") and record.correlation_id:
            labels["correlation_id"] = record.correlation_id

        if hasattr(record, "environment"):
            labels["environment"] = record.environment

        if labels:
            ecs_entry["labels"] = labels

        # Add tracing information
        if hasattr(record, "trace_id") and record.trace_id:
            ecs_entry["trace"] = {"id": record.trace_id}

            if hasattr(record, "span_id"):
                ecs_entry["span"] = {"id": record.span_id}

        # Add error information
        if record.exc_info and record.exc_info[0]:
            ecs_entry["error"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "stack_trace": self.formatException(record.exc_info),
            }

        # Add HTTP context if available
        if hasattr(record, "http"):
            http_data = record.http
            ecs_entry["http"] = {
                "request": {
                    "method": http_data.get("method"),
                    "path": http_data.get("path"),
                },
                "response": {"status_code": http_data.get("status_code")},
            }

            if "duration_ms" in http_data:
                ecs_entry["event"] = {
                    "duration": http_data["duration_ms"]
                    * 1000000  # Convert to nanoseconds
                }

        # Add database context if available
        if hasattr(record, "database"):
            db_data = record.database
            ecs_entry["database"] = {
                "statement": db_data.get("query"),
                "duration": db_data.get("duration_ms"),
            }

        # Add file location
        if record.filename and record.lineno:
            log_dict = ecs_entry["log"]
            assert isinstance(log_dict, dict)
            log_dict["origin"] = {
                "file": {"name": record.filename, "line": record.lineno},
                "function": record.funcName,
            }

        # Add custom fields with proper ECS mapping
        extra_fields = self._extract_extra_fields(record)
        if extra_fields:
            # Map to ECS fields where possible, otherwise add as custom
            for key, value in extra_fields.items():
                if key.startswith("user_"):
                    if "user" not in ecs_entry:
                        ecs_entry["user"] = {}
                    user_dict = ecs_entry["user"]
                    assert isinstance(user_dict, dict)
                    user_dict[key[5:]] = value  # Remove 'user_' prefix
                elif key.startswith("kafka_"):
                    if "kafka" not in ecs_entry:
                        ecs_entry["kafka"] = {}
                    kafka_dict = ecs_entry["kafka"]
                    assert isinstance(kafka_dict, dict)
                    kafka_dict[key[6:]] = value  # Remove 'kafka_' prefix
                else:
                    # Add as custom field
                    if "custom" not in ecs_entry:
                        ecs_entry["custom"] = {}
                    custom_dict = ecs_entry["custom"]
                    assert isinstance(custom_dict, dict)
                    custom_dict[key] = value

        return json.dumps(ecs_entry, default=str, ensure_ascii=False)

    def _extract_extra_fields(self, record: logging.LogRecord) -> Dict[str, Any]:
        """Extract extra fields from log record."""
        standard_fields = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "correlation_id",
            "trace_id",
            "span_id",
            "http",
            "database",
        }

        extra = {}
        for key, value in record.__dict__.items():
            if key not in standard_fields and not key.startswith("_"):
                extra[key] = value

        return extra


class CompactJSONFormatter(JSONFormatter):
    """Compact JSON formatter for high-volume logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as compact JSON."""
        # Minimal log structure for performance
        log_entry = {
            "ts": int(record.created * 1000),  # Millisecond timestamp
            "lvl": record.levelname[0],  # Single letter level
            "msg": record.getMessage(),
            "svc": self.service_name,
        }

        # Add correlation ID if available
        if hasattr(record, "correlation_id") and record.correlation_id:
            log_entry["cid"] = record.correlation_id

        # Add exception info if present
        if record.exc_info and record.exc_info[0]:
            log_entry["err"] = record.exc_info[0].__name__

        # Add key extra fields only
        if hasattr(record, "http"):
            http = record.http
            log_entry[
                "http"
            ] = f"{http.get('method')} {http.get('path')} {http.get('status_code')}"

        return json.dumps(log_entry, separators=(",", ":"), ensure_ascii=False)
