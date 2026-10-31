"""
Custom Log Handlers

Specialized handlers for Elasticsearch integration,
file rotation, and performance optimization.
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Dict, List, Optional


class ElasticsearchHandler(logging.Handler):
    """
    Elasticsearch log handler with buffering and async processing.

    Features:
    - Buffered writes for performance
    - Automatic index rotation (daily/monthly)
    - Connection failure resilience
    - Batch processing for efficiency
    """

    def __init__(
        self,
        host: str,
        index_prefix: str = "ecap-logs",
        buffer_size: int = 100,
        flush_interval: float = 5.0,
        index_rotation: str = "daily",  # daily, monthly, none
        timeout: float = 10.0,
    ):
        super().__init__()
        self.host = host
        self.index_prefix = index_prefix
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.index_rotation = index_rotation
        self.timeout = timeout

        # Internal state
        self.buffer: List[Dict[str, Any]] = []
        self.buffer_lock = threading.Lock()
        self.last_flush = time.time()
        self.es_client = None

        # Background flushing thread
        self.flush_thread = None
        self.stop_event = threading.Event()

        # Initialize Elasticsearch client
        self._init_elasticsearch()

        # Start background thread
        self._start_flush_thread()

    def _init_elasticsearch(self):
        """Initialize Elasticsearch client."""
        try:
            from elasticsearch.exceptions import ConnectionError, RequestError

            from elasticsearch import Elasticsearch

            self.es_client = Elasticsearch(
                [self.host], timeout=self.timeout, retry_on_timeout=True, max_retries=3
            )

            # Test connection
            self.es_client.ping()

        except ImportError:
            self.handleError(None)
            print(
                "Warning: Elasticsearch dependencies not installed. Install with: pip install elasticsearch"
            )
        except Exception as e:
            self.handleError(None)
            print(f"Warning: Failed to connect to Elasticsearch at {self.host}: {e}")

    def _start_flush_thread(self):
        """Start background thread for periodic flushing."""
        self.flush_thread = threading.Thread(target=self._flush_worker, daemon=True)
        self.flush_thread.start()

    def _flush_worker(self):
        """Background worker for periodic buffer flushing."""
        while not self.stop_event.is_set():
            try:
                self.stop_event.wait(self.flush_interval)
                if not self.stop_event.is_set():
                    self._flush_buffer()
            except Exception:
                pass  # Continue running even if flush fails

    def emit(self, record: logging.LogRecord):
        """Add log record to buffer."""
        if not self.es_client:
            return

        try:
            # Format record
            doc = self._format_document(record)

            with self.buffer_lock:
                self.buffer.append(doc)
                buffer_full = len(self.buffer) >= self.buffer_size

            # Flush outside the lock: _flush_buffer acquires it again
            if buffer_full:
                self._flush_buffer()

        except Exception:
            self.handleError(record)

    def _format_document(self, record: logging.LogRecord) -> Dict[str, Any]:
        """Format log record as Elasticsearch document."""
        # Parse JSON message if it's already formatted
        try:
            if hasattr(record, "getMessage"):
                message = record.getMessage()
                if message.startswith("{") and message.endswith("}"):
                    doc = json.loads(message)
                    if isinstance(doc, dict):
                        return doc
        except (json.JSONDecodeError, AttributeError):
            pass

        # Create document from scratch
        doc = {
            "@timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": {
                "name": getattr(record, "service_name", "ecap-service"),
                "version": getattr(record, "service_version", "1.0.0"),
                "environment": getattr(record, "environment", "development"),
            },
            "process": {"pid": record.process},
            "thread": {"id": record.thread},
        }

        # Add correlation ID
        if hasattr(record, "correlation_id") and record.correlation_id:
            doc["correlation_id"] = record.correlation_id

        # Add file location
        if record.filename and record.lineno:
            doc["file"] = {
                "name": record.filename,
                "line": record.lineno,
                "function": record.funcName,
            }

        # Add exception info
        if record.exc_info:
            doc["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.format(record),
            }

        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in [
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
            ] and not key.startswith("_"):
                doc[key] = value

        return doc

    def _get_index_name(self) -> str:
        """Get index name with rotation."""
        if self.index_rotation == "daily":
            suffix = datetime.now().strftime("%Y.%m.%d")
        elif self.index_rotation == "monthly":
            suffix = datetime.now().strftime("%Y.%m")
        else:
            suffix = ""

        return f"{self.index_prefix}-{suffix}" if suffix else self.index_prefix

    def _flush_buffer(self):
        """Flush buffered records to Elasticsearch."""
        if not self.es_client or not self.buffer:
            return

        with self.buffer_lock:
            if not self.buffer:
                return

            docs = self.buffer.copy()
            self.buffer.clear()

        try:
            # Prepare bulk request
            bulk_body = []
            index_name = self._get_index_name()

            for doc in docs:
                bulk_body.append({"index": {"_index": index_name}})
                bulk_body.append(doc)

            # Send bulk request
            if bulk_body:
                response = self.es_client.bulk(body=bulk_body, timeout=self.timeout)

                # Check for errors
                if response.get("errors"):
                    error_count = sum(
                        1
                        for item in response["items"]
                        if "index" in item and "error" in item["index"]
                    )
                    print(f"Elasticsearch bulk insert had {error_count} errors")

        except Exception as e:
            print(f"Failed to flush logs to Elasticsearch: {e}")
            # Re-add docs to buffer if connection failed
            with self.buffer_lock:
                self.buffer.extend(docs[:50])  # Keep only recent ones

    def flush(self):
        """Force flush of buffer."""
        self._flush_buffer()

    def close(self):
        """Clean shutdown of handler."""
        # Stop background thread
        if self.flush_thread:
            self.stop_event.set()
            self.flush_thread.join(timeout=5.0)

        # Final flush
        self._flush_buffer()

        super().close()


class FileRotatingHandler(RotatingFileHandler):
    """Enhanced rotating file handler with compression and cleanup."""

    def __init__(
        self,
        filename: str,
        max_bytes: int = 100 * 1024 * 1024,  # 100MB
        backup_count: int = 5,
        compress_rotated: bool = True,
        encoding: str = "utf-8",
    ):
        # Ensure directory exists
        log_path = Path(filename)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        super().__init__(
            filename=filename,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding=encoding,
        )

        self.compress_rotated = compress_rotated

    def doRollover(self):
        """Perform log rotation with optional compression."""
        super().doRollover()

        if self.compress_rotated:
            self._compress_old_logs()

    def _compress_old_logs(self):
        """Compress rotated log files."""
        try:
            import gzip
            import shutil

            base_filename = self.baseFilename

            # Compress .1, .2, etc. files
            for i in range(1, self.backupCount + 1):
                rotated_file = f"{base_filename}.{i}"
                compressed_file = f"{rotated_file}.gz"

                if Path(rotated_file).exists() and not Path(compressed_file).exists():
                    with open(rotated_file, "rb") as f_in:
                        with gzip.open(compressed_file, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)

                    # Remove uncompressed file
                    Path(rotated_file).unlink()

        except Exception:
            pass  # Don't fail if compression fails


class AsyncQueueHandler(logging.Handler):
    """
    Asynchronous logging handler using a queue.

    Prevents logging from blocking application threads.
    """

    def __init__(self, handler: logging.Handler, queue_size: int = 1000):
        super().__init__()
        self.handler = handler
        self.queue = Queue(maxsize=queue_size)
        self.worker_thread = None
        self.stop_event = threading.Event()

        # Start worker thread
        self._start_worker()

    def _start_worker(self):
        """Start background worker thread."""
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()

    def _worker(self):
        """Background worker to process queued records."""
        while not self.stop_event.is_set():
            try:
                # Get record with timeout
                record = self.queue.get(timeout=1.0)
                if record is None:  # Sentinel value
                    break

                # Process record
                self.handler.emit(record)
                self.queue.task_done()

            except Empty:
                continue  # Timeout, check stop event
            except Exception:
                pass  # Continue processing other records

    def emit(self, record: logging.LogRecord):
        """Add record to queue for async processing."""
        try:
            if not self.stop_event.is_set():
                self.queue.put_nowait(record)
        except:
            # Queue full or handler stopped
            pass

    def flush(self):
        """Wait for queue to be processed."""
        self.queue.join()
        self.handler.flush()

    def close(self):
        """Clean shutdown."""
        # Signal worker to stop
        self.stop_event.set()

        # Add sentinel value
        try:
            self.queue.put_nowait(None)
        except:
            pass

        # Wait for worker to finish
        if self.worker_thread:
            self.worker_thread.join(timeout=5.0)

        # Close underlying handler
        self.handler.close()

        super().close()


class MetricsHandler(logging.Handler):
    """Handler that exports logging metrics to Prometheus."""

    def __init__(self):
        super().__init__()
        self.counters = {"debug": 0, "info": 0, "warning": 0, "error": 0, "critical": 0}
        self.lock = threading.Lock()

    def emit(self, record: logging.LogRecord):
        """Count log messages by level."""
        level = record.levelname.lower()

        with self.lock:
            if level in self.counters:
                self.counters[level] += 1

    def get_metrics(self) -> Dict[str, int]:
        """Get current metrics."""
        with self.lock:
            return self.counters.copy()

    def reset_metrics(self):
        """Reset all counters."""
        with self.lock:
            for key in self.counters:
                self.counters[key] = 0
