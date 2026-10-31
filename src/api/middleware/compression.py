"""
Response compression middleware for the E-Commerce Analytics Platform API.

This module provides middleware for compressing API responses to improve
performance and reduce bandwidth usage.
"""

import gzip
import json
import logging
from io import BytesIO
from typing import Any, Dict, Set

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import StreamingResponse

from ..config import get_settings

logger = logging.getLogger(__name__)


class CompressionMiddleware(BaseHTTPMiddleware):
    """
    Middleware for compressing HTTP responses.

    Supports gzip compression with configurable minimum size threshold
    and content type filtering.
    """

    def __init__(
        self,
        app,
        minimum_size: int = 1024,
        compress_level: int = 6,
        excluded_media_types: Set[str] = None,
    ):
        """
        Initialize compression middleware.

        Args:
            app: FastAPI application
            minimum_size: Minimum response size in bytes to compress
            compress_level: Gzip compression level (1-9)
            excluded_media_types: Media types to exclude from compression
        """
        super().__init__(app)
        self.minimum_size = minimum_size
        self.compress_level = max(1, min(9, compress_level))

        # Default excluded media types (already compressed)
        self.excluded_media_types = excluded_media_types or {
            "image/jpeg",
            "image/png",
            "image/gif",
            "image/webp",
            "video/mp4",
            "video/mpeg",
            "audio/mpeg",
            "audio/ogg",
            "application/zip",
            "application/gzip",
            "application/x-gzip",
            "application/x-compress",
            "application/x-compressed",
        }

        settings = get_settings()
        logger.info(
            f"Compression middleware initialized: "
            f"min_size={minimum_size}, level={compress_level}, "
            f"excluded_types={len(self.excluded_media_types)}"
        )

    async def dispatch(self, request: Request, call_next):
        """
        Process request and compress response if applicable.

        Args:
            request: Incoming HTTP request
            call_next: Next middleware/handler in chain

        Returns:
            HTTP response (potentially compressed)
        """
        # Get response from next handler
        response = await call_next(request)

        # Check if compression should be applied
        if not self._should_compress(request, response):
            return response

        # Compress the response
        return await self._compress_response(response)

    def _should_compress(self, request: Request, response: Response) -> bool:
        """
        Determine if response should be compressed.

        Args:
            request: HTTP request
            response: HTTP response

        Returns:
            True if response should be compressed
        """
        # Check if client accepts gzip encoding
        accept_encoding = request.headers.get("accept-encoding", "")
        if "gzip" not in accept_encoding.lower():
            return False

        # Check if response is already compressed
        content_encoding = response.headers.get("content-encoding")
        if content_encoding:
            return False

        # Check content type
        content_type = response.headers.get("content-type", "").split(";")[0].strip()
        if content_type in self.excluded_media_types:
            return False

        # Check response size
        content_length = response.headers.get("content-length")
        if content_length and int(content_length) < self.minimum_size:
            return False

        return True

    async def _compress_response(self, response: Response) -> Response:
        """
        Compress response content using gzip.

        Args:
            response: Original response

        Returns:
            Compressed response
        """
        try:
            # Get response body
            body = b""
            async for chunk in response.body_iterator:
                body += chunk

            # Check minimum size after getting body
            if len(body) < self.minimum_size:
                # Return original response for small content
                return Response(
                    content=body,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=response.media_type,
                )

            # Compress the body
            compressed_body = self._gzip_compress(body)

            # Calculate compression ratio
            original_size = len(body)
            compressed_size = len(compressed_body)
            compression_ratio = (1 - compressed_size / original_size) * 100

            logger.debug(
                f"Compressed response: {original_size} -> {compressed_size} bytes "
                f"({compression_ratio:.1f}% reduction)"
            )

            # Create compressed response
            headers = dict(response.headers)
            headers["content-encoding"] = "gzip"
            headers["content-length"] = str(compressed_size)
            headers["vary"] = "Accept-Encoding"

            return Response(
                content=compressed_body,
                status_code=response.status_code,
                headers=headers,
                media_type=response.media_type,
            )

        except Exception as e:
            logger.error(f"Error compressing response: {e}")
            # Return original response on compression error
            return response

    def _gzip_compress(self, data: bytes) -> bytes:
        """
        Compress data using gzip.

        Args:
            data: Data to compress

        Returns:
            Compressed data
        """
        buffer = BytesIO()
        with gzip.GzipFile(
            fileobj=buffer, mode="wb", compresslevel=self.compress_level
        ) as gz_file:
            gz_file.write(data)
        return buffer.getvalue()


class JSONCompressionOptimizer:
    """
    Utility class for optimizing JSON responses before compression.
    """

    @staticmethod
    def optimize_json_response(data: Any) -> str:
        """
        Optimize JSON data for better compression.

        Args:
            data: Data to serialize

        Returns:
            Optimized JSON string
        """
        try:
            # Use compact JSON encoding (no spaces)
            return json.dumps(
                data, ensure_ascii=False, separators=(",", ":"), default=str
            )
        except Exception as e:
            logger.error(f"Error optimizing JSON: {e}")
            return json.dumps(data, default=str)

    @staticmethod
    def remove_null_fields(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove null/None fields from dictionary to reduce size.

        Args:
            data: Dictionary to clean

        Returns:
            Dictionary with null fields removed
        """
        if not isinstance(data, dict):
            return data

        cleaned = {}
        for key, value in data.items():
            if value is not None:
                if isinstance(value, dict):
                    cleaned_value = JSONCompressionOptimizer.remove_null_fields(value)
                    if cleaned_value:  # Only add if not empty
                        cleaned[key] = cleaned_value
                elif isinstance(value, list):
                    cleaned_list = [
                        JSONCompressionOptimizer.remove_null_fields(item)
                        if isinstance(item, dict)
                        else item
                        for item in value
                        if item is not None
                    ]
                    if cleaned_list:  # Only add if not empty
                        cleaned[key] = cleaned_list
                else:
                    cleaned[key] = value

        return cleaned

    @staticmethod
    def compress_repeated_values(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize repeated values in data structure.

        Args:
            data: Data to optimize

        Returns:
            Optimized data structure
        """
        # This is a placeholder for more advanced optimization
        # Could implement value deduplication, reference compression, etc.
        return data


class SmartCompressionResponse(JSONResponse):
    """
    Smart JSON response that optimizes content before compression.
    """

    def __init__(
        self,
        content: Any = None,
        status_code: int = 200,
        headers: Dict[str, str] = None,
        media_type: str = "application/json",
        optimize_json: bool = True,
        remove_nulls: bool = True,
    ):
        """
        Initialize smart compression response.

        Args:
            content: Response content
            status_code: HTTP status code
            headers: Response headers
            media_type: Media type
            optimize_json: Whether to optimize JSON
            remove_nulls: Whether to remove null fields
        """
        if optimize_json and content is not None:
            if remove_nulls and isinstance(content, dict):
                content = JSONCompressionOptimizer.remove_null_fields(content)

            # Pre-optimize the JSON for better compression
            optimized_content = JSONCompressionOptimizer.optimize_json_response(content)
            super().__init__(
                content=None,  # We'll handle JSON encoding ourselves
                status_code=status_code,
                headers=headers,
                media_type=media_type,
            )
            # Set the body directly to avoid double JSON encoding
            self.body = optimized_content.encode("utf-8")
            # The parent computed content-length from the placeholder body ("null")
            self.headers["content-length"] = str(len(self.body))
        else:
            super().__init__(
                content=content,
                status_code=status_code,
                headers=headers,
                media_type=media_type,
            )


# Utility functions for response optimization
def create_optimized_response(
    data: Any, status_code: int = 200, optimize: bool = True
) -> SmartCompressionResponse:
    """
    Create an optimized response for compression.

    Args:
        data: Response data
        status_code: HTTP status code
        optimize: Whether to apply optimizations

    Returns:
        Optimized response
    """
    return SmartCompressionResponse(
        content=data,
        status_code=status_code,
        optimize_json=optimize,
        remove_nulls=optimize,
    )


def calculate_compression_stats(
    original_size: int, compressed_size: int
) -> Dict[str, Any]:
    """
    Calculate compression statistics.

    Args:
        original_size: Original content size in bytes
        compressed_size: Compressed content size in bytes

    Returns:
        Compression statistics
    """
    reduction_bytes = original_size - compressed_size
    reduction_percentage = (
        (reduction_bytes / original_size) * 100 if original_size > 0 else 0
    )
    compression_ratio = original_size / compressed_size if compressed_size > 0 else 0

    return {
        "original_size": original_size,
        "compressed_size": compressed_size,
        "reduction_bytes": reduction_bytes,
        "reduction_percentage": round(reduction_percentage, 2),
        "compression_ratio": round(compression_ratio, 2),
        "space_saved": f"{reduction_percentage:.1f}%",
    }


def get_compression_middleware(app) -> CompressionMiddleware:
    """
    Create compression middleware with optimized settings.

    Args:
        app: FastAPI application

    Returns:
        Configured compression middleware
    """
    settings = get_settings()

    # Adjust compression settings based on environment
    if settings.environment == "production":
        # More aggressive compression in production
        return CompressionMiddleware(
            app,
            minimum_size=512,  # Compress smaller responses
            compress_level=6,  # Balanced compression level
        )
    else:
        # Lighter compression in development
        return CompressionMiddleware(
            app,
            minimum_size=1024,  # Standard minimum size
            compress_level=4,  # Faster compression
        )
