"""
Main FastAPI application for E-Commerce Analytics Platform.

This module provides the main FastAPI application with proper structure,
dependency injection, API versioning, and comprehensive error handling.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.monitoring import initialize_tracing, instrument_all

from .config import get_settings
from .exceptions import ECAPException
from .middleware.compression import get_compression_middleware
from .middleware.correlation_middleware import CorrelationIdMiddleware
from .middleware.metrics_middleware import (
    PrometheusMetricsMiddleware,
    get_metrics_response,
)
from .v1 import api_router as v1_router

# Configure logging
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.

    Handles startup and shutdown events for the FastAPI application.
    """
    # Startup
    logger.info("Starting E-Commerce Analytics Platform API")
    settings = get_settings()
    logger.info(f"Running in {settings.environment} mode")

    # Initialize distributed tracing
    try:
        initialize_tracing(
            service_name="ecap-api",
            service_version="1.0.0",
            environment=settings.environment,
        )
        logger.info("Distributed tracing initialized successfully")
    except Exception as e:
        logger.warning(f"Failed to initialize tracing: {e}")

    # Validate database connection
    try:
        from .dependencies import validate_database_connection

        await validate_database_connection()
        logger.info("Database connection validated successfully")
    except Exception as e:
        logger.error(f"Database connection validation failed: {e}")
        raise

    # Validate Redis connection
    try:
        from .dependencies import validate_redis_connection

        await validate_redis_connection()
        logger.info("Redis connection validated successfully")
    except Exception as e:
        logger.warning(f"Redis connection validation failed: {e}")
        # Redis is optional, so we don't raise here

    yield

    # Shutdown
    logger.info("Shutting down E-Commerce Analytics Platform API")


def create_application() -> FastAPI:
    """
    Application factory function.

    Returns:
        FastAPI: Configured FastAPI application instance
    """
    settings = get_settings()

    app = FastAPI(
        title="E-Commerce Analytics Platform API",
        description="REST API for accessing e-commerce analytics, fraud detection, and business intelligence data",
        version="1.0.0",
        docs_url="/docs" if settings.environment != "production" else None,
        redoc_url="/redoc" if settings.environment != "production" else None,
        openapi_url="/openapi.json" if settings.environment != "production" else None,
        lifespan=lifespan,
    )

    # Add compression middleware (before CORS)
    compression_middleware = get_compression_middleware(app)
    app.add_middleware(
        type(compression_middleware),
        minimum_size=compression_middleware.minimum_size,
        compress_level=compression_middleware.compress_level,
        excluded_media_types=compression_middleware.excluded_media_types,
    )

    # Add metrics middleware (before correlation to track all requests)
    app.add_middleware(PrometheusMetricsMiddleware)

    # Add correlation ID middleware
    app.add_middleware(CorrelationIdMiddleware)

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(v1_router, prefix="/api/v1", tags=["v1"])

    # Add exception handlers
    add_exception_handlers(app)

    # Initialize tracing instrumentation
    try:
        instrument_all(app, enable_database=True, enable_redis=True, enable_kafka=True)
        logger.info("Application instrumentation completed")
    except Exception as e:
        logger.warning(f"Failed to instrument application: {e}")

    return app


def add_exception_handlers(app: FastAPI) -> None:
    """
    Add custom exception handlers to the FastAPI application.

    Args:
        app: FastAPI application instance
    """

    @app.exception_handler(ECAPException)
    async def ecap_exception_handler(request: Request, exc: ECAPException):
        """Handle custom ECAP exceptions."""
        logger.error(
            f"ECAP Exception: {exc.message}", extra={"error_code": exc.error_code}
        )
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.error_code,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        """Handle HTTP exceptions."""
        logger.warning(
            f"HTTP Exception: {exc.detail}", extra={"status_code": exc.status_code}
        )
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": f"HTTP_{exc.status_code}",
                    "message": exc.detail,
                    "details": None,
                }
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ):
        """Handle request validation errors."""
        logger.warning(f"Validation Error: {exc.errors()}")
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "Request validation failed",
                    "details": exc.errors(),
                }
            },
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        """Handle unexpected exceptions."""
        logger.error(f"Unexpected error: {str(exc)}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": {
                    "code": "INTERNAL_SERVER_ERROR",
                    "message": "An unexpected error occurred",
                    "details": None,
                }
            },
        )


# Create the application instance
app = create_application()


# Health check endpoint
@app.get("/health", tags=["health"])
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint.

    Returns:
        Dict containing health status and basic system information
    """
    return {
        "status": "healthy",
        "version": "1.0.0",
        "environment": get_settings().environment,
        "timestamp": "2025-01-23T00:00:00Z",  # This would be actual timestamp in production
    }


# Metrics endpoint for Prometheus
@app.get("/metrics", tags=["monitoring"])
async def metrics():
    """
    Prometheus metrics endpoint.

    Returns:
        Prometheus-formatted metrics for application monitoring
    """
    return get_metrics_response()


# Root endpoint
@app.get("/", tags=["root"])
async def root() -> Dict[str, str]:
    """
    Root endpoint providing API information.

    Returns:
        Dict containing API welcome message and documentation links
    """
    settings = get_settings()
    base_url = f"http://localhost:{settings.port}"

    response = {
        "message": "Welcome to E-Commerce Analytics Platform API",
        "version": "1.0.0",
        "documentation": f"{base_url}/docs"
        if settings.environment != "production"
        else None,
        "health": f"{base_url}/health",
    }

    # Remove None values in production
    return {k: v for k, v in response.items() if v is not None}


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "src.api.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.environment == "development",
        log_level=settings.log_level.lower(),
    )
