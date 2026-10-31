"""
Configuration management for the FastAPI application.

This module provides centralized configuration management using Pydantic settings
with support for environment variables and validation.
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from pydantic import Field, validator
from pydantic_settings import BaseSettings


# Local development database (SQLite file; created and filled by scripts/seed_database.py)
DEFAULT_DATABASE_URL = f"sqlite:///{(Path(__file__).resolve().parent.parent.parent / 'data' / 'ecap.db').as_posix()}"


class Settings(BaseSettings):
    """
    Application settings with environment variable support.

    All settings can be overridden using environment variables with the prefix 'ECAP_'.
    For example: ECAP_DATABASE_URL, ECAP_REDIS_URL, etc.
    """

    # Application settings
    environment: str = Field(
        default="development", description="Application environment"
    )
    debug: bool = Field(default=False, description="Enable debug mode")
    # Use localhost for development, 0.0.0.0 for production deployment
    host: str = Field(default="127.0.0.1", description="Host to bind the server")
    port: int = Field(default=8000, description="Port to bind the server")
    log_level: str = Field(default="INFO", description="Logging level")

    # Security settings
    secret_key: str = Field(
        default="your-secret-key-change-in-production",
        description="Secret key for JWT tokens",
    )
    access_token_expire_minutes: int = Field(
        default=1440, description="Access token expiration time in minutes (24 hours)"
    )
    refresh_token_expire_days: int = Field(
        default=30, description="Refresh token expiration time in days"
    )
    allowed_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8080"],
        description="Allowed CORS origins",
    )

    # Database settings
    database_url: str = Field(
        default=DEFAULT_DATABASE_URL,
        description=(
            "Database connection URL: local SQLite by default; set ECAP_DATABASE_URL "
            "to a postgresql+psycopg2://... URL (e.g. Supabase) for Postgres"
        ),
    )
    # Small pools suit hosted Postgres poolers (Supabase free tier allows ~15 clients)
    database_pool_size: int = Field(
        default=5, description="Database connection pool size"
    )
    database_max_overflow: int = Field(
        default=5, description="Database max overflow connections"
    )

    # Redis settings
    redis_url: str = Field(
        default="redis://localhost:6379/0", description="Redis connection URL"
    )
    redis_expire_time: int = Field(
        default=3600, description="Default Redis key expiration time in seconds"
    )

    # Analytics settings
    default_page_size: int = Field(
        default=100, description="Default pagination page size"
    )
    max_page_size: int = Field(default=1000, description="Maximum pagination page size")

    # Rate limiting settings
    rate_limit_requests: int = Field(
        default=100, description="Rate limit requests per minute"
    )
    rate_limit_window: int = Field(
        default=60, description="Rate limit window in seconds"
    )

    # Spark settings
    spark_master_url: str = Field(
        default="spark://localhost:7077", description="Spark master URL"
    )
    spark_app_name: str = Field(
        default="ECAP-API", description="Spark application name"
    )

    @validator("environment")
    def validate_environment(cls, v):
        """Validate environment setting."""
        valid_environments = ["development", "staging", "production"]
        if v not in valid_environments:
            raise ValueError(f"Environment must be one of: {valid_environments}")
        return v

    @validator("log_level")
    def validate_log_level(cls, v):
        """Validate log level setting."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v.upper()

    @validator("port")
    def validate_port(cls, v):
        """Validate port setting."""
        if not 1 <= v <= 65535:
            raise ValueError("Port must be between 1 and 65535")
        return v

    @validator("default_page_size", "max_page_size")
    def validate_page_sizes(cls, v):
        """Validate pagination settings."""
        if v <= 0:
            raise ValueError("Page size must be positive")
        return v

    @validator("max_page_size")
    def validate_max_page_size(cls, v, values):
        """Ensure max page size is greater than default."""
        if "default_page_size" in values and v < values["default_page_size"]:
            raise ValueError(
                "Max page size must be greater than or equal to default page size"
            )
        return v

    class Config:
        """Pydantic configuration."""

        env_prefix = "ECAP_"
        case_sensitive = False

        # Allow loading from .env file in development
        env_file = (
            ".env"
            if os.getenv("ECAP_ENVIRONMENT", "development") == "development"
            else None
        )
        env_file_encoding = "utf-8"
        # .env also holds non-API settings (e.g. ECAP_SUPABASE_*); ignore unknown keys
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached application settings.

    Returns:
        Settings: Application settings instance
    """
    return Settings()


# Environment-specific configurations
def get_database_config() -> dict:
    """
    Get database configuration for SQLAlchemy.

    Returns:
        dict: Database configuration parameters
    """
    settings = get_settings()

    return {
        "url": settings.database_url,
        "pool_size": settings.database_pool_size,
        "max_overflow": settings.database_max_overflow,
        "pool_pre_ping": True,
        "pool_recycle": 3600,  # Recycle connections every hour
        "echo": settings.debug,  # Log SQL queries in debug mode
    }


def get_redis_config() -> dict:
    """
    Get Redis configuration.

    Returns:
        dict: Redis configuration parameters
    """
    settings = get_settings()

    return {
        "url": settings.redis_url,
        "decode_responses": True,
        "socket_keepalive": True,
        "socket_keepalive_options": {},
        "health_check_interval": 30,
    }


def get_cors_config() -> dict:
    """
    Get CORS configuration.

    Returns:
        dict: CORS configuration parameters
    """
    settings = get_settings()

    # In production, be more restrictive
    if settings.environment == "production":
        return {
            "allow_origins": settings.allowed_origins,
            "allow_credentials": True,
            "allow_methods": ["GET", "POST"],
            "allow_headers": ["Authorization", "Content-Type"],
        }
    else:
        return {
            "allow_origins": settings.allowed_origins + ["*"],
            "allow_credentials": True,
            "allow_methods": ["*"],
            "allow_headers": ["*"],
        }


# Configuration validation
def validate_configuration() -> bool:
    """
    Validate the current configuration.

    Returns:
        bool: True if configuration is valid

    Raises:
        ValueError: If configuration is invalid
    """
    try:
        settings = get_settings()

        # Validate critical settings
        if settings.environment == "production":
            if settings.secret_key == "your-secret-key-change-in-production":
                raise ValueError("Secret key must be changed in production")

            if settings.debug:
                raise ValueError("Debug mode should be disabled in production")

        # Validate database URL format
        if not settings.database_url.startswith(
            ("postgresql://", "postgresql+", "sqlite:///")
        ):
            raise ValueError("Database URL must be PostgreSQL or SQLite")

        # Validate Redis URL format
        if not settings.redis_url.startswith("redis://"):
            raise ValueError("Redis URL must start with redis://")

        return True

    except Exception as e:
        raise ValueError(f"Configuration validation failed: {e}")


if __name__ == "__main__":
    # Test configuration loading
    try:
        settings = get_settings()
        print("Configuration loaded successfully:")
        print(f"Environment: {settings.environment}")
        print(f"Host: {settings.host}:{settings.port}")
        print(f"Database: {settings.database_url}")
        print(f"Redis: {settings.redis_url}")

        validate_configuration()
        print("Configuration validation passed!")

    except Exception as e:
        print(f"Configuration error: {e}")
        exit(1)
