"""Database configuration and connection management."""

import os
from typing import Any, Dict, Optional

from loguru import logger
from pydantic import Field
from pydantic_settings import BaseSettings
from sqlalchemy import Engine, create_engine, pool, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool


class DatabaseSettings(BaseSettings):
    """Database configuration settings."""

    # Database connection parameters
    db_host: str = Field(default="localhost", description="Database host")
    db_port: int = Field(default=5432, description="Database port")
    db_name: str = Field(default="ecommerce_analytics", description="Database name")
    db_user: str = Field(default="analytics_user", description="Database user")
    db_password: str = Field(default="dev_password", description="Database password")

    # Connection pool settings
    pool_size: int = Field(default=20, description="Connection pool size")
    max_overflow: int = Field(default=30, description="Max overflow connections")
    pool_timeout: int = Field(default=30, description="Pool timeout in seconds")
    pool_recycle: int = Field(default=3600, description="Pool recycle time in seconds")

    # SQL Alchemy settings
    echo_sql: bool = Field(default=False, description="Echo SQL statements")

    class Config:
        env_file = ".env"
        case_sensitive = False
        env_prefix = "DB_"
        # .env also holds settings for other components; ignore keys that are not ours
        extra = "ignore"

    @property
    def database_url(self) -> str:
        """Generate database URL from settings."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def async_database_url(self) -> str:
        """Generate async database URL from settings."""
        return f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


class DatabaseManager:
    """Database connection and session management."""

    def __init__(self, settings: Optional[DatabaseSettings] = None):
        """Initialize database manager."""
        self.settings = settings or DatabaseSettings()
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None

    def create_engine(self) -> Engine:
        """Create database engine with connection pooling."""
        if self._engine is None:
            logger.info(
                f"Creating database engine for {self.settings.db_host}:{self.settings.db_port}"
            )

            engine_kwargs = {
                "url": self.settings.database_url,
                "echo": self.settings.echo_sql,
                "poolclass": QueuePool,
                "pool_size": self.settings.pool_size,
                "max_overflow": self.settings.max_overflow,
                "pool_timeout": self.settings.pool_timeout,
                "pool_recycle": self.settings.pool_recycle,
                "pool_pre_ping": True,  # Validate connections before use
            }

            self._engine = create_engine(**engine_kwargs)

            # Test connection
            try:
                with self._engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("Database connection successful")
            except Exception as e:
                logger.error(f"Database connection failed: {e}")
                raise

        return self._engine

    def create_session_factory(self) -> sessionmaker:
        """Create session factory."""
        if self._session_factory is None:
            engine = self.create_engine()
            self._session_factory = sessionmaker(
                bind=engine, autocommit=False, autoflush=False, expire_on_commit=False
            )
        return self._session_factory

    def get_session(self) -> Session:
        """Get database session."""
        session_factory = self.create_session_factory()
        return session_factory()

    def close_all_connections(self) -> None:
        """Close all database connections."""
        if self._engine:
            self._engine.dispose()
            logger.info("Database connections closed")


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """Get global database manager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def get_database_session() -> Session:
    """Get database session."""
    return get_database_manager().get_session()


def get_database_engine() -> Engine:
    """Get database engine."""
    return get_database_manager().create_engine()


# Context manager for database sessions
class DatabaseSession:
    """Context manager for database sessions."""

    def __init__(self, session: Optional[Session] = None):
        self.session = session or get_database_session()
        self._should_close = session is None

    def __enter__(self) -> Session:
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is not None:
                self.session.rollback()
            else:
                self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        finally:
            if self._should_close:
                self.session.close()


def create_database_session() -> DatabaseSession:
    """Create database session context manager."""
    return DatabaseSession()
