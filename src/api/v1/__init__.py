"""
API v1 module.

This module contains all v1 API endpoints and routers.
"""

from fastapi import APIRouter

from .endpoints import (
    analytics,
    analytics_enhanced,
    auth,
    cache_management,
    customers,
    customers_enhanced,
    fraud,
    fraud_enhanced,
    health,
    operations,
    realtime,
)

# Create the main API router for v1
api_router = APIRouter()

# Include endpoint routers
api_router.include_router(health.router, prefix="/health", tags=["health"])

api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])

# Enhanced analytics endpoints (comprehensive analytics with ML integration)
api_router.include_router(
    analytics_enhanced.router, prefix="/analytics", tags=["analytics"]
)

# Enhanced customer endpoints (with segmentation, CLV, churn prediction)
api_router.include_router(
    customers_enhanced.router, prefix="/customers", tags=["customers"]
)

# Enhanced fraud detection endpoints (with case management and ML models)
api_router.include_router(fraud_enhanced.router, prefix="/fraud", tags=["fraud"])

# Real-time metrics endpoints (system health, streaming, business KPIs)
api_router.include_router(realtime.router, prefix="/realtime", tags=["real-time"])
api_router.include_router(
    operations.router, prefix="/operations", tags=["operations"]
)

# Cache management endpoints (statistics, invalidation, monitoring)
api_router.include_router(cache_management.router, prefix="/cache", tags=["cache"])

# Legacy endpoints (kept for backward compatibility - will be deprecated)
# api_router.include_router(analytics.router, prefix="/analytics/legacy", tags=["analytics-legacy"])
# api_router.include_router(customers.router, prefix="/customers/legacy", tags=["customers-legacy"])
# api_router.include_router(fraud.router, prefix="/fraud/legacy", tags=["fraud-legacy"])


__all__ = ["api_router"]
