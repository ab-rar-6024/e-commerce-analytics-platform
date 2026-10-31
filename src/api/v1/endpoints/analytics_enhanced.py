"""
Analytics endpoints for API v1.

Revenue, customer segmentation, product performance, real-time metrics, cohort
retention and conversion funnels. Every figure is computed from the database by
src/api/repositories/analytics.py.
"""

import math
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ...auth.dependencies import get_current_active_user, require_permission
from ...auth.models import Permission, User
from ...dependencies import PaginationParams, get_database_session, get_pagination
from ...exceptions import ValidationError
from ...repositories import analytics as repo
from ...repositories import operations as ops

router = APIRouter()
READ = Depends(require_permission(Permission.READ_ANALYTICS))


@router.get("/revenue", summary="Get comprehensive revenue analytics")
async def get_revenue_analytics(
    start_date: Optional[str] = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="End date (YYYY-MM-DD)"),
    granularity: str = Query(default="daily", description="Data granularity: daily, weekly, monthly"),
    region: Optional[str] = Query(default=None, description="Geographic region filter"),
    category: Optional[str] = Query(default=None, description="Product category filter"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """Revenue time series, summary, forecast, breakdowns and cohort metrics."""
    if granularity not in ("daily", "weekly", "monthly"):
        raise ValidationError(
            "Invalid granularity. Must be one of: ['daily', 'weekly', 'monthly']", {"granularity": granularity}
        )
    try:
        return repo.revenue_analytics(db, start_date, end_date, granularity, region, category)
    except ValueError as exc:  # unparsable dates
        raise ValidationError(f"Invalid date: {exc}", {"start_date": start_date, "end_date": end_date})


@router.get("/customers/segments", summary="Get customer segmentation analytics")
async def get_customer_segmentation(
    segment_type: str = Query(default="rfm", description="Segmentation type: rfm, behavior, geographic"),
    include_predictions: bool = Query(default=False, description="Include ML predictions"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """RFM segments (default), loyalty tiers (behavior) or regions (geographic)."""
    return repo.customer_segments(db, segment_type)


@router.get("/products/performance", summary="Get product performance analytics")
async def get_product_performance(
    category: Optional[str] = Query(default=None, description="Product category filter"),
    sort_by: str = Query(default="revenue", description="Sort: revenue, units, rating, margin, growth"),
    time_range: str = Query(default="30d", description="Time range: 7d, 30d, 90d, 1y"),
    include_recommendations: bool = Query(default=False, description="Reserved"),
    pagination: PaginationParams = Depends(get_pagination),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """Per-product revenue, units, margin, growth, inventory and conversion."""
    result = repo.product_performance(db, category, sort_by, time_range)
    total = len(result["products"])
    result["products"] = result["products"][pagination.offset : pagination.offset + pagination.size]
    result["pagination"] = {
        "page": pagination.page, "size": pagination.size, "total": total,
        "pages": max(math.ceil(total / pagination.size), 1),
    }
    return result


@router.get("/real-time/metrics", summary="Get real-time analytics metrics")
async def get_realtime_metrics(
    metric_types: Optional[str] = Query(default="sales,traffic,inventory", description="Comma separated metric groups"),
    time_window: str = Query(default="1h", description="Time window: 5m, 15m, 1h, 4h"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """Sales, traffic and inventory over the window, plus measured data-quality and system health."""
    result = repo.realtime_metrics(db, time_window)
    quality = ops.data_quality(db)
    api = ops.api_metrics()
    dbh = ops.database_health(db, 3)
    result["metric_types"] = metric_types
    result["data_quality"] = {
        "stream_health": "stale" if quality["stale_tables"] else "healthy",
        "data_completeness": quality["completeness_score"],
        "validation_errors": quality["rows_failed"],
        "anomaly_count": len(quality["stale_tables"]),
        "error_rate": round(1 - quality["accuracy_score"], 4),
        "freshness_lag_seconds": (quality["freshness"].get("user_events") or {}).get("lag_seconds"),
    }
    result["system_health"] = {
        "api_response_time_ms": api["avg_ms"],
        "database_latency_ms": dbh["latency_p50_ms"],
        "database_connections": dbh["pool_checked_out"],
        "cache_available": ops.probe("redis")["status"] == "healthy",
    }
    return result


@router.get("/cohort-analysis", summary="Get customer cohort analysis")
async def get_cohort_analysis(
    cohort_type: str = Query(default="monthly", description="Cohort type (monthly acquisition cohorts)"),
    metric: str = Query(default="retention", description="Metric: retention or revenue"),
    start_date: Optional[str] = Query(default=None, description="Analysis start date"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """Retention (or revenue per customer) by months since first order."""
    if metric not in ("retention", "revenue"):
        raise ValidationError("Invalid metric. Must be 'retention' or 'revenue'", {"metric": metric})
    return repo.cohort_analysis(db, metric, start_date)


@router.get("/funnels", summary="Get conversion funnel analysis")
async def get_funnel_analysis(
    funnel_type: str = Query(default="purchase", description="Funnel type"),
    time_range: str = Query(default="30d", description="Time range: 7d, 30d, 90d"),
    segment: Optional[str] = Query(default=None, description="Customer tier filter (bronze, silver, gold, platinum)"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """Sessions reaching each step from landing page to purchase."""
    return repo.funnel_analysis(db, funnel_type, time_range, segment)


@router.get("/executive", summary="Get executive overview")
async def get_executive_overview(
    days: int = Query(default=90, ge=7, le=365, description="Length of the comparison period in days"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """KPIs, breakdowns and customer metrics for the last `days` days vs the days before."""
    return repo.executive_overview(db, days)
