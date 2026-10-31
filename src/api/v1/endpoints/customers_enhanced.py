"""
Customer endpoints for API v1.

Customer directory with RFM analytics, single-customer detail, co-purchase
recommendations and a simple value outlook, all computed from the database by
src/api/repositories/customers.py.
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ...auth.dependencies import get_current_active_user, require_permission
from ...auth.models import Permission, User
from ...dependencies import PaginationParams, get_database_session, get_pagination
from ...exceptions import NotFoundError
from ...repositories import customers as repo

router = APIRouter()
READ = Depends(require_permission(Permission.READ_CUSTOMERS))


@router.get("/", summary="List customers with analytics")
async def list_customers(
    segment: Optional[str] = Query(default=None, description="RFM segment filter, e.g. Champions"),
    risk_level: Optional[str] = Query(default=None, description="Churn risk filter: low, medium, high"),
    sort_by: str = Query(default="lifetime_value", description="lifetime_value, total_orders, recency, churn_risk, created_at"),
    include_predictions: bool = Query(default=False, description="Reserved"),
    pagination: PaginationParams = Depends(get_pagination),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    result = repo.list_customers(db, segment, risk_level, sort_by, pagination.page, pagination.size)
    result["filters"]["include_predictions"] = include_predictions
    return result


@router.get("/{customer_id}", summary="Get customer details with full analytics")
async def get_customer(
    customer_id: str,
    include_history: bool = Query(default=True, description="Include purchase history"),
    include_predictions: bool = Query(default=True, description="Include churn / value outlook"),
    include_journey: bool = Query(default=False, description="Reserved"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    detail = repo.customer_detail(db, customer_id, include_history)
    if detail is None:
        raise NotFoundError("Customer", customer_id)
    if not include_predictions:
        detail.pop("predictions", None)
    return detail


@router.get("/{customer_id}/recommendations", summary="Get product recommendations for a customer")
async def get_customer_recommendations(
    customer_id: str,
    recommendation_type: str = Query(default="products", description="Recommendation type"),
    limit: int = Query(default=10, ge=1, le=50, description="Maximum number of recommendations"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    result = repo.recommendations(db, customer_id, limit)
    if result is None:
        raise NotFoundError("Customer", customer_id)
    result["recommendation_type"] = recommendation_type
    return result


@router.get("/{customer_id}/value-prediction", summary="Get customer value outlook")
async def get_customer_value_prediction(
    customer_id: str,
    prediction_horizon: str = Query(default="12m", description="Horizon in months, e.g. 12m"),
    include_scenarios: bool = Query(default=False, description="Reserved"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    months = int(prediction_horizon.rstrip("m") or 12) if prediction_horizon.rstrip("m").isdigit() else 12
    result = repo.value_prediction(db, customer_id, horizon_months=months)
    if result is None:
        raise NotFoundError("Customer order history", customer_id)
    return result
