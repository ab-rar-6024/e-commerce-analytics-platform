"""
Fraud detection endpoints for API v1.

Alerts come from the rules engine in src/database/fraud_scoring.py run over stored
orders. Detection quality is measured against known fraud outcomes. Everything is
computed by src/api/repositories/fraud.py.
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ...auth.dependencies import get_current_active_user, require_permission
from ...auth.models import Permission, User
from ...dependencies import PaginationParams, get_database_session, get_pagination
from ...repositories import fraud as repo

router = APIRouter()
READ = Depends(require_permission(Permission.READ_FRAUD))


@router.get("/alerts", summary="Get fraud alerts")
async def get_fraud_alerts(
    status: Optional[str] = Query(default=None, description="open, investigating, resolved, escalated, false_positive"),
    priority: Optional[str] = Query(default=None, description="critical, high, medium, low"),
    alert_type: Optional[str] = Query(default=None, description="amount, velocity, behavioral, pattern"),
    risk_score_min: Optional[float] = Query(default=None, ge=0, le=1, description="Minimum risk score"),
    start_date: Optional[str] = Query(default=None, description="Start date filter (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="End date filter (YYYY-MM-DD)"),
    assigned_to: Optional[str] = Query(default=None, description="Investigator filter"),
    pagination: PaginationParams = Depends(get_pagination),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    return repo.list_alerts(
        db, status, priority, alert_type, risk_score_min, start_date, end_date, assigned_to,
        pagination.page, pagination.size,
    )


@router.get("/dashboard", summary="Get fraud detection dashboard")
async def get_fraud_dashboard(
    time_range: str = Query(default="24h", description="Time range, e.g. 24h, 7d, 30d"),
    include_predictions: bool = Query(default=False, description="Reserved"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    return repo.fraud_dashboard(db, time_range)


@router.get("/models", summary="Get fraud detection rule performance")
async def get_model_performance(
    model_type: str = Query(default="rules", description="Detection component type"),
    time_range: str = Query(default="7d", description="Requested range; evaluation uses at least 90d"),
    include_details: bool = Query(default=True, description="Reserved"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """Precision/recall of each rule against known outcomes (needs enough history to be meaningful)."""
    result = repo.detection_models(db, "90d")
    result["requested_time_range"] = time_range
    result["evaluation_window"] = "90d"
    return result


@router.get("/cases", summary="Get fraud investigation cases")
async def get_fraud_cases(
    status: Optional[str] = Query(default=None, description="open, in_progress, escalated"),
    investigator: Optional[str] = Query(default=None, description="Assigned investigator"),
    priority: Optional[str] = Query(default=None, description="Case priority filter"),
    case_type: Optional[str] = Query(default=None, description="Case type filter"),
    pagination: PaginationParams = Depends(get_pagination),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    return repo.list_cases(db, status, investigator, priority, case_type, pagination.page, pagination.size)


@router.get("/risk-assessment", summary="Get risk assessment")
async def get_risk_assessment(
    assessment_type: str = Query(default="overall", description="overall, customer, merchant, geographic"),
    time_range: str = Query(default="30d", description="Time range: 7d, 30d, 90d"),
    risk_threshold: float = Query(default=0.7, description="Score at which risk is rated high"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    return repo.risk_assessment(db, assessment_type, time_range, risk_threshold)
