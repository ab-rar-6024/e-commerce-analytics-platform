"""
Operational monitoring endpoints for API v1.

Fraud-model quality, data quality, alerts, performance and pipeline freshness,
measured from the database, this machine and the API's own request metrics
(see src/api/repositories/operations.py). Components that are not deployed are
reported as unavailable rather than simulated.
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.database.models import FraudAlert, Order, UserEvent

from ...auth.dependencies import get_current_active_user, require_permission
from ...auth.models import Permission, User
from ...dependencies import get_database_session
from ...repositories import fraud as fraud_repo
from ...repositories import operations as ops
from ...repositories.common import data_now

router = APIRouter()
READ = Depends(require_permission(Permission.READ_ANALYTICS))


@router.get("/fraud-monitoring", summary="Get fraud detection operational metrics")
async def get_fraud_monitoring(
    time_range: str = Query(default="24h", description="Time range: 1h, 24h, 7d"),
    include_alerts: bool = Query(default=True, description="Include alert counts"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    dashboard = fraud_repo.fraud_dashboard(db, time_range)
    models = fraud_repo.detection_models(db, "90d")
    return {
        "time_range": time_range, "generated_at": datetime.utcnow().isoformat(),
        "monitoring": {
            "alerts_last_24h": fraud_repo.fraud_dashboard(db, "24h")["alerts"]["total"] if include_alerts else 0,
            "alerts_in_range": dashboard["alerts"]["total"],
            "false_positive_rate": dashboard["overview"]["false_positive_rate"],
        },
        "model_health": {
            "ensemble_accuracy": models["ensemble_performance"]["combined_precision"],
            "ensemble_recall": models["ensemble_performance"]["combined_recall"],
            "avg_detection_latency_ms": dashboard["performance"]["detection_latency_ms"],
            "individual_models": [
                {"name": m["name"], "accuracy": m["precision"], "recall": m["recall"], "last_trained": None}
                for m in models["models"]
            ],
        },
    }


@router.get("/data-quality", summary="Get data quality metrics")
async def get_data_quality(
    time_range: str = Query(default="24h", description="Time range: 1h, 24h, 7d"),
    include_anomalies: bool = Query(default=True, description="Include anomalies"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    q = ops.data_quality(db)
    now = datetime.utcnow()
    pipelines = []
    for name, v in q["freshness"].items():
        rows = q["tables"].get(name, 0)
        pipelines.append({
            "name": name, "status": "stale" if v["stale"] else "healthy", "last_run": v["latest"],
            "lag_seconds": v["lag_seconds"], "success_rate": q["accuracy_score"], "avg_duration_minutes": None,
            "records_processed": rows,
        })
    alerts = []
    if include_anomalies:
        alerts = [{"severity": "warning", "message": f"{k.replace('_', ' ')}: {n} rows", "detected_at": now.isoformat()}
                  for k, n in q["checks"].items() if n]
    return {
        "time_range": time_range, "generated_at": now.isoformat(),
        "overview": {
            "overall_quality_score": q["overall_quality_score"], "completeness_score": q["completeness_score"],
            "accuracy_score": q["accuracy_score"], "freshness_violations": q["freshness_violations"],
        },
        "alerts": alerts, "pipeline_health": pipelines,
        "issue_distribution": {k: n for k, n in q["checks"].items() if n},
        "checks": q["checks"], "rows_tested": q["rows_tested"],
    }


@router.get("/alerts", summary="Get operational alerts")
async def get_operational_alerts(
    status: str = Query(default="active", description="Alert status filter"),
    priority: str = Query(default="all", description="Severity filter: critical, warning, info, all"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=200),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    alerts = ops.operational_alerts(db)
    if priority != "all":
        alerts = [a for a in alerts if a["severity"] == priority]
    start = (page - 1) * size
    return {
        "summary": {
            "critical": sum(a["severity"] == "critical" for a in alerts),
            "warning": sum(a["severity"] == "warning" for a in alerts),
            "info": sum(a["severity"] == "info" for a in alerts),
            "resolved_last_24h": None, "avg_resolution_time_minutes": None,
        },
        "alerts_by_category": {"system": [], "performance": [], "fraud": []},
        "recent_alerts": alerts[start : start + size],
    }


@router.get("/performance", summary="Get performance metrics")
async def get_operational_performance(
    benchmark_type: str = Query(default="all", description="Benchmark type"),
    time_range: str = Query(default="24h", description="Time range"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    api, dbh = ops.api_metrics(), ops.database_health(db, 5)
    return {
        "benchmark_type": benchmark_type, "time_range": time_range, "generated_at": datetime.utcnow().isoformat(),
        "metrics": {
            "api_response_p95_ms": api["p95_ms"], "database_query_p95_ms": dbh["latency_p95_ms"],
            "kafka_consumer_lag": None, "spark_active_jobs": None,
        },
        "api": {k: api[k] for k in ("total_requests", "requests_per_second", "avg_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms",
                                     "error_rate_4xx", "error_rate_5xx", "sla_compliance")},
        "database": dbh,
        "endpoints": ops.endpoint_stats(),
        "notes": "API figures cover requests since the API process started; Kafka and Spark are not configured.",
    }


@router.get("/streaming-health", summary="Get data pipeline freshness")
async def get_streaming_health(
    include_lag: bool = Query(default=True, description="Include data lag"),
    include_throughput: bool = Query(default=True, description="Include throughput"),
    current_user: User = Depends(get_current_active_user),
    _: None = READ,
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """Freshness and volume of the tables that stand in for streaming topics."""
    now = data_now(db)
    q = ops.data_quality(db)
    since = now - timedelta(minutes=15)
    counts = {
        "orders": db.execute(select(func.count()).select_from(Order).where(Order.order_date >= since)).scalar() or 0,
        "user_events": db.execute(select(func.count()).select_from(UserEvent).where(UserEvent.event_timestamp >= since)).scalar() or 0,
        "fraud_alerts": db.execute(select(func.count()).select_from(FraudAlert).where(FraudAlert.created_at >= since)).scalar() or 0,
    }
    topics = []
    for name, n in counts.items():
        fr = q["freshness"][name]
        topics.append({
            "name": name, "status": "stale" if fr["stale"] else "healthy",
            "messages_per_second": round(n / 900, 4) if include_throughput else None,
            "consumer_lag": None, "partition_count": None, "last_message_time": fr["latest"],
            "lag_seconds": fr["lag_seconds"] if include_lag else None,
        })
    lags = [t["lag_seconds"] for t in topics if t["lag_seconds"] is not None]
    return {
        "generated_at": datetime.utcnow().isoformat(),
        "pipeline_metrics": {
            "messages_per_second": round(sum(counts.values()) / 900, 4) if include_throughput else None,
            "processing_lag_seconds": max(lags) if lags and include_lag else None,
            "error_rate": round(1 - q["accuracy_score"], 4), "active_consumers": None,
        },
        "topic_health": topics,
        "notes": "Kafka is not configured; topics here are database tables and lag is the age of the newest row.",
    }
