"""
Operational monitoring endpoints for API v1.

Provides the operational dashboard with fraud-model health, data quality,
alerting, performance and streaming-pipeline metrics. Like the other
"enhanced" endpoints, these generate representative sample data until they
are wired to the monitoring backends.
"""

import random
from datetime import datetime, timedelta
from typing import Any, Dict

from fastapi import APIRouter, Depends, Query

from ...auth.dependencies import get_current_active_user, require_permission
from ...auth.models import Permission, User

router = APIRouter()


def _iso_ago(**delta: float) -> str:
    """ISO timestamp for `now - timedelta(**delta)`."""
    return (datetime.now() - timedelta(**delta)).isoformat()


@router.get("/fraud-monitoring", summary="Get fraud detection operational metrics")
async def get_fraud_monitoring(
    time_range: str = Query(default="24h", description="Time range: 1h, 24h, 7d"),
    include_alerts: bool = Query(default=True, description="Include alert counts"),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
) -> Dict[str, Any]:
    """Fraud monitoring metrics and per-model health."""
    models = [
        ("Rules Engine", 0.91),
        ("Isolation Forest", 0.89),
        ("Gradient Boosting", 0.94),
        ("Neural Network", 0.92),
    ]
    return {
        "time_range": time_range,
        "generated_at": datetime.now().isoformat(),
        "monitoring": {
            "alerts_last_24h": random.randint(60, 140) if include_alerts else 0,
            "false_positive_rate": round(random.uniform(0.08, 0.16), 3),
        },
        "model_health": {
            "ensemble_accuracy": round(random.uniform(0.92, 0.95), 3),
            "avg_detection_latency_ms": random.randint(90, 160),
            "individual_models": [
                {
                    "name": name,
                    "accuracy": round(base + random.uniform(-0.02, 0.02), 3),
                    "last_trained": _iso_ago(days=random.randint(1, 10)),
                }
                for name, base in models
            ],
        },
    }


@router.get("/data-quality", summary="Get data quality metrics")
async def get_data_quality(
    time_range: str = Query(default="24h", description="Time range: 1h, 24h, 7d"),
    include_anomalies: bool = Query(default=True, description="Include anomalies"),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
) -> Dict[str, Any]:
    """Data quality scores, pipeline health and issue distribution."""
    pipelines = [
        ("transactions-ingest", "healthy"),
        ("user-behavior-ingest", "healthy"),
        ("product-catalog-sync", "healthy"),
        ("delta-lake-compaction", "degraded"),
    ]
    return {
        "time_range": time_range,
        "generated_at": datetime.now().isoformat(),
        "overview": {
            "overall_quality_score": round(random.uniform(0.94, 0.98), 3),
            "completeness_score": round(random.uniform(0.96, 0.99), 3),
            "accuracy_score": round(random.uniform(0.95, 0.99), 3),
            "freshness_violations": random.randint(0, 4),
        },
        "alerts": [],
        "pipeline_health": [
            {
                "name": name,
                "status": status,
                "last_run": _iso_ago(minutes=random.randint(1, 45)),
                "success_rate": round(random.uniform(0.97, 1.0), 3),
                "avg_duration_minutes": round(random.uniform(2, 18), 1),
                "records_processed": random.randint(50_000, 2_000_000),
            }
            for name, status in pipelines
        ],
        "issue_distribution": {
            "null_values": random.randint(20, 120),
            "schema_drift": random.randint(0, 12),
            "duplicates": random.randint(10, 80),
            "out_of_range": random.randint(5, 40),
        },
    }


@router.get("/alerts", summary="Get operational alerts")
async def get_operational_alerts(
    status: str = Query(default="active", description="Alert status filter"),
    priority: str = Query(default="all", description="Priority filter"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=200),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
) -> Dict[str, Any]:
    """Operational alert summary and recent alerts."""
    samples = [
        ("critical", "system", "Spark executor lost on worker-3"),
        ("warning", "performance", "API p95 latency above 400ms"),
        ("warning", "system", "Kafka consumer lag growing on transactions topic"),
        ("info", "fraud", "Fraud model retrained and promoted"),
        ("warning", "performance", "Database connection pool at 85%"),
        ("info", "system", "Delta Lake compaction completed"),
    ]
    recent = [
        {
            "timestamp": _iso_ago(minutes=i * random.randint(8, 25)),
            "severity": severity,
            "category": category,
            "message": message,
            "status": random.choice(["active", "acknowledged", "resolved"]),
            "assigned_to": random.choice(["oncall-1", "oncall-2", None]),
        }
        for i, (severity, category, message) in enumerate(samples, start=1)
    ]
    if status != "all":
        recent = [a for a in recent if a["status"] == status] or recent[:2]
    if priority != "all":
        recent = [a for a in recent if a["severity"] == priority] or recent[:2]
    start = (page - 1) * size
    return {
        "summary": {
            "critical": sum(a["severity"] == "critical" for a in recent),
            "warning": sum(a["severity"] == "warning" for a in recent),
            "resolved_last_24h": random.randint(10, 40),
            "avg_resolution_time_minutes": round(random.uniform(12, 55), 1),
        },
        "alerts_by_category": {"system": [], "performance": [], "fraud": []},
        "recent_alerts": recent[start : start + size],
    }


@router.get("/performance", summary="Get performance benchmarks")
async def get_operational_performance(
    benchmark_type: str = Query(default="all", description="Benchmark type"),
    time_range: str = Query(default="24h", description="Time range: 1h, 24h, 7d"),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
) -> Dict[str, Any]:
    """Headline latency, lag and job metrics."""
    return {
        "benchmark_type": benchmark_type,
        "time_range": time_range,
        "generated_at": datetime.now().isoformat(),
        "metrics": {
            "api_response_p95_ms": random.randint(120, 380),
            "database_query_p95_ms": random.randint(30, 140),
            "kafka_consumer_lag": random.randint(50, 1500),
            "spark_active_jobs": random.randint(2, 12),
        },
    }


@router.get("/streaming-health", summary="Get streaming pipeline health")
async def get_streaming_health(
    include_lag: bool = Query(default=True, description="Include consumer lag"),
    include_throughput: bool = Query(default=True, description="Include throughput"),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
) -> Dict[str, Any]:
    """Pipeline-level metrics and per-topic health."""
    topics = [
        ("transactions", 6),
        ("user-events", 8),
        ("product-updates", 3),
        ("fraud-alerts", 2),
    ]
    return {
        "generated_at": datetime.now().isoformat(),
        "pipeline_metrics": {
            "messages_per_second": random.randint(4_000, 12_000)
            if include_throughput
            else 0,
            "processing_lag_seconds": round(random.uniform(0.5, 6.0), 1)
            if include_lag
            else 0,
            "error_rate": round(random.uniform(0.0005, 0.01), 4),
            "active_consumers": random.randint(6, 16),
        },
        "topic_health": [
            {
                "name": name,
                "status": random.choice(["healthy", "healthy", "degraded"]),
                "messages_per_second": random.randint(300, 4_000),
                "consumer_lag": random.randint(0, 900),
                "partition_count": partitions,
                "last_message_time": _iso_ago(seconds=random.randint(1, 20)),
            }
            for name, partitions in topics
        ],
    }
