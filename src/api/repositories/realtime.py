"""
Real-time style views (system health, stream metrics, business metrics, benchmarks).

Computed from the database, this machine and the API's own request metrics.
Components that are not deployed (Kafka, Spark, Redis, marketing systems) are
reported as not configured rather than given invented numbers.
"""

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.database.models import Customer, FraudAlert, Order, OrderItem, Product, UserEvent

from . import analytics as an
from . import operations as ops
from .common import data_now, f, parse_window, ttl_cache

NOT_CONFIGURED = {"kafka": "Kafka is not configured", "spark": "No Spark cluster is configured", "redis": "Redis is not running"}


def _service(name: str) -> Dict[str, Any]:
    p = ops.probe(name)
    if p["status"] == "healthy":
        return {"status": "healthy", "latency_ms": p["latency_ms"]}
    return {"status": "not_configured", "reason": NOT_CONFIGURED[name]}


def system_health(db: Session, include_details: bool = False) -> Dict[str, Any]:
    host, api, dbh = ops.host_metrics(), ops.api_metrics(), ops.database_health(db)
    quality = ops.data_quality(db)

    problems = []
    if host["cpu_percent"] > 90 or host["memory_percent"] > 92:
        problems.append("critical")
    elif host["cpu_percent"] > 75 or host["memory_percent"] > 85:
        problems.append("degraded")
    if api["error_rate_5xx"] > 0.05:
        problems.append("critical")
    if dbh["latency_p95_ms"] > 500:
        problems.append("degraded")
    if quality["stale_tables"]:
        problems.append("degraded")
    overall = "critical" if "critical" in problems else "degraded" if problems else "healthy"

    level = {"critical": "error", "warning": "warning", "info": "info"}
    alerts = [{"level": level[a["severity"]], "component": a["category"], "message": a["message"], "timestamp": a["timestamp"]}
              for a in ops.operational_alerts(db) if a["severity"] != "info"]
    return {
        "timestamp": datetime.utcnow().isoformat(), "overall_status": overall,
        "uptime_seconds": host["uptime_seconds"], "response_time_ms": api["avg_ms"],
        "throughput_rps": api["requests_per_second"], "error_rate": api["error_rate_5xx"],
        "core_metrics": {
            "api_server": {"status": "healthy", "response_time_p95_ms": api["p95_ms"], "active_connections": api["requests_in_progress"],
                           "memory_usage_mb": host["process_memory_mb"], "cpu_usage_percent": host["cpu_percent"]},
            "database": {"status": dbh["status"], "engine": dbh["engine"], "connection_pool_usage": dbh["pool_usage"],
                         "query_time_p95_ms": dbh["latency_p95_ms"], "size_mb": dbh["size_mb"],
                         "disk_usage_percent": host["disk_percent"]},
            "kafka": _service("kafka"), "spark": _service("spark"), "redis": _service("redis"),
        },
        "host": {"cpu_percent": host["cpu_percent"], "memory_percent": host["memory_percent"],
                 "memory_used_mb": host["memory_used_mb"], "memory_total_mb": host["memory_total_mb"]},
        "alerts": alerts,
    }


@ttl_cache(20)
def stream_metrics(db: Session, stream_type: str = "all", time_window: str = "5m") -> Dict[str, Any]:
    now = data_now(db)
    window = parse_window(time_window, timedelta(minutes=5))
    since, secs = now - window, max(window.total_seconds(), 1)
    quality = ops.data_quality(db)
    fresh = quality["freshness"]

    n_orders = int(db.execute(select(func.count()).select_from(Order).where(Order.order_date >= since)).scalar() or 0)
    failed = int(db.execute(select(func.count()).select_from(Order).where(Order.order_date >= since, Order.payment_status == "failed")).scalar() or 0)
    dist = dict(db.execute(select(UserEvent.event_type, func.count()).where(UserEvent.event_timestamp >= since).group_by(UserEvent.event_type)).all())
    sessions = int(db.execute(select(func.count(func.distinct(UserEvent.session_id))).where(UserEvent.event_timestamp >= since)).scalar() or 0)
    new_sessions = int(db.execute(select(func.count(func.distinct(UserEvent.session_id))).where(
        UserEvent.event_timestamp >= since, UserEvent.user_id.is_(None))).scalar() or 0)
    n_events = sum(dist.values())
    alerts = db.execute(select(FraudAlert.alert_type, func.count()).where(FraudAlert.created_at >= since).group_by(FraudAlert.alert_type)).all()
    anomalies = sum(n for _, n in alerts)
    rows_tested = max(quality["rows_tested"], 1)

    return {
        "stream_type": stream_type, "time_window": time_window, "timestamp": datetime.utcnow().isoformat(),
        "transactions": {
            "messages_processed": n_orders, "messages_per_second": round(n_orders / secs, 4), "processing_latency_p95_ms": None,
            "errors": failed, "error_rate": round(failed / n_orders, 4) if n_orders else 0.0,
            "last_message_at": fresh["orders"]["latest"], "lag_seconds": fresh["orders"]["lag_seconds"],
            "quality_metrics": {
                "valid_records": rows_tested - quality["rows_failed"], "invalid_records": quality["rows_failed"],
                "duplicate_records": quality["checks"]["duplicate_emails"], "completeness_score": quality["completeness_score"],
                "schema_violations": quality["checks"]["orders_total_mismatch"],
            },
        },
        "user_events": {
            "messages_processed": n_events, "messages_per_second": round(n_events / secs, 4), "processing_latency_p95_ms": None,
            "errors": 0, "error_rate": 0.0, "last_message_at": fresh["user_events"]["latest"], "lag_seconds": fresh["user_events"]["lag_seconds"],
            "session_tracking": {"active_sessions": sessions, "new_sessions": new_sessions, "expired_sessions": None,
                                 "avg_session_duration_minutes": None},
            "event_distribution": {"page_views": dist.get("page_view", 0), "product_views": dist.get("product_view", 0),
                                   "add_to_cart": dist.get("add_to_cart", 0), "purchases": dist.get("purchase", 0)},
        },
        "data_quality": {
            "overall_health": "poor" if quality["rows_failed"] else "stale" if quality["stale_tables"] else "good",
            "anomaly_detection": {"anomalies_detected": anomalies, "anomaly_rate": round(anomalies / n_orders, 4) if n_orders else 0.0,
                                  "most_common_anomaly": max(alerts, key=lambda r: r[1])[0] if alerts else None},
            "validation_results": {"total_validations": rows_tested, "passed_validations": rows_tested - quality["rows_failed"],
                                   "failed_validations": quality["rows_failed"],
                                   "validation_success_rate": quality["accuracy_score"]},
            "profiling_stats": {"null_percentage": round(1 - quality["completeness_score"], 4),
                                "unique_value_ratio": None, "data_type_consistency": None, "pattern_compliance": None},
        },
    }


def _hourly_revenue(db: Session, now: datetime, days: int = 14) -> Dict[int, List[float]]:
    rows = db.execute(select(Order.order_date, Order.subtotal - Order.discount_amount).where(
        an.PAID, Order.order_date >= now - timedelta(days=days))).all()
    by_hour_day: Dict[tuple, float] = defaultdict(float)
    for ts, rev in rows:
        ts = datetime.fromisoformat(str(ts)[:19]) if isinstance(ts, str) else ts
        by_hour_day[(ts.date(), ts.hour)] += f(rev)
    per_hour: Dict[int, List[float]] = defaultdict(list)
    for d in {k[0] for k in by_hour_day}:
        for h in range(24):
            per_hour[h].append(by_hour_day.get((d, h), 0.0))
    return per_hour


@ttl_cache(30)
def business_metrics(db: Session, metric_category: str = "all", time_window: str = "1h") -> Dict[str, Any]:
    now = data_now(db)
    window = parse_window(time_window, timedelta(hours=1))
    since, prev_since = now - window, now - 2 * window

    def sales(lo: datetime, hi: datetime):
        r = db.execute(select(func.coalesce(func.sum(an.NET_REVENUE), 0), func.count(Order.order_id)).where(
            an.PAID, Order.order_date >= lo, Order.order_date < hi)).one()
        return f(r[0]), int(r[1])

    rev, orders = sales(since, now + timedelta(seconds=1))
    prev_rev, prev_orders = sales(prev_since, since)
    # What this window usually looks like: the same clock window on each of the last 7 days
    typical = [sales(since - timedelta(days=d), now - timedelta(days=d) + timedelta(seconds=1)) for d in range(1, 8)]
    typical_rev = sum(t[0] for t in typical) / len(typical)
    typical_orders = sum(t[1] for t in typical) / len(typical)
    rt = an.realtime_metrics(db, time_window)
    prev_rt_sessions = int(db.execute(select(func.count(func.distinct(UserEvent.session_id))).where(
        UserEvent.event_timestamp >= prev_since, UserEvent.event_timestamp < since)).scalar() or 0)

    top = db.execute(
        select(OrderItem.product_id, Product.name, func.sum(OrderItem.line_total).label("r"), func.sum(OrderItem.quantity).label("u"))
        .join(Order, Order.order_id == OrderItem.order_id).join(Product, Product.product_id == OrderItem.product_id)
        .where(an.PAID, Order.order_date >= since).group_by(OrderItem.product_id, Product.name)
        .order_by(func.sum(OrderItem.line_total).desc()).limit(5)).all()
    pay = Counter(dict(db.execute(select(Order.payment_method, func.count()).where(Order.order_date >= since).group_by(Order.payment_method)).all()))
    pay_total = sum(pay.values()) or 1
    groups = {"credit_card": ["credit_card"], "debit_card": ["debit_card"], "digital_wallet": ["paypal", "apple_pay"],
              "other": ["bank_transfer", "gift_card"]}

    day_start_ts = now.replace(hour=0, minute=0, second=0, microsecond=0)
    hourly_sessions = db.execute(select(func.count(func.distinct(UserEvent.session_id))).where(
        UserEvent.event_timestamp >= day_start_ts).group_by(func.extract("hour", UserEvent.event_timestamp))).scalars().all()

    rfm = an.customer_rfm(db)
    seg = Counter(c["segment"] for c in rfm)
    tiers = dict(db.execute(select(Customer.customer_tier, func.count()).group_by(Customer.customer_tier)).all())
    avg_ltv = f(db.execute(select(func.avg(Customer.lifetime_value)).where(Customer.total_orders > 0)).scalar(), 2)

    prods = db.execute(select(Product.product_id, Product.name, Product.inventory_count).where(Product.status == "active")).all()
    low = [p for p in prods if 0 < p.inventory_count <= 15]
    out = [p for p in prods if p.inventory_count <= 0]
    week_units = f(db.execute(select(func.coalesce(func.sum(OrderItem.quantity), 0)).join(Order, Order.order_id == OrderItem.order_id).where(
        an.PAID, Order.order_date >= now - timedelta(days=7))).scalar())
    total_stock = sum(p.inventory_count for p in prods)

    per_hour = _hourly_revenue(db, now)
    hour_means = {h: float(np.mean(v)) for h, v in per_hour.items()} or {h: 0.0 for h in range(24)}
    remaining = sum(hour_means.get(h, 0.0) for h in range(now.hour + 1, 24))
    today_so_far = f(db.execute(select(func.coalesce(func.sum(an.NET_REVENUE), 0)).where(an.PAID, Order.order_date >= day_start_ts)).scalar())
    cv = float(np.std([sum(v) for v in zip(*per_hour.values())]) / max(np.mean([sum(v) for v in zip(*per_hour.values())]), 1e-9)) if per_hour else 1.0

    def change(a: float, b: float) -> float:
        return round((a - b) / b, 3) if b else 0.0

    return {
        "metric_category": metric_category, "time_window": time_window, "timestamp": datetime.utcnow().isoformat(),
        "data_as_of": now.isoformat(),
        "sales": {
            "revenue": {"current": round(rev, 2), "target": None, "previous_period": round(prev_rev, 2),
                        "typical_for_this_time": round(typical_rev, 2),
                        "growth_rate": change(rev, typical_rev)},  # vs the same window on recent days
            "orders": {"count": orders, "average_value": round(rev / orders, 2) if orders else 0.0,
                       "conversion_rate": rt["sales"]["conversion_rate"], "abandoned_carts": rt["sales"]["abandoned_carts"]},
            "top_products": [{"product_id": r.product_id, "name": r.name, "revenue": round(f(r.r), 2), "units_sold": int(r.u)} for r in top],
            "payment_methods": {k: round(sum(pay.get(m, 0) for m in v) / pay_total, 3) for k, v in groups.items()},
        },
        "customer": {
            "active_users": {"current": rt["traffic"]["active_users"], "peak_today": max(hourly_sessions, default=0),
                             "new_users": rt["traffic"]["new_vs_returning"]["new_users"],
                             "returning_users": rt["traffic"]["new_vs_returning"]["returning_users"]},
            "engagement": {"avg_session_duration_minutes": round(rt["traffic"]["session_duration_avg"] / 60, 2),
                           "pages_per_session": round(rt["traffic"]["page_views"] / rt["traffic"]["active_users"], 2) if rt["traffic"]["active_users"] else 0.0,
                           "bounce_rate": rt["traffic"]["bounce_rate"],
                           "time_on_site_minutes": round(rt["traffic"]["session_duration_avg"] / 60, 2)},
            "customer_acquisition": {"cost_per_acquisition": None, "lifetime_value": avg_ltv,
                                     "acquisition_channels": rt["traffic"]["traffic_sources"]},
            "segmentation": {"high_value_customers": tiers.get("platinum", 0) + tiers.get("gold", 0),
                             "at_risk_customers": seg.get("At Risk", 0), "new_customers": seg.get("New Customers", 0),
                             "loyal_customers": seg.get("Loyal Customers", 0) + seg.get("Champions", 0)},
        },
        "inventory": {
            "stock_levels": {"total_products": len(prods), "in_stock": len(prods) - len(out), "low_stock": len(low),
                             "out_of_stock": len(out), "overstock": sum(p.inventory_count > 300 for p in prods)},
            "movement": {"units_sold": int(week_units), "units_restocked": None,
                         "inventory_turnover": round(week_units * 52 / max(total_stock, 1), 2),
                         "sell_through_rate": round(week_units / (week_units + total_stock), 4) if total_stock else 0.0},
            "alerts": [{"type": "out_of_stock" if p.inventory_count <= 0 else "low_stock", "product_id": p.product_id, "name": p.name,
                        "current_stock": p.inventory_count, "reorder_point": 15, "urgency": "high" if p.inventory_count <= 0 else "medium"}
                       for p in (out + low)[:4]],
            "forecasting": {"predicted_stockouts_7d": rt["inventory"]["stockout_risk"], "reorder_recommendations": len(low) + len(out),
                            "seasonal_demand_indicator": None},
        },
        "marketing": {"available": False, "reason": "No marketing or campaign data source is configured"},
        "trends": {
            "vs_previous_period": {"revenue_change": change(rev, typical_rev), "orders_change": change(orders, typical_orders),
                                   "customer_change": change(rt["traffic"]["active_users"], prev_rt_sessions), "conversion_change": None},
            "vs_target": {"available": False, "reason": "No revenue or order targets are stored"},
            "forecasts": {"next_hour_revenue": round(hour_means.get((now.hour + 1) % 24, 0.0), 2),
                          "end_of_day_projection": round(today_so_far + remaining, 2),
                          "confidence_level": round(max(0.0, min(1.0, 1 - cv / 2)), 2)},
        },
    }


@ttl_cache(15)
def activity(db: Session, hours: int = 24, limit: int = 10) -> Dict[str, Any]:
    """Recent orders, orders/revenue per hour and the regional split over the last `hours`."""
    now = data_now(db)
    since = now - timedelta(hours=hours)

    recent = db.execute(
        select(Order.order_id, Order.order_date, Order.total_amount, Order.order_status, Order.payment_status,
               Order.payment_method, Order.channel, Customer.region)
        .join(Customer, Customer.user_id == Order.user_id).order_by(Order.order_date.desc()).limit(limit)
    ).all()

    rows = db.execute(
        select(Order.order_date, Order.subtotal - Order.discount_amount, Customer.region)
        .join(Customer, Customer.user_id == Order.user_id).where(an.PAID, Order.order_date >= since)
    ).all()
    by_hour: Dict[datetime, List[float]] = {}
    for h in range(hours + 1):
        slot = (now - timedelta(hours=hours - h)).replace(minute=0, second=0, microsecond=0)
        by_hour[slot] = [0, 0.0]
    regions: Counter = Counter()
    for ts, rev, region in rows:
        ts = datetime.fromisoformat(str(ts)[:19]) if isinstance(ts, str) else ts
        slot = ts.replace(minute=0, second=0, microsecond=0)
        if slot in by_hour:
            by_hour[slot][0] += 1
            by_hour[slot][1] += f(rev)
        regions[region or "Unknown"] += 1
    total = sum(regions.values()) or 1
    return {
        "generated_at": datetime.utcnow().isoformat(), "data_as_of": now.isoformat(), "hours": hours,
        "recent_transactions": [{
            "transaction_id": r.order_id, "time": str(r.order_date), "amount": f(r.total_amount, 2),
            "status": r.order_status, "payment_status": r.payment_status,
            "payment_method": (r.payment_method or "").replace("_", " ").title(), "channel": r.channel, "region": r.region,
        } for r in recent],
        "hourly": [{"hour": slot.strftime("%H:00"), "date": slot.date().isoformat(), "orders": v[0], "revenue": round(v[1], 2)}
                   for slot, v in sorted(by_hour.items())],
        "regions": [{"region": k, "transactions": n, "percentage": round(100 * n / total, 1)} for k, n in regions.most_common()],
    }


def performance_benchmarks(db: Session, benchmark_type: str = "all", time_range: str = "24h") -> Dict[str, Any]:
    api, dbh, host = ops.api_metrics(), ops.database_health(db, 8), ops.host_metrics()
    now = datetime.utcnow()
    probes = {name: ops.probe(name) for name in ("redis", "kafka", "spark")}

    def dep(name: str) -> Dict[str, Any]:
        p = probes[name]
        return {"status": "healthy" if p["status"] == "healthy" else "not_configured", "uptime": None}

    sla = api["sla_compliance"]
    return {
        "benchmark_type": benchmark_type, "time_range": time_range, "timestamp": now.isoformat(),
        "period": {"start": datetime.fromtimestamp(ops.STARTED_AT).isoformat(), "end": now.isoformat(),
                   "note": "API metrics cover the time since this API process started"},
        "latency": {
            "api_response_times": {"p50_ms": api["p50_ms"], "p90_ms": api["p90_ms"], "p95_ms": api["p95_ms"], "p99_ms": api["p99_ms"],
                                   "max_ms": None, "sla_target_ms": ops.SLA_TARGET_MS, "sla_compliance": sla},
            "database_query_times": {"p50_ms": dbh["latency_p50_ms"], "p90_ms": dbh["latency_p95_ms"], "p95_ms": dbh["latency_p95_ms"],
                                     "p99_ms": dbh["latency_max_ms"], "slow_queries": None},
            "external_service_latency": {"available": False, "reason": "No external services are configured"},
        },
        "throughput": {
            "requests_per_second": {"current": api["requests_per_second"], "peak": None, "average": api["requests_per_second"],
                                    "capacity_limit": None, "utilization": None},
            "data_processing": {"available": False, "reason": "No stream processing is configured"},
            "concurrent_users": {"current": api["requests_in_progress"], "peak": None, "capacity": None, "sessions_per_minute": None},
        },
        "reliability": {
            "uptime": {"current_uptime_seconds": api["uptime_seconds"], "uptime_percentage": None, "sla_target": 99.9,
                       "incidents_count": None, "mttr_minutes": None},
            "error_rates": {"http_4xx_rate": api["error_rate_4xx"], "http_5xx_rate": api["error_rate_5xx"],
                            "timeout_rate": None, "database_error_rate": None},
            "service_dependencies": {"database": {"status": dbh["status"], "uptime": None}, "redis": dep("redis"),
                                     "kafka": dep("kafka"), "spark": dep("spark")},
        },
        "sla_compliance": {"overall_score": sla, "violations_count": None, "critical_violations": None},
    }
