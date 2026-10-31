"""
Operational measurements.

Real sources only:
  * this machine (psutil), the database (latency, size, freshness, integrity checks)
  * the API's own request metrics (the Prometheus histogram kept by the middleware)
  * TCP probes of Redis / Kafka / Spark

Components that are not configured are reported as "unavailable" with a reason; their
numeric fields are None rather than invented.
"""

import os
import socket
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import psutil
from prometheus_client import REGISTRY
from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.orm import Session

from src.database.models import Customer, FraudAlert, Order, OrderItem, Product, UserEvent

from .common import data_now, f, ttl_cache

STARTED_AT = time.time()
SLA_TARGET_MS = 500
SERVICES = {  # name -> (env var, default host, default port)
    "redis": ("ECAP_REDIS_URL", "localhost", 6379),
    "kafka": ("KAFKA_BOOTSTRAP_SERVERS", "localhost", 9092),
    "spark": ("SPARK_MASTER_HOST", "localhost", 7077),
}
UNAVAILABLE = {
    "kafka": "Kafka is not configured in this deployment",
    "spark": "No Spark cluster is configured in this deployment",
    "redis": "Redis is not running; responses are not cached",
}


# ----------------------------------------------------------------------------- host / API
_MEMO: Dict[str, Any] = {}


def _memo(key: str, seconds: float, fn):
    """Tiny in-process memo for cheap-but-slow probes (host stats, TCP checks)."""
    hit = _MEMO.get(key)
    if hit and time.time() - hit[0] < seconds:
        return hit[1]
    value = fn()
    _MEMO[key] = (time.time(), value)
    return value


def host_metrics() -> Dict[str, Any]:
    return _memo("host", 5, _host_metrics)


def _host_metrics() -> Dict[str, Any]:
    vm, disk = psutil.virtual_memory(), psutil.disk_usage(os.path.abspath(os.sep))
    proc = psutil.Process()
    return {
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": vm.percent, "memory_used_mb": round(vm.used / 1e6), "memory_total_mb": round(vm.total / 1e6),
        "disk_percent": disk.percent, "process_memory_mb": round(proc.memory_info().rss / 1e6),
        "process_threads": proc.num_threads(), "uptime_seconds": int(time.time() - STARTED_AT),
    }


def api_metrics() -> Dict[str, Any]:
    """Aggregate the Prometheus HTTP metrics recorded since the API started."""
    buckets: Dict[float, float] = defaultdict(float)
    total_requests, status_classes, duration_sum = 0.0, defaultdict(float), 0.0
    for family in REGISTRY.collect():
        if family.name == "http_request_duration_seconds":
            for s in family.samples:
                if s.name.endswith("_bucket"):
                    le = float("inf") if s.labels["le"] == "+Inf" else float(s.labels["le"])
                    buckets[le] += s.value
                elif s.name.endswith("_sum"):
                    duration_sum += s.value
        elif family.name == "http_requests":
            for s in family.samples:
                if s.name.endswith("_total"):
                    total_requests += s.value
                    status_classes[s.labels.get("status", "0")[:1]] += s.value

    def quantile(q: float) -> Optional[float]:
        if not buckets or not total_requests:
            return None
        ordered = sorted(buckets.items())
        count = ordered[-1][1]
        target, prev_le, prev_c = q * count, 0.0, 0.0
        for le, c in ordered:
            if c >= target:
                if le == float("inf"):
                    return round(prev_le * 1000, 1)
                span = c - prev_c
                frac = (target - prev_c) / span if span else 0
                return round((prev_le + frac * (le - prev_le)) * 1000, 1)
            prev_le, prev_c = le, c
        return None

    in_progress = 0.0
    for family in REGISTRY.collect():
        if family.name == "http_requests_in_progress":
            in_progress = sum(s.value for s in family.samples)
    uptime = max(time.time() - STARTED_AT, 1)
    sla_bucket = max((le for le in buckets if le <= SLA_TARGET_MS / 1000), default=None)
    within = buckets[sla_bucket] if sla_bucket is not None else 0.0
    return {
        "total_requests": int(total_requests),
        "requests_per_second": round(total_requests / uptime, 3),
        "avg_ms": round(duration_sum / total_requests * 1000, 1) if total_requests else None,
        "p50_ms": quantile(0.5), "p90_ms": quantile(0.9), "p95_ms": quantile(0.95), "p99_ms": quantile(0.99),
        "error_rate_5xx": round(status_classes["5"] / total_requests, 4) if total_requests else 0.0,
        "error_rate_4xx": round(status_classes["4"] / total_requests, 4) if total_requests else 0.0,
        "sla_compliance": round(within / total_requests, 4) if total_requests else None,
        "uptime_seconds": int(uptime), "requests_in_progress": int(in_progress),
    }


def endpoint_stats(limit: int = 10) -> List[Dict[str, Any]]:
    """Per-endpoint request count, mean/p95 latency and error rate since the API started."""
    count: Dict[str, float] = defaultdict(float)
    total_s: Dict[str, float] = defaultdict(float)
    errors: Dict[str, float] = defaultdict(float)
    buckets: Dict[str, Dict[float, float]] = defaultdict(lambda: defaultdict(float))
    for family in REGISTRY.collect():
        if family.name == "http_request_duration_seconds":
            for s in family.samples:
                ep = s.labels.get("endpoint", "?")
                if s.name.endswith("_bucket"):
                    le = float("inf") if s.labels["le"] == "+Inf" else float(s.labels["le"])
                    buckets[ep][le] += s.value
                elif s.name.endswith("_count"):
                    count[ep] += s.value
                    if s.labels.get("status", "0")[:1] in ("4", "5"):
                        errors[ep] += s.value
                elif s.name.endswith("_sum"):
                    total_s[ep] += s.value

    def p95(ep: str) -> Optional[float]:
        ordered = sorted(buckets[ep].items())
        if not ordered or not count[ep]:
            return None
        target, prev_le, prev_c = 0.95 * ordered[-1][1], 0.0, 0.0
        for le, c in ordered:
            if c >= target:
                if le == float("inf"):
                    return round(prev_le * 1000, 1)
                frac = (target - prev_c) / (c - prev_c) if c > prev_c else 0
                return round((prev_le + frac * (le - prev_le)) * 1000, 1)
            prev_le, prev_c = le, c
        return None

    return [
        {"endpoint": ep, "requests": int(n), "avg_ms": round(total_s[ep] / n * 1000, 1), "p95_ms": p95(ep),
         "error_rate": round(errors[ep] / n, 4)}
        for ep, n in sorted(count.items(), key=lambda kv: -kv[1])[:limit] if n
    ]


# ----------------------------------------------------------------------------- database
def _db_kind(db: Session) -> str:
    return db.get_bind().dialect.name


def database_health(db: Session, pings: int = 5) -> Dict[str, Any]:
    samples = []
    for _ in range(pings):
        t0 = time.perf_counter()
        db.execute(text("SELECT 1")).scalar()
        samples.append((time.perf_counter() - t0) * 1000)
    engine, kind = db.get_bind(), _db_kind(db)
    pool = getattr(engine, "pool", None)
    size_mb = None
    try:
        if kind == "postgresql":
            size_mb = round(db.execute(text("SELECT pg_database_size(current_database())")).scalar() / 1e6, 1)
        elif kind == "sqlite" and engine.url.database:
            size_mb = round(os.path.getsize(engine.url.database) / 1e6, 1)
    except Exception:
        pass
    checked_out = pool.checkedout() if hasattr(pool, "checkedout") else None
    pool_size = (pool.size() + getattr(pool, "_max_overflow", 0)) if hasattr(pool, "size") else None
    return {
        "status": "healthy", "engine": kind,
        "latency_p50_ms": round(float(np.percentile(samples, 50)), 2), "latency_p95_ms": round(float(np.percentile(samples, 95)), 2),
        "latency_max_ms": round(max(samples), 2), "size_mb": size_mb,
        "pool_checked_out": checked_out, "pool_capacity": pool_size,
        "pool_usage": round(checked_out / pool_size, 3) if checked_out is not None and pool_size else None,
    }


def probe(service: str) -> Dict[str, Any]:
    return _memo(f"probe:{service}", 15, lambda: _probe(service))


def _probe(service: str) -> Dict[str, Any]:
    """TCP probe of a service's configured host:port."""
    env, host, port = SERVICES[service]
    target = os.environ.get(env, "")
    if "://" in target:
        target = target.split("://", 1)[1].split("/")[0]
    if ":" in target:
        host, _, p = target.partition(":")
        port = int(p) if p.isdigit() else port
    t0 = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=0.3):
            return {"status": "healthy", "latency_ms": round((time.perf_counter() - t0) * 1000, 1), "target": f"{host}:{port}"}
    except OSError:
        return {"status": "unavailable", "reason": UNAVAILABLE[service], "target": f"{host}:{port}"}


@ttl_cache(30)
def freshness(db: Session) -> Dict[str, Any]:
    now = datetime.utcnow()

    def newest(col) -> Optional[datetime]:
        v = db.execute(select(func.max(col))).scalar()
        return datetime.fromisoformat(str(v)[:19]) if v else None

    out = {}
    for name, col, limit_h in (("orders", Order.order_date, 24), ("user_events", UserEvent.event_timestamp, 6),
                               ("fraud_alerts", FraudAlert.created_at, 72)):
        latest = newest(col)
        lag = (now - latest).total_seconds() if latest else None
        out[name] = {"latest": latest.isoformat() if latest else None, "lag_seconds": round(lag) if lag is not None else None,
                     "stale": lag is None or lag > limit_h * 3600, "threshold_hours": limit_h}
    return out


# ----------------------------------------------------------------------------- data quality
@ttl_cache(120)
def data_quality(db: Session) -> Dict[str, Any]:
    """Integrity and completeness checks over the stored data."""
    n_customers = db.execute(select(func.count()).select_from(Customer)).scalar() or 0
    n_orders = db.execute(select(func.count()).select_from(Order)).scalar() or 0
    n_items = db.execute(select(func.count()).select_from(OrderItem)).scalar() or 0
    n_events = db.execute(select(func.count()).select_from(UserEvent)).scalar() or 0
    n_products = db.execute(select(func.count()).select_from(Product)).scalar() or 0

    def count(stmt) -> int:
        return int(db.execute(stmt).scalar() or 0)

    checks = {
        "customers_missing_email": count(select(func.count()).select_from(Customer).where(or_(Customer.email.is_(None), Customer.email == ""))),
        "duplicate_emails": count(select(func.count()).select_from(
            select(Customer.email).group_by(Customer.email).having(func.count() > 1).subquery())),
        "orders_total_mismatch": count(select(func.count()).select_from(Order).where(
            func.abs(Order.total_amount - (Order.subtotal - Order.discount_amount + Order.tax_amount + Order.shipping_cost)) > 0.02)),
        "orders_without_items": count(select(func.count()).select_from(Order).where(
            ~select(OrderItem.order_id).where(OrderItem.order_id == Order.order_id).exists())),
        "orders_before_registration": count(select(func.count()).select_from(Order).join(
            Customer, Customer.user_id == Order.user_id).where(Order.order_date < Customer.registration_date)),
        "future_dated_orders": count(select(func.count()).select_from(Order).where(Order.order_date > datetime.utcnow() + timedelta(minutes=5))),
        "orders_missing_payment_method": count(select(func.count()).select_from(Order).where(
            Order.payment_method.is_(None), Order.payment_status == "paid")),
        "products_negative_margin": count(select(func.count()).select_from(Product).where(Product.cost > Product.price)),
        "events_without_session": count(select(func.count()).select_from(UserEvent).where(UserEvent.session_id.is_(None))),
    }
    tested = {"customers_missing_email": n_customers, "duplicate_emails": n_customers, "orders_total_mismatch": n_orders,
              "orders_without_items": n_orders, "orders_before_registration": n_orders, "future_dated_orders": n_orders,
              "orders_missing_payment_method": n_orders, "products_negative_margin": n_products, "events_without_session": n_events}
    failed_rows = sum(checks.values())
    tested_rows = sum(tested.values()) or 1
    required_nulls = checks["customers_missing_email"] + checks["orders_missing_payment_method"] + checks["events_without_session"]
    required_total = n_customers + n_orders + n_events or 1
    fresh = freshness(db)
    stale_tables = [name for name, v in fresh.items() if v["stale"]]
    return {
        "checks": checks, "rows_tested": tested_rows, "rows_failed": failed_rows,
        "completeness_score": round(1 - required_nulls / required_total, 4),
        "accuracy_score": round(1 - failed_rows / tested_rows, 4),
        "overall_quality_score": round((1 - required_nulls / required_total + 1 - failed_rows / tested_rows) / 2, 4),
        "freshness_violations": len(stale_tables), "stale_tables": stale_tables,
        "tables": {"customers": n_customers, "products": n_products, "orders": n_orders, "order_items": n_items, "user_events": n_events},
        "freshness": fresh,
    }


# ----------------------------------------------------------------------------- alerts
def operational_alerts(db: Session) -> List[Dict[str, Any]]:
    """Alerts derived from current, measured conditions."""
    now = datetime.utcnow().isoformat()
    host, api, dbh, dq = host_metrics(), api_metrics(), database_health(db, 3), data_quality(db)
    alerts: List[Dict[str, Any]] = []

    def add(severity: str, category: str, message: str, status: str = "active"):
        alerts.append({"timestamp": now, "severity": severity, "category": category, "message": message,
                       "status": status, "assigned_to": None})

    if host["cpu_percent"] > 85:
        add("critical", "system", f"CPU usage {host['cpu_percent']:.0f}%")
    elif host["cpu_percent"] > 70:
        add("warning", "system", f"CPU usage {host['cpu_percent']:.0f}%")
    if host["memory_percent"] > 90:
        add("critical", "system", f"Memory usage {host['memory_percent']:.0f}%")
    elif host["memory_percent"] > 80:
        add("warning", "system", f"Memory usage {host['memory_percent']:.0f}%")
    if host["disk_percent"] > 90:
        add("warning", "system", f"Disk usage {host['disk_percent']:.0f}%")
    if api["p95_ms"] and api["p95_ms"] > SLA_TARGET_MS:
        add("warning", "performance", f"API p95 latency {api['p95_ms']:.0f} ms exceeds the {SLA_TARGET_MS} ms target")
    if api["error_rate_5xx"] > 0.01:
        add("critical", "performance", f"HTTP 5xx rate {api['error_rate_5xx'] * 100:.1f}%")
    if dbh["latency_p95_ms"] > 150:
        add("warning", "performance", f"Database round-trip p95 {dbh['latency_p95_ms']:.0f} ms")
    for name in dq["stale_tables"]:
        lag_h = (dq["freshness"][name]["lag_seconds"] or 0) / 3600
        add("warning", "system", f"{name} has no new data for {lag_h:.1f} h")
    for check, n in dq["checks"].items():
        if n:
            add("warning", "system", f"Data integrity: {check.replace('_', ' ')} = {n}")
    for svc in ("kafka", "redis", "spark"):
        if probe(svc)["status"] != "healthy":
            add("info", "system", UNAVAILABLE[svc])
    backlog = db.execute(select(func.count()).select_from(FraudAlert).where(FraudAlert.status.in_(("open", "investigating")))).scalar() or 0
    critical = db.execute(select(func.count()).select_from(FraudAlert).where(
        FraudAlert.status.in_(("open", "investigating")), FraudAlert.priority == "critical")).scalar() or 0
    if critical:
        add("critical", "fraud", f"{critical} critical fraud alerts awaiting investigation")
    elif backlog > 25:
        add("warning", "fraud", f"Fraud investigation backlog: {backlog} alerts")
    return alerts
