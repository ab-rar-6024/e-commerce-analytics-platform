"""
Fraud queries over the fraud_alerts table and the order outcomes.

Detection quality (accuracy, precision, recall, F1, AUC) is measured by comparing
the alerts with each order's known outcome (`orders.is_fraudulent`), so these are
measurements of the rules engine in src/database/fraud_scoring.py - not estimates.
"""

import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.database.fraud_scoring import RULES, priority_for
from src.database.models import Customer, FraudAlert, Order

from .common import data_now, f, parse_window, ttl_cache

INVESTIGATION_COST = 25.0  # assumed analyst cost per alert (USD); a business assumption
SLA_HOURS = 24
PRIORITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}
CONFIRMED = ("resolved", "escalated")  # alert outcomes that mean fraud was confirmed (with the label)


def _dt(v: Any) -> Optional[datetime]:
    if v is None or isinstance(v, datetime):
        return v
    return datetime.fromisoformat(str(v)[:19])


def _iso(v: Any) -> Optional[str]:
    v = _dt(v)
    return v.isoformat() if v else None


@ttl_cache(30)
def load_alerts(db: Session, since: Optional[datetime] = None, until: Optional[datetime] = None) -> List[Dict[str, Any]]:
    stmt = (
        select(FraudAlert, Order.is_fraudulent, Order.order_date, Order.channel, Order.currency, Customer.region,
               Customer.registration_date)
        .join(Order, Order.order_id == FraudAlert.order_id, isouter=True)
        .join(Customer, Customer.user_id == FraudAlert.user_id)
    )
    if since:
        stmt = stmt.where(FraudAlert.created_at >= since)
    if until:
        stmt = stmt.where(FraudAlert.created_at < until)
    out = []
    for a, is_fraud, order_date, channel, currency, region, reg in db.execute(stmt.order_by(FraudAlert.created_at.desc())).all():
        out.append({
            "alert_id": a.alert_id, "order_id": a.order_id, "user_id": a.user_id, "alert_type": a.alert_type,
            "priority": a.priority, "status": a.status, "risk_score": f(a.risk_score, 3), "amount": f(a.amount, 2),
            "description": a.description, "rules": a.rules_triggered or [], "assigned_to": a.assigned_to,
            "created_at": _dt(a.created_at), "updated_at": _dt(a.updated_at), "resolved_at": _dt(a.resolved_at),
            "is_fraud": bool(is_fraud), "order_date": _dt(order_date), "channel": channel, "currency": currency or "USD",
            "region": region or "Unknown", "account_age_days": (_dt(a.created_at) - _dt(reg)).days if reg else None,
        })
    return out


def _hours(a: Dict[str, Any]) -> Optional[float]:
    return (a["resolved_at"] - a["created_at"]).total_seconds() / 3600 if a["resolved_at"] else None


def _alert_view(a: Dict[str, Any]) -> Dict[str, Any]:
    hours = _hours(a)
    return {
        "alert_id": a["alert_id"], "transaction_id": a["order_id"], "customer_id": a["user_id"],
        "alert_type": a["alert_type"], "status": a["status"], "priority": a["priority"], "risk_score": a["risk_score"],
        "amount": a["amount"], "currency": a["currency"], "channel": a["channel"], "region": a["region"],
        "created_at": _iso(a["created_at"]), "updated_at": _iso(a["updated_at"]), "description": a["description"],
        "investigation": {"assigned_to": a["assigned_to"], "resolved_at": _iso(a["resolved_at"]),
                          "investigation_time_hours": round(hours, 2) if hours is not None else None},
        "fraud_indicators": {"rules_triggered": a["rules"], "account_age_days": a["account_age_days"],
                             "confirmed_fraud": a["is_fraud"] if a["status"] in ("resolved", "false_positive", "escalated") else None},
        "financial_impact": {"amount_at_risk": a["amount"] if a["status"] in ("open", "investigating", "escalated") else 0.0,
                             "amount_blocked": a["amount"] if a["is_fraud"] and a["status"] in CONFIRMED else 0.0},
    }


def _sla(closed: List[Dict[str, Any]], everything: List[Dict[str, Any]]) -> Dict[str, float]:
    hours = [h for h in (_hours(a) for a in closed) if h is not None]
    return {
        "on_time_resolution": round(sum(h <= SLA_HOURS for h in hours) / len(hours), 3) if hours else 0.0,
        "avg_resolution_time_hours": round(sum(hours) / len(hours), 2) if hours else 0.0,
        "escalation_rate": round(sum(a["status"] == "escalated" for a in everything) / len(everything), 3) if everything else 0.0,
    }


def list_alerts(db: Session, status: Optional[str], priority: Optional[str], alert_type: Optional[str],
                risk_score_min: Optional[float], start_date: Optional[str], end_date: Optional[str],
                assigned_to: Optional[str], page: int, size: int) -> Dict[str, Any]:
    since = datetime.strptime(start_date[:10], "%Y-%m-%d") if start_date else None
    until = datetime.strptime(end_date[:10], "%Y-%m-%d") + timedelta(days=1) if end_date else None
    rows = load_alerts(db, since, until)
    if status:
        rows = [a for a in rows if a["status"] == status]
    if priority:
        rows = [a for a in rows if a["priority"] == priority]
    if alert_type:
        rows = [a for a in rows if a["alert_type"] == alert_type]
    if risk_score_min is not None:
        rows = [a for a in rows if a["risk_score"] >= risk_score_min]
    if assigned_to:
        rows = [a for a in rows if a["assigned_to"] == assigned_to]
    rows = sorted(rows, key=lambda a: (PRIORITY_ORDER[a["priority"]], -a["created_at"].timestamp()))
    total, pages = len(rows), max(math.ceil(len(rows) / size), 1)
    chunk = rows[(page - 1) * size : page * size]

    closed = [a for a in rows if a["status"] in ("resolved", "false_positive")]
    recent = [a for a in rows if a["created_at"] >= data_now(db) - timedelta(days=7)]
    hours = Counter(a["created_at"].hour for a in rows)
    regions = Counter(a["region"] for a in rows)
    return {
        "alerts": [_alert_view(a) for a in chunk],
        "filters": {"status": status, "priority": priority, "alert_type": alert_type, "risk_score_min": risk_score_min,
                    "start_date": start_date, "end_date": end_date, "assigned_to": assigned_to},
        "pagination": {"page": page, "size": size, "total": total, "pages": pages},
        "summary": {
            "total_alerts": total,
            "total_amount_at_risk": round(sum(a["amount"] for a in rows if a["status"] in ("open", "investigating", "escalated")), 2),
            "average_risk_score": round(sum(a["risk_score"] for a in rows) / total, 3) if total else 0.0,
            "priority_distribution": {p: sum(a["priority"] == p for a in rows) for p in PRIORITY_ORDER},
            "status_distribution": {s: sum(a["status"] == s for a in rows) for s in ("open", "investigating", "resolved", "escalated", "false_positive")},
            "sla_performance": _sla(closed, rows),
        },
        "insights": {
            "trending_alert_types": [t for t, _ in Counter(a["alert_type"] for a in recent).most_common(3)],
            "peak_hours": [f"{h:02d}:00" for h, _ in hours.most_common(2)],
            "highest_risk_region": regions.most_common(1)[0][0] if regions else None,
            "false_positive_rate": round(sum(a["status"] == "false_positive" for a in closed) / len(closed), 3) if closed else 0.0,
        },
    }


# ------------------------------------------------------------------ detection quality
@ttl_cache(120)
def _order_labels(db: Session, since: datetime, until: datetime) -> List[Dict[str, Any]]:
    rows = db.execute(select(Order.order_id, Order.is_fraudulent, Order.order_date).where(
        Order.order_date >= since, Order.order_date < until)).all()
    return [{"order_id": r.order_id, "fraud": bool(r.is_fraudulent)} for r in rows]


def _auc(scores: np.ndarray, labels: np.ndarray) -> float:
    """Area under the ROC curve via the rank-sum (Mann-Whitney U) identity."""
    pos, neg = int(labels.sum()), int((~labels).sum())
    if pos == 0 or neg == 0:
        return 0.0
    order = np.argsort(scores, kind="mergesort")
    ranks = np.empty(len(scores))
    sorted_scores = scores[order]
    i = 0
    while i < len(scores):  # average ranks for ties
        j = i
        while j + 1 < len(scores) and sorted_scores[j + 1] == sorted_scores[i]:
            j += 1
        ranks[order[i : j + 1]] = (i + j) / 2 + 1
        i = j + 1
    return float((ranks[labels].sum() - pos * (pos + 1) / 2) / (pos * neg))


def _confusion(labels: List[Dict[str, Any]], flagged: set) -> Dict[str, float]:
    tp = sum(1 for o in labels if o["fraud"] and o["order_id"] in flagged)
    fp = sum(1 for o in labels if not o["fraud"] and o["order_id"] in flagged)
    fn = sum(1 for o in labels if o["fraud"] and o["order_id"] not in flagged)
    tn = len(labels) - tp - fp - fn
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {
        "accuracy": round((tp + tn) / len(labels), 4) if labels else 0.0,
        "precision": round(precision, 3), "recall": round(recall, 3),
        "f1_score": round(2 * precision * recall / (precision + recall), 3) if precision + recall else 0.0,
        "true_positives": tp, "false_positives": fp, "false_negatives": fn,
    }


@ttl_cache(120)
def detection_models(db: Session, time_range: str = "90d") -> Dict[str, Any]:
    now = data_now(db)
    since = now - parse_window(time_range, timedelta(days=90))
    labels = _order_labels(db, since, now + timedelta(days=1))
    alerts = load_alerts(db, since)
    today = [a for a in load_alerts(db, now - timedelta(days=1))]

    models = []
    for rule, (weight, _, description) in RULES.items():
        flagged = {a["order_id"] for a in alerts if rule in a["rules"]}
        stats = _confusion(labels, flagged)
        models.append({
            "model_id": rule, "name": rule.replace("_", " ").title(), "version": "rules-v1", "type": "rule",
            "description": description, "weight": weight, "status": "active", **stats,
            "alerts_in_period": len(flagged), "predictions_today": sum(rule in a["rules"] for a in today),
            "error_rate": round(1 - stats["precision"], 3) if flagged else 0.0,
        })
    combined = _confusion(labels, {a["order_id"] for a in alerts})
    multi = sum(len(a["rules"]) >= 2 for a in alerts)
    return {
        "model_type": "rules", "time_range": time_range, "generated_at": datetime.utcnow().isoformat(), "models": models,
        "ensemble_performance": {
            "combined_accuracy": combined["accuracy"], "combined_precision": combined["precision"],
            "combined_recall": combined["recall"], "combined_f1": combined["f1_score"],
            "model_agreement_rate": round(multi / len(alerts), 3) if alerts else 0.0,
            "orders_evaluated": len(labels), "confirmed_fraud_orders": sum(o["fraud"] for o in labels),
        },
    }


# ------------------------------------------------------------------ dashboard
@ttl_cache(60)
def fraud_dashboard(db: Session, time_range: str = "24h") -> Dict[str, Any]:
    now = data_now(db)
    window = parse_window(time_range, timedelta(hours=24))
    since, prev_since = now - window, now - 2 * window
    alerts = load_alerts(db, since)
    labels = _order_labels(db, since, now + timedelta(days=1))
    total_tx = len(labels)

    status = Counter(a["status"] for a in alerts)
    closed = [a for a in alerts if a["status"] in ("resolved", "false_positive")]
    confirmed = [a for a in alerts if a["is_fraud"] and a["status"] in CONFIRMED]
    blocked = sum(a["amount"] for a in confirmed)
    exposure = sum(a["amount"] for a in alerts)
    costs = len(alerts) * INVESTIGATION_COST
    latencies = [(a["created_at"] - a["order_date"]).total_seconds() * 1000 for a in alerts if a["order_date"]]

    flagged = {a["order_id"] for a in alerts}
    quality = _confusion(labels, flagged)
    score_of = {a["order_id"]: a["risk_score"] for a in alerts}
    auc = _auc(np.array([score_of.get(o["order_id"], 0.0) for o in labels]), np.array([o["fraud"] for o in labels], dtype=bool)) if labels else 0.0

    hourly = Counter(a["created_at"].hour for a in alerts)
    by_day_alerts = Counter(a["created_at"].date().isoformat() for a in alerts)
    orders_by_day = {str(d): n for d, n in db.execute(
        select(func.date(Order.order_date), func.count(Order.order_id)).where(Order.order_date >= since)
        .group_by(func.date(Order.order_date))).all()}
    pats = Counter(r for a in alerts for r in a["rules"])
    prev_pats = Counter(r for a in load_alerts(db, prev_since, since) for r in a["rules"])

    region_alerts = Counter(a["region"] for a in alerts)
    region_orders = dict(db.execute(select(Customer.region, func.count(Order.order_id)).join(
        Customer, Customer.user_id == Order.user_id).where(Order.order_date >= since).group_by(Customer.region)).all())

    workload = defaultdict(lambda: {"open": 0, "closed": 0})
    for a in alerts:
        if a["assigned_to"]:
            workload[a["assigned_to"]]["closed" if a["status"] in ("resolved", "false_positive") else "open"] += 1
    resolved_hours = [h for h in (_hours(a) for a in closed) if h is not None]

    return {
        "time_range": time_range, "generated_at": datetime.utcnow().isoformat(),
        "overview": {
            "total_transactions": total_tx, "flagged_transactions": len(alerts), "confirmed_fraud": len(confirmed),
            "false_positives": status["false_positive"],
            "fraud_rate": round(len(alerts) / total_tx, 4) if total_tx else 0.0,  # share of transactions flagged
            "confirmed_fraud_rate": round(len(confirmed) / total_tx, 4) if total_tx else 0.0,
            "false_positive_rate": round(status["false_positive"] / len(closed), 3) if closed else 0.0,
            "total_blocked_amount": round(blocked, 2), "total_loss_prevented": round(blocked, 2),
            "investigation_backlog": status["open"] + status["investigating"],
        },
        "alerts": {
            "total": len(alerts), "open": status["open"], "investigating": status["investigating"], "resolved": status["resolved"],
            "escalated": status["escalated"], "false_positives": status["false_positive"],
            "by_priority": {p: sum(a["priority"] == p for a in alerts) for p in PRIORITY_ORDER},
            "by_type": {t: sum(a["alert_type"] == t for a in alerts) for t in ("velocity", "location", "amount", "pattern", "ml_anomaly", "device", "behavioral")},
        },
        "performance": {
            "detection_latency_ms": int(sum(latencies) / len(latencies)) if latencies else 0,
            "model_accuracy": quality["accuracy"], "precision": quality["precision"], "recall": quality["recall"],
            "f1_score": quality["f1_score"], "auc_roc": round(auc, 3),
        },
        "financial_impact": {
            "total_exposure": round(exposure, 2), "amount_blocked": round(blocked, 2), "loss_prevented": round(blocked, 2),
            "investigation_costs": round(costs, 2), "roi": round((blocked - costs) / costs, 2) if costs else 0.0,
            "assumptions": f"investigation cost ${INVESTIGATION_COST:.0f}/alert",
        },
        "trends": {
            "hourly_alerts": [{"hour": h, "alerts": hourly.get(h, 0), "count": hourly.get(h, 0)} for h in range(24)],
            "daily_fraud_rate": [{"date": d, "alerts": n, "alert_rate": round(n / orders_by_day[d], 4) if orders_by_day.get(d) else 0.0}
                                 for d, n in sorted(by_day_alerts.items())],
            "top_fraud_patterns": [
                {"pattern": p, "frequency": n, "count": n, "percentage": round(100 * n / max(sum(pats.values()), 1), 1),
                 "severity": priority_for(float(np.mean([a["risk_score"] for a in alerts if p in a["rules"]])))}
                for p, n in pats.most_common(4)
            ],
        },
        "geographical_insights": {
            "high_risk_regions": [{"region": r, "alerts": n, "alert_rate": round(n / region_orders.get(r, 1), 4)}
                                  for r, n in sorted(region_alerts.items(), key=lambda kv: -kv[1] / max(region_orders.get(kv[0], 1), 1))[:3]],
            "emerging_threats": [{"pattern": p, "change": round((pats[p] - prev_pats.get(p, 0)) / max(prev_pats.get(p, 0), 1), 2),
                                  "count": pats[p]} for p in pats if pats[p] > prev_pats.get(p, 0)][:2],
        },
        "investigation_metrics": {
            "avg_investigation_time_hours": round(sum(resolved_hours) / len(resolved_hours), 2) if resolved_hours else 0.0,
            "investigator_workload": dict(workload),
            "case_resolution_rate": round(len(closed) / len(alerts), 3) if alerts else 0.0,
            "quality_score": round(sum(a["is_fraud"] for a in closed) / len(closed), 3) if closed else 0.0,
        },
    }


# ------------------------------------------------------------------ cases
def list_cases(db: Session, status: Optional[str], investigator: Optional[str], priority: Optional[str],
               case_type: Optional[str], page: int, size: int) -> Dict[str, Any]:
    """A case groups the not-yet-closed alerts of one customer."""
    alerts = [a for a in load_alerts(db) if a["status"] in ("open", "investigating", "escalated")]
    now = data_now(db)
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for a in alerts:
        grouped[a["user_id"]].append(a)

    cases = []
    for uid, items in grouped.items():
        top = min(items, key=lambda a: PRIORITY_ORDER[a["priority"]])
        case_status = "escalated" if any(a["status"] == "escalated" for a in items) else \
            "in_progress" if any(a["status"] == "investigating" for a in items) else "open"
        opened = min(a["created_at"] for a in items)
        overdue = (now - opened) > timedelta(hours=SLA_HOURS * 2) and case_status == "open"
        cases.append({
            "case_id": f"case_{uid}", "alert_ids": [a["alert_id"] for a in items], "status": case_status,
            "priority": top["priority"], "case_type": top["alert_type"], "customer_id": uid,
            "total_amount": round(sum(a["amount"] for a in items), 2), "transaction_count": len(items),
            "created_at": _iso(opened), "updated_at": _iso(max(a["updated_at"] for a in items)),
            "assigned_to": next((a["assigned_to"] for a in items if a["assigned_to"]), None),
            "risk_assessment": {"max_risk_score": max(a["risk_score"] for a in items),
                                "rules_triggered": sorted({r for a in items for r in a["rules"]})},
            "sla_compliance": {"sla_hours": SLA_HOURS * 2, "age_hours": round((now - opened).total_seconds() / 3600, 1), "overdue": overdue},
        })
    if status:
        cases = [c for c in cases if c["status"] == status]
    if investigator:
        cases = [c for c in cases if c["assigned_to"] == investigator]
    if priority:
        cases = [c for c in cases if c["priority"] == priority]
    if case_type:
        cases = [c for c in cases if c["case_type"] == case_type]
    cases.sort(key=lambda c: (PRIORITY_ORDER[c["priority"]], c["created_at"]))
    total, pages = len(cases), max(math.ceil(len(cases) / size), 1)

    closed = [a for a in load_alerts(db) if a["status"] in ("resolved", "false_positive")]
    hours = [h for h in (_hours(a) for a in closed) if h is not None]
    workload = Counter(c["assigned_to"] for c in cases if c["assigned_to"])
    return {
        "cases": cases[(page - 1) * size : page * size],
        "filters": {"status": status, "investigator": investigator, "priority": priority, "case_type": case_type},
        "pagination": {"page": page, "size": size, "total": total, "pages": pages},
        "summary": {
            "total_cases": total, "open_cases": sum(c["status"] == "open" for c in cases),
            "in_progress_cases": sum(c["status"] == "in_progress" for c in cases),
            "escalated_cases": sum(c["status"] == "escalated" for c in cases),
            "overdue_cases": sum(c["sla_compliance"]["overdue"] for c in cases),
            "avg_investigation_time_hours": round(sum(hours) / len(hours), 2) if hours else 0.0,
            "sla_compliance_rate": round(1 - sum(c["sla_compliance"]["overdue"] for c in cases) / total, 3) if total else 1.0,
            "total_amount_under_investigation": round(sum(c["total_amount"] for c in cases), 2),
        },
        "insights": {
            "most_common_case_type": Counter(c["case_type"] for c in cases).most_common(1)[0][0] if cases else None,
            "busiest_investigator": workload.most_common(1)[0][0] if workload else None,
            "resolution_rate": round(len(closed) / max(len(closed) + len(alerts), 1), 3),
        },
    }


# ------------------------------------------------------------------ risk assessment
@ttl_cache(60)
def risk_assessment(db: Session, assessment_type: str, time_range: str, risk_threshold: float) -> Dict[str, Any]:
    now = data_now(db)
    window = parse_window(time_range, timedelta(days=30))
    alerts = load_alerts(db, now - window)
    previous = load_alerts(db, now - 2 * window, now - window)
    orders_now = len(_order_labels(db, now - window, now + timedelta(days=1)))
    orders_prev = len(_order_labels(db, now - 2 * window, now - window))
    rate, prev_rate = len(alerts) / max(orders_now, 1), len(previous) / max(orders_prev, 1)

    active = [a for a in alerts if a["status"] in ("open", "investigating", "escalated")]
    score = round(float(np.mean([a["risk_score"] for a in active])), 3) if active else 0.0
    level = "critical" if score >= 0.85 else "high" if score >= risk_threshold else "medium" if score >= 0.5 else "low"
    trend = "increasing" if rate > prev_rate * 1.1 else "decreasing" if rate < prev_rate * 0.9 else "stable"

    pats, prev_pats = Counter(r for a in alerts for r in a["rules"]), Counter(r for a in previous for r in a["rules"])
    total_pats = max(sum(pats.values()), 1)
    daily_loss: Dict[str, float] = defaultdict(float)
    for a in load_alerts(db, now - timedelta(days=60)):
        if a["is_fraud"] and a["status"] in CONFIRMED:
            daily_loss[a["created_at"].date().isoformat()] += a["amount"]
    series = [daily_loss.get((now.date() - timedelta(days=i)).isoformat(), 0.0) for i in range(60)]
    mean, sd = float(np.mean(series)), float(np.std(series))
    projected = mean * 30
    margin = 1.645 * sd * math.sqrt(30)
    return {
        "assessment_type": assessment_type, "time_range": time_range, "risk_threshold": risk_threshold,
        "generated_at": datetime.utcnow().isoformat(), "overall_risk_level": level, "risk_score": score, "trend": trend,
        "key_risk_factors": [{"factor": p, "impact": round(n / total_pats, 3),
                              "trend": "increasing" if n > prev_pats.get(p, 0) else "decreasing" if n < prev_pats.get(p, 0) else "stable"}
                             for p, n in pats.most_common(5)],
        "risk_distribution": {
            "low_risk": round(sum(a["priority"] == "low" for a in alerts) / max(len(alerts), 1), 3),
            "medium_risk": round(sum(a["priority"] == "medium" for a in alerts) / max(len(alerts), 1), 3),
            "high_risk": round(sum(a["priority"] == "high" for a in alerts) / max(len(alerts), 1), 3),
            "critical_risk": round(sum(a["priority"] == "critical" for a in alerts) / max(len(alerts), 1), 3),
        },
        "projected_loss": {"next_30_days": round(projected, 2),
                           "confidence_interval": {"lower": round(max(0.0, projected - margin), 2), "upper": round(projected + margin, 2)}},
    }
