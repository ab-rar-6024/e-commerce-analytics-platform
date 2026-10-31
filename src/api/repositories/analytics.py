"""
Analytics queries (revenue, segments, cohorts, funnels, products, real-time).

Every figure is computed from the orders / order_items / customers / products /
user_events tables. Monetary "revenue" is merchandise revenue net of discounts
(subtotal - discount_amount) on paid orders; tax and shipping are excluded.
"""

import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy import and_, case, distinct, func, select
from sqlalchemy.orm import Session

from src.database.models import Customer, Order, OrderItem, Product, UserEvent

from .common import (
    data_now,
    day_start,
    f,
    month_index,
    parse_date,
    parse_window,
    ttl_cache,
)

PAID = Order.payment_status == "paid"
NET_REVENUE = Order.subtotal - Order.discount_amount
CHANNEL_LABELS = {"online": "Online", "mobile_app": "Mobile App", "in_store": "In-Store"}
MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]


# --------------------------------------------------------------------------- revenue
def _order_cost_subquery():
    return (
        select(OrderItem.order_id.label("order_id"), func.sum(OrderItem.quantity * Product.cost).label("cost"))
        .join(Product, Product.product_id == OrderItem.product_id)
        .group_by(OrderItem.order_id)
        .subquery()
    )


def _daily_rows(db: Session, start: date, end: date, region: Optional[str], category: Optional[str]) -> Dict[str, Dict[str, float]]:
    """{'YYYY-MM-DD': {revenue, orders, cost}} for paid orders in [start, end]."""
    lo, hi = day_start(start), day_start(end + timedelta(days=1))
    if category:
        stmt = (
            select(
                func.date(Order.order_date).label("d"),
                func.sum(OrderItem.line_total).label("rev"),
                func.count(distinct(Order.order_id)).label("orders"),
                func.sum(OrderItem.quantity * Product.cost).label("cost"),
            )
            .select_from(OrderItem)
            .join(Order, Order.order_id == OrderItem.order_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .where(PAID, Order.order_date >= lo, Order.order_date < hi, OrderItem.product_category == category)
            .group_by("d")
        )
    else:
        cost = _order_cost_subquery()
        stmt = (
            select(
                func.date(Order.order_date).label("d"),
                func.sum(NET_REVENUE).label("rev"),
                func.count(Order.order_id).label("orders"),
                func.coalesce(func.sum(cost.c.cost), 0).label("cost"),
            )
            .select_from(Order)
            .outerjoin(cost, cost.c.order_id == Order.order_id)
            .where(PAID, Order.order_date >= lo, Order.order_date < hi)
            .group_by("d")
        )
    if region:
        stmt = stmt.join(Customer, Customer.user_id == Order.user_id).where(Customer.region == region)
    return {
        str(r.d): {"revenue": f(r.rev), "orders": int(r.orders), "cost": f(r.cost)}
        for r in db.execute(stmt).all()
    }


def _bucket(day: date, granularity: str) -> date:
    if granularity == "weekly":
        return day - timedelta(days=day.weekday())
    if granularity == "monthly":
        return day.replace(day=1)
    return day


def _time_series(daily: Dict[str, Dict[str, float]], start: date, end: date, granularity: str) -> List[Dict[str, Any]]:
    buckets: Dict[date, Dict[str, float]] = {}
    day = start
    while day <= end:
        row = daily.get(day.isoformat(), {"revenue": 0.0, "orders": 0, "cost": 0.0})
        b = buckets.setdefault(_bucket(day, granularity), {"revenue": 0.0, "orders": 0, "cost": 0.0})
        for k in b:
            b[k] += row[k]
        day += timedelta(days=1)
    series, prev = [], None
    for key in sorted(buckets):
        b = buckets[key]
        rev, orders = b["revenue"], int(b["orders"])
        series.append(
            {
                "date": key.isoformat(),
                "revenue": round(rev, 2),
                "orders": orders,
                "average_order_value": round(rev / orders, 2) if orders else 0.0,
                "profit_margin": round((rev - b["cost"]) / rev, 3) if rev else 0.0,
                "growth_rate": round((rev - prev) / prev, 3) if prev else 0.0,
            }
        )
        prev = rev
    return series


def _forecast(daily_revenue: List[float], horizon: int) -> Dict[str, Any]:
    """Linear trend over the observed days, projected for the next `horizon` days."""
    y = np.array(daily_revenue[-120:], dtype=float)
    if len(y) < 3 or y.sum() == 0:
        return {"next_period_revenue": 0.0, "confidence_interval": {"lower": 0.0, "upper": 0.0}, "trend": "stable"}
    x = np.arange(len(y))
    slope, intercept = np.polyfit(x, y, 1)
    future = np.arange(len(y), len(y) + horizon)
    predicted = np.clip(slope * future + intercept, 0, None)
    resid = y - (slope * x + intercept)
    sigma = float(resid.std(ddof=2)) if len(y) > 2 else 0.0
    margin = 1.645 * sigma * math.sqrt(horizon)
    total = float(predicted.sum())
    rel_slope = slope / max(float(y.mean()), 1e-9)

    # Backtest: refit without the last `holdout` days and compare the projected total with what happened
    holdout = min(14, len(y) // 3)
    accuracy = None
    if holdout >= 3 and y[-holdout:].sum() > 0:
        xt = np.arange(len(y) - holdout)
        b_slope, b_icpt = np.polyfit(xt, y[:-holdout], 1)
        back = np.clip(b_slope * np.arange(len(y) - holdout, len(y)) + b_icpt, 0, None).sum()
        accuracy = round(max(0.0, 1 - abs(back - y[-holdout:].sum()) / y[-holdout:].sum()), 3)
    trend = "increasing" if rel_slope > 0.002 else "decreasing" if rel_slope < -0.002 else "stable"
    return {
        "next_period_revenue": round(total, 2),
        "confidence_interval": {"lower": round(max(0.0, total - margin), 2), "upper": round(total + margin, 2)},
        "trend": trend,
        "backtest_accuracy": accuracy,
    }


def _breakdown(db: Session, start: date, end: date, region: Optional[str], category: Optional[str]) -> Dict[str, List[Dict[str, Any]]]:
    lo, hi = day_start(start), day_start(end + timedelta(days=1))
    base_where = [PAID, Order.order_date >= lo, Order.order_date < hi]

    def pack(rows, label_key, label_fn=lambda v: v):
        total = sum(f(r.rev) for r in rows) or 1.0
        return [
            {label_key: label_fn(r.k), "revenue": round(f(r.rev), 2), "percentage": round(100 * f(r.rev) / total, 1)}
            for r in sorted(rows, key=lambda r: -f(r.rev))
        ]

    cat_stmt = (
        select(OrderItem.product_category.label("k"), func.sum(OrderItem.line_total).label("rev"))
        .join(Order, Order.order_id == OrderItem.order_id)
        .where(*base_where)
        .group_by(OrderItem.product_category)
    )
    reg_stmt = (
        select(Customer.region.label("k"), func.sum(NET_REVENUE).label("rev"))
        .join(Customer, Customer.user_id == Order.user_id)
        .where(*base_where)
        .group_by(Customer.region)
    )
    chan_stmt = select(Order.channel.label("k"), func.sum(NET_REVENUE).label("rev")).where(*base_where).group_by(Order.channel)
    if region:
        cat_stmt = cat_stmt.join(Customer, Customer.user_id == Order.user_id).where(Customer.region == region)
        chan_stmt = chan_stmt.join(Customer, Customer.user_id == Order.user_id).where(Customer.region == region)
    if category:
        reg_stmt = reg_stmt.join(OrderItem, OrderItem.order_id == Order.order_id).where(OrderItem.product_category == category)
        chan_stmt = chan_stmt.join(OrderItem, OrderItem.order_id == Order.order_id).where(OrderItem.product_category == category)
    return {
        "by_category": pack(db.execute(cat_stmt).all(), "category"),
        "by_region": pack(db.execute(reg_stmt).all(), "region", lambda v: v or "Unknown"),
        "by_channel": pack(db.execute(chan_stmt).all(), "channel", lambda v: CHANNEL_LABELS.get(v, v)),
    }


@ttl_cache(180)
def revenue_analytics(db: Session, start_date: Optional[str], end_date: Optional[str], granularity: str,
                      region: Optional[str], category: Optional[str]) -> Dict[str, Any]:
    end = parse_date(end_date, data_now(db).date())
    start = parse_date(start_date, end - timedelta(days=29))
    daily = _daily_rows(db, start, end, region, category)
    series = _time_series(daily, start, end, granularity)

    days = (end - start).days + 1
    prev_end, prev_start = start - timedelta(days=1), start - timedelta(days=days)
    prev_rows = _daily_rows(db, prev_start, prev_end, region, category)
    prev_total = sum(r["revenue"] for r in prev_rows.values())
    prev_profit = prev_total - sum(r["cost"] for r in prev_rows.values())

    total_rev = sum(r["revenue"] for r in daily.values())
    total_orders = sum(r["orders"] for r in daily.values())
    total_cost = sum(r["cost"] for r in daily.values())
    daily_values = [daily.get((start + timedelta(days=i)).isoformat(), {"revenue": 0.0})["revenue"] for i in range(days)]

    cohorts = cohort_table(db)
    return {
        "period": {"start_date": start.isoformat(), "end_date": end.isoformat(), "granularity": granularity},
        "filters": {"region": region, "category": category},
        "summary": {
            "total_revenue": round(total_rev, 2),
            "total_orders": total_orders,
            "average_order_value": round(total_rev / total_orders, 2) if total_orders else 0.0,
            "profit_margin": round((total_rev - total_cost) / total_rev, 3) if total_rev else 0.0,
            "growth_rate": round((total_rev - prev_total) / prev_total, 3) if prev_total else 0.0,
            "profit": round(total_rev - total_cost, 2),
            "profit_growth": round(((total_rev - total_cost) - prev_profit) / prev_profit, 3) if prev_profit > 0 else 0.0,
        },
        "time_series": series,
        "forecast": _forecast(daily_values, max(days, 7)),
        "breakdown": _breakdown(db, start, end, region, category),
        "cohort_analysis": {
            "retention_rates": cohorts["retention_rates"],
            "ltv_by_cohort": cohorts["ltv_by_cohort"],
        },
    }


# --------------------------------------------------------------------------- cohorts
def _first_order_month_and_activity(db: Session) -> Tuple[Dict[str, int], Dict[str, Dict[int, float]]]:
    """user -> first paid-order month index, and user -> {month index: revenue}."""
    month_expr = (func.extract("year", Order.order_date) * 12 + func.extract("month", Order.order_date) - 1)
    rows = db.execute(
        select(Order.user_id, month_expr.label("m"), func.sum(NET_REVENUE).label("rev")).where(PAID).group_by(Order.user_id, "m")
    ).all()
    activity: Dict[str, Dict[int, float]] = defaultdict(dict)
    for r in rows:
        activity[r.user_id][int(r.m)] = f(r.rev)
    first = {u: min(m) for u, m in activity.items()}
    return first, activity


@ttl_cache(300)
def cohort_table(db: Session, max_cohorts: int = 6, max_periods: int = 12) -> Dict[str, Any]:
    """Monthly acquisition cohorts: retention and revenue per customer by months since first order."""
    first, activity = _first_order_month_and_activity(db)
    if not first:
        return {"cohorts": [], "retention_rates": {f"month_{k}": 0.0 for k in (1, 3, 6, 12)}, "ltv_by_cohort": {}}
    latest = max(m for months in activity.values() for m in months)
    members: Dict[int, List[str]] = defaultdict(list)
    for u, m in first.items():
        members[m].append(u)

    # The first month of data holds every pre-existing customer, not a real cohort
    first_month = min(members)
    cohort_months = [m for m in sorted(members) if m != first_month and len(members[m]) >= 20]
    cohorts = []
    for m in cohort_months:
        users = members[m]
        periods = []
        for k in range(0, min(max_periods, latest - m) + 1):
            active = [u for u in users if activity[u].get(m + k)]
            revenue = sum(activity[u].get(m + k, 0.0) for u in users)
            periods.append(
                {
                    "period": k,
                    "value": round(len(active) / len(users), 3),
                    "percentage": round(100 * len(active) / len(users), 1),
                    "revenue_per_customer": round(revenue / len(users), 2),
                }
            )
        cohorts.append({"cohort": f"{m // 12}-{m % 12 + 1:02d}", "month_index": m, "size": len(users), "periods": periods})

    def avg_retention(k: int) -> float:
        vals = [c["periods"][k]["value"] for c in cohorts if len(c["periods"]) > k]
        return round(float(np.mean(vals)), 3) if vals else None  # None: no cohort is old enough yet

    ltv = {}
    for c in cohorts[-3:]:
        users = members[c["month_index"]]
        total = sum(sum(activity[u].values()) for u in users)
        ltv[f"{MONTHS[c['month_index'] % 12]}_{c['month_index'] // 12}"] = round(total / len(users), 2)
    return {
        "cohorts": cohorts,
        "retention_rates": {f"month_{k}": avg_retention(k) for k in (1, 3, 6, 12)},
        "ltv_by_cohort": ltv,
    }


@ttl_cache(300)
def cohort_analysis(db: Session, metric: str = "retention", start_date: Optional[str] = None) -> Dict[str, Any]:
    table = cohort_table(db)
    cohorts = table["cohorts"]
    if start_date:
        floor = month_index(datetime.strptime(start_date[:10], "%Y-%m-%d"))
        cohorts = [c for c in cohorts if c["month_index"] >= floor]
    cohorts = cohorts[-6:]
    out = []
    for c in cohorts:
        periods = [
            {"period": p["period"], "value": p["value"] if metric == "retention" else p["revenue_per_customer"],
             "percentage": p["percentage"]}
            for p in c["periods"]
        ]
        out.append({"cohort": c["cohort"], "size": c["size"], "periods": periods})

    def p(c, k):
        return c["periods"][k]["value"] if len(c["periods"]) > k else None

    p1 = [p(c, 1) for c in cohorts if p(c, 1) is not None]
    p6 = [p(c, 6) for c in cohorts if p(c, 6) is not None]
    avg1, avg6 = (float(np.mean(p1)) if p1 else 0.0), (float(np.mean(p6)) if p6 else 0.0)
    best = max((c for c in cohorts if p(c, 1) is not None), key=lambda c: p(c, 1), default=None)
    half = len(p1) // 2
    trend = "stable"
    if half >= 1:
        delta = float(np.mean(p1[half:])) - float(np.mean(p1[:half]))
        trend = "improving" if delta > 0.02 else "declining" if delta < -0.02 else "stable"
    drops = []
    if cohorts:
        longest = max(cohorts, key=lambda c: len(c["periods"]))["periods"]
        drops = [(longest[i - 1]["value"] - longest[i]["value"], i) for i in range(1, len(longest))]
    biggest = max(drops, default=(0, 1))
    return {
        "cohort_type": "monthly",
        "metric": metric,
        "generated_at": datetime.utcnow().isoformat(),
        "cohorts": out,
        "summary": {
            "avg_retention_period_1": round(avg1, 3),
            "avg_retention_period_6": round(avg6, 3),
            "best_performing_cohort": best["cohort"] if best else None,
            "cohort_health": "good" if avg1 >= 0.35 else "moderate" if avg1 >= 0.2 else "poor",
        },
        "insights": {
            "retention_trend": trend,
            "churn_pattern": "early_dropoff" if drops and drops[0][0] > 0.5 else "gradual_decay",
            "revenue_opportunity": f"increase_engagement_period_{biggest[1]}",
        },
    }


# --------------------------------------------------------------------------- segments
SEGMENTS = ["Champions", "Loyal Customers", "Potential Loyalists", "New Customers", "Promising", "Need Attention", "About to Sleep", "At Risk"]


def _scores(values: np.ndarray, higher_is_better: bool = True) -> np.ndarray:
    """1..5 quintile score by rank (ties broken by order)."""
    n = len(values)
    ranks = np.argsort(np.argsort(values if higher_is_better else -values, kind="stable"), kind="stable")
    return 1 + np.floor(5 * ranks / max(n, 1)).astype(int)


def _rfm_segment(r: int, fq: int, freq: int) -> str:
    if r >= 4 and fq >= 4:
        return "Champions"
    if r >= 3 and fq >= 4:
        return "Loyal Customers"
    if r >= 4 and freq == 1:
        return "New Customers"
    if r >= 4:
        return "Potential Loyalists"
    if r == 3:
        return "Promising" if fq <= 3 else "Loyal Customers"
    if r == 2 and fq >= 3:
        return "At Risk"
    if r == 2:
        return "Need Attention"
    return "At Risk" if fq >= 4 else "About to Sleep"


@ttl_cache(300)
def customer_rfm(db: Session) -> List[Dict[str, Any]]:
    """Per-customer recency/frequency/monetary values with RFM scores and segment."""
    now = data_now(db)
    rows = db.execute(
        select(Order.user_id, func.max(Order.order_date).label("last"), func.min(Order.order_date).label("first"),
               func.count(Order.order_id).label("freq"), func.sum(NET_REVENUE).label("mon")).where(PAID).group_by(Order.user_id)
    ).all()
    if not rows:
        return []

    def as_dt(v):
        return datetime.fromisoformat(str(v)[:19]) if isinstance(v, str) else v

    last = [as_dt(r.last) for r in rows]
    first_dt = [as_dt(r.first) for r in rows]
    recency = np.array([(now - t).total_seconds() / 86400 for t in last])
    freq = np.array([int(r.freq) for r in rows])
    mon = np.array([f(r.mon) for r in rows])
    r_s, f_s, m_s = _scores(recency, False), _scores(freq), _scores(mon)
    return [
        {"user_id": rows[i].user_id, "recency_days": int(recency[i]), "frequency": int(freq[i]), "monetary": float(mon[i]),
         "first_order": first_dt[i].isoformat(), "last_order": last[i].isoformat(),
         "avg_interval_days": round((last[i] - first_dt[i]).total_seconds() / 86400 / (freq[i] - 1), 1) if freq[i] > 1 else None,
         "r": int(r_s[i]), "f": int(f_s[i]), "m": int(m_s[i]), "rfm_score": f"{r_s[i]}{f_s[i]}{m_s[i]}",
         "segment": _rfm_segment(int(r_s[i]), int(f_s[i]), int(freq[i]))}
        for i in range(len(rows))
    ]


@ttl_cache(300)
def customer_segments(db: Session, segment_type: str = "rfm") -> Dict[str, Any]:
    rfm = customer_rfm(db)
    total_monetary = sum(c["monetary"] for c in rfm) or 1.0
    n = len(rfm) or 1

    if segment_type == "geographic":
        groups = db.execute(
            select(Customer.region, func.count(Customer.user_id), func.coalesce(func.sum(Customer.lifetime_value), 0),
                   func.coalesce(func.avg(Customer.total_orders), 0)).group_by(Customer.region)
        ).all()
        total = sum(g[1] for g in groups) or 1
        total_ltv = sum(f(g[2]) for g in groups) or 1.0
        segments = [{"name": g[0] or "Unknown", "count": int(g[1]), "percentage": round(100 * g[1] / total, 1),
                     "characteristics": {"avg_customer_lifetime_value": round(f(g[2]) / g[1], 2),
                                         "avg_orders_per_customer": round(f(g[3]), 2),
                                         "revenue_contribution": round(f(g[2]) / total_ltv, 3)}} for g in groups]
    elif segment_type == "behavior":
        groups = db.execute(
            select(Customer.customer_tier, func.count(Customer.user_id), func.coalesce(func.sum(Customer.lifetime_value), 0),
                   func.coalesce(func.avg(Customer.total_orders), 0)).group_by(Customer.customer_tier)
        ).all()
        total = sum(g[1] for g in groups) or 1
        total_ltv = sum(f(g[2]) for g in groups) or 1.0
        segments = [{"name": g[0].title(), "count": int(g[1]), "percentage": round(100 * g[1] / total, 1),
                     "characteristics": {"avg_customer_lifetime_value": round(f(g[2]) / g[1], 2),
                                         "avg_orders_per_customer": round(f(g[3]), 2),
                                         "revenue_contribution": round(f(g[2]) / total_ltv, 3)}} for g in groups]
    else:
        by_seg: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for c in rfm:
            by_seg[c["segment"]].append(c)
        segments = []
        for name in SEGMENTS:
            members = by_seg.get(name, [])
            m_sum = sum(c["monetary"] for c in members)
            segments.append({
                "name": name, "count": len(members), "percentage": round(100 * len(members) / n, 1),
                "characteristics": {
                    "avg_customer_lifetime_value": round(m_sum / len(members), 2) if members else 0.0,
                    "avg_orders_per_customer": round(sum(c["frequency"] for c in members) / len(members), 2) if members else 0.0,
                    "revenue_contribution": round(m_sum / total_monetary, 3),
                },
            })

    top = max(segments, key=lambda s: s["characteristics"]["revenue_contribution"], default=None)
    growth_pool = [s for s in segments if s["name"] in ("Potential Loyalists", "Promising", "New Customers")]
    at_risk = sum(c["monetary"] for c in rfm if c["segment"] in ("At Risk", "Need Attention"))
    return {
        "segment_type": segment_type,
        "generated_at": datetime.utcnow().isoformat(),
        "total_customers": len(rfm),
        "segments": segments,
        "insights": {
            "top_segment": top["name"] if top else None,
            "growth_opportunity": max(growth_pool, key=lambda s: s["count"])["name"] if growth_pool else None,
            "at_risk_revenue": round(at_risk, 2),
        },
        "rfm_breakdown": {
            "recency_distribution": {
                "recent": round(sum(c["recency_days"] <= 30 for c in rfm) / n, 3),
                "moderate": round(sum(30 < c["recency_days"] <= 90 for c in rfm) / n, 3),
                "distant": round(sum(c["recency_days"] > 90 for c in rfm) / n, 3),
            },
            "frequency_distribution": {
                "high": round(sum(c["frequency"] >= 5 for c in rfm) / n, 3),
                "medium": round(sum(2 <= c["frequency"] < 5 for c in rfm) / n, 3),
                "low": round(sum(c["frequency"] == 1 for c in rfm) / n, 3),
            },
            "monetary_distribution": {
                "high_value": round(sum(c["monetary"] >= 1000 for c in rfm) / n, 3),
                "medium_value": round(sum(200 <= c["monetary"] < 1000 for c in rfm) / n, 3),
                "low_value": round(sum(c["monetary"] < 200 for c in rfm) / n, 3),
            },
        },
    }


# --------------------------------------------------------------------------- funnels
FUNNEL_STEPS = [("Landing Page", "page_view"), ("Product View", "product_view"), ("Add to Cart", "add_to_cart"),
                ("Checkout Started", "checkout_start"), ("Purchase", "purchase")]
FUNNEL_ACTIONS = {
    "Product View": "Improve landing-page product discovery and search relevance",
    "Add to Cart": "Reduce friction on product pages: clearer pricing, reviews, stock and delivery info",
    "Checkout Started": "Reduce friction in the cart: show shipping costs early and add saved carts",
    "Purchase": "Simplify checkout: guest checkout, fewer form fields, more payment methods",
}


@ttl_cache(120)
def funnel_analysis(db: Session, funnel_type: str = "purchase", time_range: str = "30d", segment: Optional[str] = None) -> Dict[str, Any]:
    now = data_now(db)
    since = now - parse_window(time_range)
    stmt = (
        select(UserEvent.event_type, func.count(distinct(UserEvent.session_id)))
        .where(UserEvent.event_timestamp >= since)
        .group_by(UserEvent.event_type)
    )
    if segment:
        stmt = stmt.join(Customer, Customer.user_id == UserEvent.user_id).where(Customer.customer_tier == segment.lower())
    sessions = dict(db.execute(stmt).all())

    steps, first, prev = [], None, None
    for name, etype in FUNNEL_STEPS:
        users = int(sessions.get(etype, 0))
        first = users if first is None else first
        drop = (prev - users) if prev is not None else 0
        steps.append({
            "name": name, "users": users,
            "conversion_rate": round(users / first, 3) if first else 0.0,
            "drop_off": max(drop, 0),
            "drop_off_rate": round(max(drop, 0) / prev, 3) if prev else 0.0,
        })
        prev = users

    later = steps[1:]
    worst = max(later, key=lambda s: s["drop_off_rate"], default=None)
    total_entries, conversions = (steps[0]["users"], steps[-1]["users"]) if steps else (0, 0)
    overall = round(conversions / total_entries, 3) if total_entries else 0.0
    ranked = sorted(later, key=lambda s: -s["drop_off_rate"])[:2]

    def lift(step):  # relative conversion gain if this step's drop-off fell by 10%
        dr = step["drop_off_rate"]
        return round(0.1 * dr / (1 - dr), 3) if dr < 1 else 0.0

    return {
        "funnel_type": funnel_type, "time_range": time_range, "segment": segment,
        "generated_at": datetime.utcnow().isoformat(),
        "steps": steps,
        "summary": {
            "total_entries": total_entries, "total_conversions": conversions, "overall_conversion_rate": overall,
            "biggest_drop_off_step": worst["name"] if worst else None,
            "optimization_priority": f"{steps[steps.index(worst) - 1]['name']} to {worst['name']}" if worst else None,
        },
        "insights": {
            "funnel_health": "good" if overall >= 0.08 else "moderate" if overall >= 0.04 else "poor",
            "key_bottleneck": worst["name"] if worst else None,
            "improvement_potential": round(100 * lift(worst), 1) if worst else 0.0,
        },
        "recommendations": [{"step": s["name"], "action": FUNNEL_ACTIONS[s["name"]], "expected_lift": lift(s)} for s in ranked],
    }


# --------------------------------------------------------------------------- products
def _inventory_status(units: int) -> str:
    return "out_of_stock" if units <= 0 else "low_stock" if units <= 15 else "in_stock"


@ttl_cache(180)
def product_performance(db: Session, category: Optional[str], sort_by: str, time_range: str) -> Dict[str, Any]:
    now = data_now(db)
    window = parse_window(time_range)
    lo, mid = now - 2 * window, now - window

    def sales(since: datetime, until: datetime):
        stmt = (
            select(OrderItem.product_id, func.sum(OrderItem.line_total).label("rev"), func.sum(OrderItem.quantity).label("units"),
                   func.sum(case((Order.order_status == "refunded", OrderItem.quantity), else_=0)).label("returned"))
            .join(Order, Order.order_id == OrderItem.order_id)
            .where(Order.payment_status.in_(("paid", "refunded")), Order.order_date >= since, Order.order_date < until)
            .group_by(OrderItem.product_id)
        )
        return {r.product_id: r for r in db.execute(stmt).all()}

    current, previous = sales(mid, now + timedelta(seconds=1)), sales(lo, mid)
    views = dict(db.execute(select(UserEvent.product_id, func.count()).where(
        UserEvent.event_type == "product_view", UserEvent.event_timestamp >= mid, UserEvent.product_id.isnot(None)
    ).group_by(UserEvent.product_id)).all())
    buys = dict(db.execute(select(UserEvent.product_id, func.count()).where(
        UserEvent.event_type == "purchase", UserEvent.event_timestamp >= mid, UserEvent.product_id.isnot(None)
    ).group_by(UserEvent.product_id)).all())

    stmt = select(Product).where(Product.status == "active")
    if category:
        stmt = stmt.where(Product.category_name == category)
    products = []
    for p in db.execute(stmt).scalars():
        cur, prev = current.get(p.product_id), previous.get(p.product_id)
        rev, units = f(cur.rev if cur else 0), int(cur.units if cur else 0)
        prev_rev = f(prev.rev if prev else 0)
        returned = int(cur.returned if cur else 0)
        v = views.get(p.product_id, 0)
        products.append({
            "product_id": p.product_id, "name": p.name, "category": p.category_name, "sku": p.sku,
            "revenue": round(rev, 2), "units_sold": units, "average_rating": f(p.rating_avg, 2),
            "margin": round((f(p.price) - f(p.cost)) / f(p.price), 3) if f(p.price) else 0.0,
            "growth_rate": round((rev - prev_rev) / prev_rev, 3) if prev_rev else 0.0,
            "inventory_status": _inventory_status(p.inventory_count), "inventory_count": p.inventory_count,
            "inventory_turnover": round(units / max(p.inventory_count, 1), 2),
            "return_rate": round(returned / units, 3) if units else 0.0,
            "conversion_rate": round(buys.get(p.product_id, 0) / v, 3) if v else 0.0,
        })

    keys = {"revenue": "revenue", "units": "units_sold", "rating": "average_rating", "margin": "margin", "growth": "growth_rate"}
    products.sort(key=lambda r: -r[keys.get(sort_by, "revenue")])

    total_rev = sum(p["revenue"] for p in products)
    total_units = sum(p["units_sold"] for p in products)
    cat_rev: Dict[str, float] = defaultdict(float)
    for p in products:
        cat_rev[p["category"]] += p["revenue"]
    prev_total = sum(f(r.rev) for pid, r in previous.items() if pid in {p["product_id"] for p in products})
    weighted_margin = sum(p["margin"] * p["revenue"] for p in products) / total_rev if total_rev else 0.0
    out_of_stock = sum(p["inventory_status"] == "out_of_stock" for p in products)
    movers = [p for p in products if p["growth_rate"] and p["revenue"] > 0]
    return {
        "products": products,
        "filters": {"category": category, "sort_by": sort_by, "time_range": time_range},
        "summary": {
            "total_revenue": round(total_rev, 2), "total_units": total_units,
            "average_margin": round(weighted_margin, 3),
            "avg_rating": round(float(np.mean([p["average_rating"] for p in products])), 2) if products else 0.0,
            "top_performing_category": max(cat_rev, key=cat_rev.get) if cat_rev else None,
            "fastest_growing_product": max(movers, key=lambda p: p["growth_rate"])["name"] if movers else None,
        },
        "trends": {
            "revenue_growth": round((total_rev - prev_total) / prev_total, 3) if prev_total else 0.0,
            "margin_trend": "stable",
            "inventory_health": "good" if out_of_stock / max(len(products), 1) < 0.05 else "attention",
            "return_rate_trend": "stable",
        },
    }


# --------------------------------------------------------------------------- real-time
@ttl_cache(20)
def realtime_metrics(db: Session, time_window: str = "1h") -> Dict[str, Any]:
    now = data_now(db)
    window = parse_window(time_window, timedelta(hours=1))
    since = now - window
    minutes = max(window.total_seconds() / 60, 1)

    orders = db.execute(
        select(func.coalesce(func.sum(NET_REVENUE), 0), func.count(Order.order_id)).where(PAID, Order.order_date >= since)
    ).one()
    revenue, order_count = f(orders[0]), int(orders[1])

    sess = db.execute(
        select(UserEvent.session_id, func.count().label("n"), func.min(UserEvent.event_timestamp).label("t0"),
               func.max(UserEvent.event_timestamp).label("t1"),
               func.sum(case((UserEvent.event_type == "add_to_cart", 1), else_=0)).label("cart"),
               func.sum(case((UserEvent.event_type == "purchase", 1), else_=0)).label("buy"),
               func.max(case((UserEvent.user_id.isnot(None), 1), else_=0)).label("known"))
        .where(UserEvent.event_timestamp >= since).group_by(UserEvent.session_id)
    ).all()
    n_sess = len(sess)

    def secs(r):
        t0, t1 = (datetime.fromisoformat(str(x)[:19]) for x in (r.t0, r.t1))
        return (t1 - t0).total_seconds()

    page_views = int(db.execute(select(func.count()).where(UserEvent.event_type == "page_view", UserEvent.event_timestamp >= since)).scalar() or 0)
    sources = dict(db.execute(select(UserEvent.traffic_source, func.count(distinct(UserEvent.session_id))).where(
        UserEvent.event_timestamp >= since).group_by(UserEvent.traffic_source)).all())
    src_total = sum(sources.values()) or 1

    top = db.execute(
        select(OrderItem.product_id, func.sum(OrderItem.quantity).label("u"), func.sum(OrderItem.line_total).label("r"))
        .join(Order, Order.order_id == OrderItem.order_id).where(PAID, Order.order_date >= since)
        .group_by(OrderItem.product_id).order_by(func.sum(OrderItem.line_total).desc()).limit(2)
    ).all()

    week_ago = now - timedelta(days=7)
    velocity = db.execute(
        select(OrderItem.product_id, (func.sum(OrderItem.quantity) / 7.0).label("v"))
        .join(Order, Order.order_id == OrderItem.order_id).where(PAID, Order.order_date >= week_ago)
        .group_by(OrderItem.product_id).order_by(func.sum(OrderItem.quantity).desc()).limit(2)
    ).all()
    inv = db.execute(select(
        func.sum(case((Product.inventory_count == 0, 1), else_=0)),
        func.sum(case((and_(Product.inventory_count > 0, Product.inventory_count <= 15), 1), else_=0)),
        func.coalesce(func.sum(Product.inventory_count), 0)).where(Product.status == "active")).one()
    sold_30d = f(db.execute(select(func.coalesce(func.sum(OrderItem.quantity), 0)).join(
        Order, Order.order_id == OrderItem.order_id).where(PAID, Order.order_date >= now - timedelta(days=30))).scalar())
    # Stock-out risk: products whose last-7-day sales already exceed what is left in stock
    weekly = (select(OrderItem.product_id.label("pid"), func.sum(OrderItem.quantity).label("u"))
              .join(Order, Order.order_id == OrderItem.order_id).where(PAID, Order.order_date >= week_ago)
              .group_by(OrderItem.product_id).subquery())
    products_at_risk = db.execute(
        select(func.count()).select_from(Product).join(weekly, weekly.c.pid == Product.product_id)
        .where(Product.status == "active", Product.inventory_count < weekly.c.u)
    ).scalar()

    return {
        "timestamp": now.isoformat(), "time_window": time_window, "data_as_of": now.isoformat(),
        "sales": {
            "current_revenue": round(revenue, 2),
            "orders_per_minute": round(order_count / minutes, 3),
            "orders": order_count,
            "conversion_rate": round(sum(1 for r in sess if r.buy) / n_sess, 3) if n_sess else 0.0,
            "average_order_value": round(revenue / order_count, 2) if order_count else 0.0,
            "abandoned_carts": sum(1 for r in sess if r.cart and not r.buy),
            "top_selling_products": [{"product_id": r.product_id, "units": int(r.u), "revenue": round(f(r.r), 2)} for r in top],
        },
        "traffic": {
            "active_users": n_sess,
            "page_views": page_views,
            "session_duration_avg": round(float(np.mean([secs(r) for r in sess])), 1) if sess else 0.0,
            "bounce_rate": round(sum(1 for r in sess if r.n == 1) / n_sess, 3) if n_sess else 0.0,
            "new_vs_returning": {"new_users": sum(1 for r in sess if not r.known), "returning_users": sum(1 for r in sess if r.known)},
            "traffic_sources": {k: round(sources.get(k, 0) / src_total, 3) for k in ("organic", "direct", "social", "paid", "email")},
        },
        "inventory": {
            "low_stock_items": int(inv[1] or 0), "out_of_stock_items": int(inv[0] or 0),
            "inventory_turnover": round(sold_30d * 12 / max(f(inv[2]), 1), 2),  # annualised
            "stockout_risk": int(products_at_risk or 0),
            "fast_moving_items": [{"product_id": r.product_id, "velocity": round(f(r.v), 2)} for r in velocity],
        },
    }


# --------------------------------------------------------------------------- executive
COUNTRY_NAMES = {"US": "United States", "CA": "Canada", "GB": "United Kingdom", "DE": "Germany", "FR": "France",
                 "IN": "India", "JP": "Japan", "SG": "Singapore", "AU": "Australia", "BR": "Brazil", "ZA": "South Africa"}


def _period_totals(daily: Dict[str, Dict[str, float]]) -> Dict[str, float]:
    rev = sum(r["revenue"] for r in daily.values())
    return {"revenue": rev, "orders": sum(r["orders"] for r in daily.values()), "cost": sum(r["cost"] for r in daily.values())}


def _pct(new: float, old: float) -> float:
    return round(100 * (new - old) / old, 1) if old else 0.0


@ttl_cache(300)
def executive_overview(db: Session, days: int = 90) -> Dict[str, Any]:
    """Period-over-period executive KPIs and breakdowns, all from stored orders."""
    now = data_now(db)
    end = now.date()
    start = end - timedelta(days=days - 1)
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days - 1)
    lo, hi = day_start(start), day_start(end + timedelta(days=1))
    plo, phi = day_start(prev_start), day_start(prev_end + timedelta(days=1))

    cur = _period_totals(_daily_rows(db, start, end, None, None))
    prev = _period_totals(_daily_rows(db, prev_start, prev_end, None, None))
    margin = (cur["revenue"] - cur["cost"]) / cur["revenue"] if cur["revenue"] else 0.0
    prev_margin = (prev["revenue"] - prev["cost"]) / prev["revenue"] if prev["revenue"] else 0.0

    def satisfaction(a: datetime, b: datetime) -> float:
        r = db.execute(
            select(func.sum(Product.rating_avg * OrderItem.quantity), func.sum(OrderItem.quantity))
            .join(Order, Order.order_id == OrderItem.order_id).join(Product, Product.product_id == OrderItem.product_id)
            .where(PAID, Order.order_date >= a, Order.order_date < b)
        ).one()
        return f(r[0]) / f(r[1]) if f(r[1]) else 0.0

    sat, prev_sat = satisfaction(lo, hi), satisfaction(plo, phi)
    active = db.execute(select(func.count(distinct(Order.user_id))).where(PAID, Order.order_date >= lo, Order.order_date < hi)).scalar() or 0

    geo = db.execute(
        select(Customer.country, func.sum(NET_REVENUE)).join(Customer, Customer.user_id == Order.user_id)
        .where(PAID, Order.order_date >= lo, Order.order_date < hi).group_by(Customer.country)
    ).all()

    def channel_rev(a: datetime, b: datetime) -> Dict[str, float]:
        rows = db.execute(select(Order.channel, func.sum(NET_REVENUE)).where(
            PAID, Order.order_date >= a, Order.order_date < b).group_by(Order.channel)).all()
        return {r[0]: f(r[1]) for r in rows}

    ch_cur, ch_prev = channel_rev(lo, hi), channel_rev(plo, phi)

    # last six calendar months: revenue and margin
    m_start = (end.replace(day=1) - timedelta(days=150)).replace(day=1)
    monthly = _time_series(_daily_rows(db, m_start, end, None, None), m_start, end, "monthly")

    # new customers per month (last 12 months)
    reg_rows = db.execute(select(Customer.registration_date).where(Customer.registration_date >= now - timedelta(days=365))).scalars().all()
    new_by_month: Dict[str, int] = defaultdict(int)
    for ts in reg_rows:
        ts = datetime.fromisoformat(str(ts)[:19]) if isinstance(ts, str) else ts
        new_by_month[f"{ts.year}-{ts.month:02d}"] += 1

    rfm = customer_rfm(db)
    edges = [(0, 100, "$0-100"), (100, 500, "$100-500"), (500, 1000, "$500-1000"), (1000, 2000, "$1000-2000"), (2000, float("inf"), "$2000+")]
    clv = [{"clv_range": label, "customer_count": sum(a <= c["monetary"] < b for c in rfm)} for a, b, label in edges]

    cohorts = cohort_table(db)["cohorts"]
    retention = []
    for k in (1, 2, 3, 6, 12):
        vals = [c["periods"][k]["value"] for c in cohorts if len(c["periods"]) > k]
        if vals:
            retention.append({"cohort_period": f"Month {k}", "retention_rate": round(float(np.mean(vals)), 3)})

    _, activity = _first_order_month_and_activity(db)
    latest_m = max((m for months in activity.values() for m in months), default=0)
    last_full, before = latest_m - 1, latest_m - 2  # the newest month is still in progress
    base = [u for u, months in activity.items() if before in months]
    kept = [u for u in base if last_full in activity[u]]
    churn = round(1 - len(kept) / len(base), 3) if base else 0.0

    return {
        "period": {"days": days, "start": start.isoformat(), "end": end.isoformat(), "label": f"Last {days} days"},
        "executive_kpis": {
            "total_revenue": round(cur["revenue"], 2), "revenue_growth": _pct(cur["revenue"], prev["revenue"]),
            "total_orders": cur["orders"], "active_customers": int(active),
            "average_order_value": round(cur["revenue"] / cur["orders"], 2) if cur["orders"] else 0.0,
            "profit_margin": round(100 * margin, 1), "margin_change": round(100 * (margin - prev_margin), 2),
            "customer_satisfaction": round(sat, 2), "satisfaction_change": round(sat - prev_sat, 3),
        },
        "geographic_data": [{"location": c, "country": COUNTRY_NAMES.get(c, c), "revenue": round(f(v), 2)}
                            for c, v in sorted(geo, key=lambda r: -f(r[1])) if c],
        "revenue_performance": {
            "period_comparison": {"previous_period": round(prev["revenue"], 2), "current_period": round(cur["revenue"], 2),
                                  "growth_amount": round(cur["revenue"] - prev["revenue"], 2),
                                  "previous_label": f"Previous {days} days", "current_label": f"Last {days} days"},
            "profit_trends": [{"period": m["date"][:7], "revenue": m["revenue"], "profit_margin": round(100 * m["profit_margin"], 1)}
                              for m in monthly[-6:]],
            "channel_performance": [{"channel": CHANNEL_LABELS.get(k, k), "revenue": round(v, 2), "growth_rate": _pct(v, ch_prev.get(k, 0))}
                                    for k, v in sorted(ch_cur.items(), key=lambda kv: -kv[1])],
        },
        "customer_analytics": {
            "acquisition_trends": [{"period": k, "new_customers": v} for k, v in sorted(new_by_month.items())],
            "clv_distribution": clv, "retention_data": retention,
            "churn_analysis": {"current_churn_rate": churn,
                               "basis": "customers active the month before last who did not order last month"},
        },
    }
