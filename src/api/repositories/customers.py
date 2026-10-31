"""
Customer queries: directory with RFM analytics, single-customer detail,
co-purchase recommendations and a simple value outlook.

Churn risk and the 12-month value outlook are transparent heuristics over the
customer's own order history (not a trained model); responses say so under "method".
"""

import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from src.database.models import Customer, Order, OrderItem, Product

from .analytics import NET_REVENUE, PAID, customer_rfm
from .common import data_now, f, ttl_cache

DEFAULT_CADENCE_DAYS = 45.0  # assumed typical gap for customers with a single order
SORT_KEYS = {
    "lifetime_value": lambda c: -c["analytics"]["lifetime_value"],
    "total_orders": lambda c: -c["analytics"]["total_orders"],
    "recency": lambda c: c["analytics"]["recency_days"],
    "churn_risk": lambda c: -c["analytics"]["churn_risk"],
    "created_at": lambda c: c["created_at"],
}


def churn_risk(recency_days: float, avg_interval_days: Optional[float]) -> float:
    """0..1 - zero until a customer is overdue relative to their own buying rhythm."""
    cadence = avg_interval_days or DEFAULT_CADENCE_DAYS
    overdue = max(recency_days - cadence, 0.0)
    return round(1 - math.exp(-overdue / (2 * cadence)), 3)


def risk_level(risk: float) -> str:
    return "high" if risk >= 0.6 else "medium" if risk >= 0.3 else "low"


@ttl_cache(300)
def _directory(db: Session) -> Dict[str, Dict[str, Any]]:
    rows = db.execute(
        select(Customer.user_id, Customer.email, Customer.first_name, Customer.last_name, Customer.registration_date,
               Customer.country, Customer.region, Customer.account_status, Customer.customer_tier, Customer.address,
               Customer.marketing_consent)
    ).all()
    return {r.user_id: dict(r._mapping) for r in rows}


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


@ttl_cache(300)
def _customer_rows(db: Session) -> List[Dict[str, Any]]:
    directory = _directory(db)
    now = data_now(db)
    out = []
    for r in customer_rfm(db):
        c = directory.get(r["user_id"])
        if not c:
            continue
        tenure_months = max((now - datetime.fromisoformat(r["first_order"])).days / 30.4, 1.0)
        risk = churn_risk(r["recency_days"], r["avg_interval_days"])
        out.append({
            "id": r["user_id"], "email": c["email"], "created_at": _iso(c["registration_date"]), "last_purchase": r["last_order"],
            "analytics": {
                "lifetime_value": round(r["monetary"], 2), "total_orders": r["frequency"], "total_spent": round(r["monetary"], 2),
                "average_order_value": round(r["monetary"] / r["frequency"], 2), "rfm_segment": r["segment"],
                "recency_days": r["recency_days"], "frequency_score": r["f"], "monetary_score": r["m"],
                "churn_risk": risk, "churn_risk_level": risk_level(risk),
                "engagement_score": round(min(1.0, r["frequency"] / tenure_months / 1.5), 3),
            },
            "demographics": {"location": {"city": (c["address"] or {}).get("city"), "country": c["country"], "region": c["region"]}},
        })
    return out


def list_customers(db: Session, segment: Optional[str], risk: Optional[str], sort_by: str, page: int, size: int) -> Dict[str, Any]:
    rows = _customer_rows(db)
    if segment:
        rows = [r for r in rows if r["analytics"]["rfm_segment"].lower() == segment.lower()]
    if risk:
        rows = [r for r in rows if r["analytics"]["churn_risk_level"] == risk.lower()]
    rows = sorted(rows, key=SORT_KEYS.get(sort_by, SORT_KEYS["lifetime_value"]))
    total = len(rows)
    pages = max(math.ceil(total / size), 1)
    chunk = rows[(page - 1) * size : page * size]

    everyone = _customer_rows(db)
    dist = Counter(r["analytics"]["rfm_segment"] for r in everyone)
    ltv = [r["analytics"]["lifetime_value"] for r in everyone]
    return {
        "items": chunk,
        "pagination": {"page": page, "size": size, "total_items": total, "total_pages": pages,
                       "has_next": page < pages, "has_previous": page > 1, "next_page": page + 1 if page < pages else None},
        "summary": {"total_items": total, "current_page_items": len(chunk), "page": page, "total_pages": pages},
        "filters": {"segment": segment, "risk_level": risk, "sort_by": sort_by},
        "analytics_summary": {
            "avg_lifetime_value": round(sum(ltv) / len(ltv), 2) if ltv else 0.0,
            "high_value_customers": sum(1 for v in ltv if v >= 1000),
            "at_risk_customers": sum(1 for r in everyone if r["analytics"]["churn_risk_level"] == "high"),
            "segment_distribution": dict(dist),
        },
    }


def customer_detail(db: Session, customer_id: str, include_history: bool = True) -> Optional[Dict[str, Any]]:
    directory = _directory(db)
    base = directory.get(customer_id)
    if not base:
        return None
    row = next((r for r in _customer_rows(db) if r["id"] == customer_id), None)
    rfm = next((r for r in customer_rfm(db) if r["user_id"] == customer_id), None)

    orders = db.execute(
        select(Order.order_id, Order.order_date, Order.total_amount, Order.order_status, Order.payment_status)
        .where(Order.user_id == customer_id).order_by(Order.order_date.desc())
    ).all()
    n_orders = len(orders)
    items = db.execute(
        select(OrderItem.order_id, OrderItem.product_category, OrderItem.line_total, OrderItem.quantity)
        .join(Order, Order.order_id == OrderItem.order_id).where(Order.user_id == customer_id)
    ).all()
    by_order = defaultdict(list)
    spend_by_cat: Counter = Counter()
    for it in items:
        by_order[it.order_id].append(it)
        spend_by_cat[it.product_category] += f(it.line_total)
    refunded = sum(1 for o in orders if o.order_status == "refunded")
    cancelled = sum(1 for o in orders if o.order_status == "cancelled")

    history = []
    if include_history:
        history = [{
            "order_id": o.order_id, "date": _iso(o.order_date), "amount": f(o.total_amount, 2),
            "items_count": sum(i.quantity for i in by_order[o.order_id]), "status": o.order_status,
            "categories": sorted({i.product_category for i in by_order[o.order_id]}),
        } for o in orders[:10]]

    a = row["analytics"] if row else {}
    return {
        "id": customer_id, "email": base["email"], "created_at": _iso(base["registration_date"]),
        "last_purchase": row["last_purchase"] if row else None, "status": base["account_status"], "tier": base["customer_tier"],
        "profile": {
            "first_name": base["first_name"], "last_name": base["last_name"],
            "address": {"city": (base["address"] or {}).get("city"), "country": base["country"], "region": base["region"]},
            "preferences": {"marketing_consent": bool(base["marketing_consent"])},
        },
        "analytics": {
            "lifetime_value": a.get("lifetime_value", 0.0), "total_orders": a.get("total_orders", 0),
            "total_spent": a.get("total_spent", 0.0), "average_order_value": a.get("average_order_value", 0.0),
            "rfm_analysis": {"recency": rfm["recency_days"], "frequency": rfm["frequency"], "monetary": round(rfm["monetary"], 2),
                             "rfm_score": rfm["rfm_score"], "segment": rfm["segment"]} if rfm else None,
            "behavior": {
                "preferred_categories": [c for c, _ in spend_by_cat.most_common(3)],
                "return_rate": round(refunded / n_orders, 3) if n_orders else 0.0,
                "cancellation_rate": round(cancelled / n_orders, 3) if n_orders else 0.0,
            },
        },
        "purchase_history": history,
        "predictions": value_prediction(db, customer_id, row=row, rfm=rfm) if row else None,
    }


def value_prediction(db: Session, customer_id: str, row: Optional[Dict[str, Any]] = None,
                     rfm: Optional[Dict[str, Any]] = None, horizon_months: int = 12) -> Optional[Dict[str, Any]]:
    """12-month outlook = observed monthly spend x months x (1 - churn risk)."""
    row = row or next((r for r in _customer_rows(db) if r["id"] == customer_id), None)
    rfm = rfm or next((r for r in customer_rfm(db) if r["user_id"] == customer_id), None)
    if not row or not rfm:
        return None
    tenure_months = max((data_now(db) - datetime.fromisoformat(rfm["first_order"])).days / 30.4, 1.0)
    monthly = rfm["monetary"] / tenure_months
    risk = row["analytics"]["churn_risk"]
    predicted = monthly * horizon_months * (1 - risk)
    spread = 0.25 + 0.5 * (1 / math.sqrt(rfm["frequency"]))  # fewer orders -> wider band
    return {
        "customer_id": customer_id, "method": "heuristic: observed spend rate x (1 - churn risk)",
        "prediction_horizon": f"{horizon_months}m", "generated_at": datetime.utcnow().isoformat(),
        "current_ltv": round(rfm["monetary"], 2),
        "predicted_ltv": round(predicted, 2),
        "confidence_interval": {"lower": round(predicted * (1 - spread), 2), "upper": round(predicted * (1 + spread), 2)},
        "churn_risk": {"probability": risk, "risk_level": risk_level(risk)},
        "drivers": {"monthly_spend": round(monthly, 2), "orders": rfm["frequency"], "recency_days": rfm["recency_days"],
                    "avg_interval_days": rfm["avg_interval_days"]},
    }


def recommendations(db: Session, customer_id: str, limit: int = 10) -> Optional[Dict[str, Any]]:
    """Products bought together with what this customer already bought (item co-occurrence)."""
    if customer_id not in _directory(db):
        return None
    mine = {r.product_id for r in db.execute(
        select(OrderItem.product_id).join(Order, Order.order_id == OrderItem.order_id).where(Order.user_id == customer_id)
    )}
    a, b = OrderItem.__table__.alias("a"), OrderItem.__table__.alias("b")
    recs: List[Dict[str, Any]] = []
    if mine:
        co = db.execute(
            select(b.c.product_id, func.count().label("n"))
            .select_from(a.join(b, and_(a.c.order_id == b.c.order_id, a.c.product_id != b.c.product_id)))
            .where(a.c.product_id.in_(mine), b.c.product_id.notin_(mine)).group_by(b.c.product_id)
            .order_by(func.count().desc()).limit(limit)
        ).all()
        top = co[0].n if co else 1
        products = {p.product_id: p for p in db.execute(select(Product).where(Product.product_id.in_([r.product_id for r in co]))).scalars()}
        recs = [{"product_id": r.product_id, "name": products[r.product_id].name, "category": products[r.product_id].category_name,
                 "price": f(products[r.product_id].price), "rating": f(products[r.product_id].rating_avg, 2),
                 "confidence_score": round(r.n / top, 3), "reason": "frequently_bought_together", "co_purchases": int(r.n)}
                for r in co]
    if len(recs) < limit:  # top up with best-sellers in the customer's favourite category
        fav = db.execute(
            select(OrderItem.product_category, func.sum(OrderItem.line_total)).join(Order, Order.order_id == OrderItem.order_id)
            .where(Order.user_id == customer_id).group_by(OrderItem.product_category).order_by(func.sum(OrderItem.line_total).desc()).limit(1)
        ).first()
        stmt = select(Product, func.coalesce(func.sum(OrderItem.quantity), 0).label("u")).outerjoin(
            OrderItem, OrderItem.product_id == Product.product_id).where(
            Product.product_id.notin_(mine | {r["product_id"] for r in recs}), Product.inventory_count > 0)
        if fav:
            stmt = stmt.where(Product.category_name == fav[0])
        stmt = stmt.group_by(Product.product_id).order_by(func.sum(OrderItem.quantity).desc()).limit(limit - len(recs))
        for p, units in db.execute(stmt).all():
            recs.append({"product_id": p.product_id, "name": p.name, "category": p.category_name, "price": f(p.price),
                         "rating": f(p.rating_avg, 2), "confidence_score": 0.3, "reason": "popular_in_favourite_category",
                         "units_sold": int(units)})
    return {"customer_id": customer_id, "recommendation_type": "products", "method": "item co-occurrence",
            "generated_at": datetime.utcnow().isoformat(), "recommendations": recs[:limit]}
