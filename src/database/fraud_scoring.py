"""
Rule-based fraud scoring over stored orders.

Each rule has a weight (the probability-like strength of the signal). An order's
risk score combines the triggered rules as 1 - prod(1 - weight), and an alert is
raised when the score reaches ALERT_THRESHOLD.

Because the orders carry a known outcome (`is_fraudulent`, e.g. a chargeback),
the precision and recall of each rule can be measured - see
src/api/repositories/fraud.py.
"""

import random
from collections import defaultdict
from datetime import datetime, timedelta
from statistics import median
from typing import Any, Dict, List, Sequence

ALERT_THRESHOLD = 0.5
NEW_ACCOUNT_MIN_AMOUNT = 1200.0  # typical first orders are far smaller

# rule name -> (weight, alert_type shown on the dashboard, description)
RULES = {
    "high_amount": (0.45, "amount", "Order value far above this customer's norm"),
    "velocity": (0.55, "velocity", "Several orders placed within minutes"),
    "new_account_high_value": (0.60, "behavioral", "High-value order on a brand-new account"),
    "payment_failures": (0.50, "pattern", "Order following repeated failed payments"),
    "night_high_value": (0.25, "behavioral", "High-value order in the small hours"),
}


def priority_for(score: float) -> str:
    if score >= 0.85:
        return "critical"
    if score >= 0.70:
        return "high"
    if score >= 0.60:
        return "medium"
    return "low"


def combine(weights: Sequence[float]) -> float:
    remaining = 1.0
    for w in weights:
        remaining *= 1 - w
    return round(1 - remaining, 3)


def triggered_rules(orders: List[Dict[str, Any]], registered: Dict[str, datetime]) -> Dict[str, List[str]]:
    """Return {order_id: [rule names]} for orders that trip at least one rule.

    `orders` need: order_id, user_id, order_date, total_amount, payment_status.
    `registered` maps user_id -> registration datetime.
    """
    by_user: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for o in orders:
        by_user[o["user_id"]].append(o)

    hits: Dict[str, List[str]] = {}
    for uid, user_orders in by_user.items():
        user_orders.sort(key=lambda o: o["order_date"])
        prior_amounts: List[float] = []
        times = [o["order_date"] for o in user_orders]
        for i, o in enumerate(user_orders):
            ts, amount = o["order_date"], float(o["total_amount"])
            rules: List[str] = []

            norm = median(prior_amounts) if len(prior_amounts) >= 2 else 0.0
            if amount > max(2500.0, 5 * norm):
                rules.append("high_amount")

            recent = sum(1 for t in times[max(0, i - 8) : i + 1] if ts - t <= timedelta(minutes=15))
            if recent >= 3:
                rules.append("velocity")

            reg = registered.get(uid)
            if reg and (ts - reg) < timedelta(days=7) and amount > NEW_ACCOUNT_MIN_AMOUNT:
                rules.append("new_account_high_value")

            failures = sum(
                1
                for prev in user_orders[max(0, i - 6) : i]
                if prev["payment_status"] == "failed" and ts - prev["order_date"] <= timedelta(hours=1)
            )
            if failures >= 2 and o["payment_status"] != "failed":
                rules.append("payment_failures")

            if 1 <= ts.hour <= 5 and amount > 800:
                rules.append("night_high_value")

            if rules:
                hits[o["order_id"]] = rules
            prior_amounts.append(amount)
    return hits


def score_orders(
    orders: List[Dict[str, Any]],
    customers: Dict[str, Dict[str, Any]],
    now: datetime,
    rng: random.Random,
    investigators: Sequence[str],
) -> List[Dict[str, Any]]:
    """Build fraud_alerts rows for orders whose combined risk reaches the threshold."""
    registered = {uid: c["registration_date"] for uid, c in customers.items()}
    hits = triggered_rules(orders, registered)
    by_id = {o["order_id"]: o for o in orders}

    alerts: List[Dict[str, Any]] = []
    for n, (order_id, rules) in enumerate(sorted(hits.items(), key=lambda kv: by_id[kv[0]]["order_date"]), 1):
        score = combine([RULES[r][0] for r in rules])
        if score < ALERT_THRESHOLD:
            continue
        o = by_id[order_id]
        lead = max(rules, key=lambda r: RULES[r][0])
        created = o["order_date"] + timedelta(milliseconds=rng.randint(20, 160))  # rules are cheap: scored as orders arrive
        age = now - created
        fraud = bool(o["is_fraudulent"])

        # Investigation outcome follows the known outcome once the alert is old enough;
        # anything older than a week has been worked to a conclusion.
        if age < timedelta(days=2):
            status = rng.choices(["open", "investigating", "escalated"], weights=[62, 33, 5])[0]
        elif age < timedelta(days=7):
            if fraud:
                status = rng.choices(["resolved", "investigating", "open"], weights=[78, 14, 8])[0]
            else:
                status = rng.choices(["false_positive", "resolved", "investigating"], weights=[78, 12, 10])[0]
        elif fraud:
            status = rng.choices(["resolved", "false_positive"], weights=[96, 4])[0]
        else:
            status = rng.choices(["false_positive", "resolved"], weights=[85, 15])[0]
        closed = status in ("resolved", "false_positive")
        resolved_at = created + timedelta(hours=rng.uniform(1, 46)) if closed else None
        alerts.append(
            {
                "alert_id": f"alert_{n:06d}",
                "order_id": order_id,
                "user_id": o["user_id"],
                "alert_type": RULES[lead][1],
                "priority": priority_for(score),
                "status": status,
                "risk_score": score,
                "amount": o["total_amount"],
                "description": RULES[lead][2],
                "rules_triggered": rules,
                "assigned_to": None if status == "open" else rng.choice(list(investigators)),
                "created_at": created,
                "updated_at": resolved_at or created + timedelta(minutes=rng.randint(1, 90)),
                "resolved_at": resolved_at,
            }
        )
    return alerts
