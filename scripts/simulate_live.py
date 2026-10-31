#!/usr/bin/env python3
"""
Simulate live store traffic by inserting new rows into the analytics database.

There is no real storefront feeding this project, so this script plays that role:
every few seconds it creates a new order (with line items and stock movement), the
clickstream sessions around it (including sessions that never buy), and runs the
fraud rules over the customer's recent orders, raising an alert when they fire.
The dashboards then show genuinely changing data.

    python scripts/simulate_live.py                       # local SQLite, ~6 orders/min
    python scripts/simulate_live.py --supabase --orders-per-minute 12
    python scripts/simulate_live.py --duration 600         # stop after 10 minutes
"""

import argparse
import random
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from seed_database import DEFAULT_URL, dotenv_value, load_dotenv_url  # noqa: E402
from sqlalchemy import create_engine, func, insert, select, update  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from src.database.demo_seed import (  # noqa: E402
    CHANNELS, PAYMENT_METHODS, TRAFFIC_SOURCES, _choice,
)
from src.database.fraud_scoring import ALERT_THRESHOLD, RULES, combine, priority_for, triggered_rules  # noqa: E402
from src.database.models import Customer, FraudAlert, Order, OrderItem, Product, UserEvent  # noqa: E402

FUNNEL = ["page_view", "product_view", "add_to_cart", "checkout_start"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--supabase", action="store_true", help="use ECAP_SUPABASE_URL from .env")
    parser.add_argument("--orders-per-minute", type=float, default=6.0)
    parser.add_argument("--session-conversion", type=float, default=0.08, help="share of sessions that buy")
    parser.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until Ctrl+C)")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    url = args.database_url or (dotenv_value("ECAP_SUPABASE_URL") if args.supabase else load_dotenv_url()) or DEFAULT_URL
    engine = create_engine(url, pool_pre_ping=True)
    rng = random.Random(args.seed)
    print(f"Simulating live traffic into {url.split('@')[-1] if '@' in url else url} "
          f"at ~{args.orders_per_minute:g} orders/min (Ctrl+C to stop)")

    with Session(engine) as db:
        products = db.execute(select(Product).where(Product.status == "active")).scalars().all()
        customers = db.execute(select(Customer.user_id, Customer.registration_date, Customer.total_orders)).all()
    if not products or not customers:
        sys.exit("The database is empty - run scripts/seed_database.py first")
    registered = {c.user_id: c.registration_date for c in customers}
    weights = [1 + c.total_orders for c in customers]
    pop = [1 / (i + 1) ** 0.7 for i in range(len(products))]
    products.sort(key=lambda p: p.product_id)
    n_orders = n_sessions = n_alerts = 0
    started = time.time()

    try:
        while not args.duration or time.time() - started < args.duration:
            time.sleep(rng.expovariate(args.orders_per_minute / 60))
            now = datetime.utcnow().replace(microsecond=0)
            uid = rng.choices([c.user_id for c in customers], weights=weights)[0]
            channel = _choice(rng, CHANNELS)
            picks = rng.choices(products, weights=pop, k=rng.choices([1, 2, 3], weights=[60, 28, 12])[0])
            lines = [(p, rng.choices([1, 2], weights=[80, 20])[0]) for p in picks]
            subtotal = round(sum(float(p.price) * q for p, q in lines), 2)
            discount = round(subtotal * rng.choice([0.05, 0.1]), 2) if rng.random() < 0.2 else 0.0
            tax = round((subtotal - discount) * 0.08, 2)
            shipping = 0.0 if subtotal > 75 else rng.choice([5.99, 7.99])
            failed = rng.random() < 0.015
            order_id = f"ord_live_{uuid.uuid4().hex[:10]}"
            device = {"mobile_app": "mobile", "online": rng.choice(["desktop", "mobile"]), "in_store": "pos"}[channel]
            source = _choice(rng, TRAFFIC_SOURCES)

            events = []
            for sess_uid, converts in [(uid, True)] + [(rng.choice([uid, None]), False) for _ in range(rng.randint(0, int(1 / args.session_conversion) - 1))]:
                sess = uuid.uuid4().hex
                depth = len(FUNNEL) + 1 if converts else rng.choices([1, 2, 3, 4], weights=[45, 30, 15, 10])[0]
                for i, etype in enumerate((FUNNEL + ["purchase"])[:depth]):
                    pid = None if etype == "page_view" else (picks[0].product_id if converts else rng.choice(products).product_id)
                    events.append({"event_id": uuid.uuid4().hex, "user_id": sess_uid, "session_id": sess, "event_type": etype,
                                   "product_id": pid, "device_type": device, "traffic_source": source,
                                   "event_timestamp": now - timedelta(seconds=(depth - i) * rng.randint(20, 90) if converts else rng.randint(0, 59))})
                n_sessions += 1

            with Session(engine) as db:
                db.execute(insert(Order), [{
                    "order_id": order_id, "user_id": uid, "order_date": now, "order_status": "cancelled" if failed else "confirmed",
                    "subtotal": subtotal, "tax_amount": tax, "shipping_cost": shipping, "discount_amount": discount,
                    "total_amount": round(subtotal - discount + tax + shipping, 2), "payment_method": _choice(rng, PAYMENT_METHODS),
                    "payment_status": "failed" if failed else "paid", "payment_date": None if failed else now, "currency": "USD",
                    "channel": channel, "is_fraudulent": False, "created_at": now, "updated_at": now,
                }])
                db.execute(insert(OrderItem), [{
                    "order_item_id": f"{order_id}_{i}", "order_id": order_id, "product_id": p.product_id, "quantity": q,
                    "unit_price": float(p.price), "discount_amount": 0.0, "line_total": round(float(p.price) * q, 2),
                    "product_name": p.name, "product_sku": p.sku, "product_category": p.category_name, "created_at": now,
                } for i, (p, q) in enumerate(lines)])
                if not failed:
                    for p, q in lines:  # stock movement
                        db.execute(update(Product).where(Product.product_id == p.product_id)
                                   .values(inventory_count=func.max(Product.inventory_count - q, 0) if db.get_bind().dialect.name == "sqlite"
                                           else func.greatest(Product.inventory_count - q, 0)))
                    db.execute(update(Customer).where(Customer.user_id == uid).values(
                        total_orders=Customer.total_orders + 1,
                        lifetime_value=Customer.lifetime_value + round(subtotal - discount, 2), last_login=now))
                db.execute(insert(UserEvent), events)

                # fraud rules over this customer's most recent orders
                recent = db.execute(select(Order.order_id, Order.user_id, Order.order_date, Order.total_amount, Order.payment_status)
                                    .where(Order.user_id == uid).order_by(Order.order_date.desc()).limit(10)).mappings().all()
                hit = triggered_rules([dict(r) for r in recent], registered).get(order_id)
                if hit and combine([RULES[r][0] for r in hit]) >= ALERT_THRESHOLD:
                    score = combine([RULES[r][0] for r in hit])
                    lead = max(hit, key=lambda r: RULES[r][0])
                    db.execute(insert(FraudAlert), [{
                        "alert_id": f"alert_live_{uuid.uuid4().hex[:8]}", "order_id": order_id, "user_id": uid,
                        "alert_type": RULES[lead][1], "priority": priority_for(score), "status": "open", "risk_score": score,
                        "amount": round(subtotal - discount + tax + shipping, 2), "description": RULES[lead][2],
                        "rules_triggered": hit, "assigned_to": None, "created_at": now, "updated_at": now,
                    }])
                    n_alerts += 1
                db.commit()
            n_orders += 1
            if n_orders % 5 == 0:
                print(f"  {n_orders} orders, {n_sessions} sessions, {n_alerts} alerts  ({datetime.utcnow():%H:%M:%S})")
    except KeyboardInterrupt:
        pass
    print(f"stopped: {n_orders} orders, {n_sessions} sessions, {n_alerts} alerts in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
