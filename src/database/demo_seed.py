"""
Deterministic generator that fills the analytics database with a year of
realistic e-commerce activity.

There is no live storefront feeding this project, so the *records* are generated
(customers, products, orders, order items, clickstream events, and a few injected
fraud patterns). Everything the API reports is then computed from those stored rows
with SQL - nothing is hard-coded in the endpoints.

Usage (see scripts/seed_database.py):
    seed_database(engine, customers=4000, days=365, seed=42)
"""

import math
import random
import uuid
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import accumulate
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import insert
from sqlalchemy.engine import Engine

from .fraud_scoring import score_orders
from .models import (
    Base,
    Category,
    Customer,
    FraudAlert,
    Order,
    OrderItem,
    Product,
    Supplier,
    UserEvent,
)

# category_id, name, (min price, max price), (min margin, max margin), popularity,
# months with a seasonal lift
CATEGORIES = [
    ("cat_electronics", "Electronics", (25, 1800), (0.10, 0.25), 0.22, {11: 1.5, 12: 1.6}),
    ("cat_clothing", "Clothing", (12, 220), (0.35, 0.60), 0.20, {3: 1.15, 9: 1.2, 11: 1.3}),
    ("cat_home", "Home & Garden", (10, 600), (0.25, 0.45), 0.16, {4: 1.25, 5: 1.3}),
    ("cat_books", "Books", (6, 45), (0.30, 0.50), 0.10, {8: 1.3, 12: 1.2}),
    ("cat_sports", "Sports & Outdoors", (14, 450), (0.25, 0.42), 0.10, {5: 1.2, 6: 1.3}),
    ("cat_beauty", "Beauty", (8, 120), (0.45, 0.70), 0.08, {12: 1.3}),
    ("cat_toys", "Toys & Games", (8, 150), (0.30, 0.50), 0.08, {11: 1.5, 12: 1.9}),
    ("cat_grocery", "Grocery", (3, 60), (0.12, 0.25), 0.06, {}),
]
NOUNS = {
    "cat_electronics": ["Headphones", "Laptop", "Smartwatch", "Tablet", "Speaker", "Monitor", "Camera", "Router"],
    "cat_clothing": ["Jacket", "Sneakers", "T-Shirt", "Jeans", "Hoodie", "Dress", "Backpack", "Watch Strap"],
    "cat_home": ["Lamp", "Cookware Set", "Vacuum", "Bedding Set", "Planter", "Blender", "Shelf", "Rug"],
    "cat_books": ["Novel", "Cookbook", "Biography", "Guide", "Atlas", "Workbook", "Anthology", "Textbook"],
    "cat_sports": ["Yoga Mat", "Dumbbells", "Tent", "Bike Light", "Running Belt", "Racket", "Helmet", "Bottle"],
    "cat_beauty": ["Moisturizer", "Serum", "Shampoo", "Perfume", "Lip Balm", "Sunscreen", "Face Mask", "Trimmer"],
    "cat_toys": ["Puzzle", "Board Game", "Building Set", "Action Figure", "Plush", "RC Car", "Doll House", "Cards"],
    "cat_grocery": ["Coffee Beans", "Olive Oil", "Granola", "Tea Box", "Pasta Pack", "Honey", "Snack Box", "Spice Set"],
}
ADJECTIVES = ["Classic", "Pro", "Eco", "Ultra", "Compact", "Premium", "Smart", "Essential", "Deluxe", "Lite"]
BRANDS = ["Northwind", "Acme", "Zenith", "Orbit", "Lumen", "Harbor", "Nimbus", "Cobalt", "Aster", "Vertex"]
# country, region, weight
COUNTRIES = [
    ("US", "North America", 38), ("CA", "North America", 6), ("GB", "Europe", 9),
    ("DE", "Europe", 7), ("FR", "Europe", 5), ("IN", "Asia", 12), ("JP", "Asia", 4),
    ("SG", "Asia", 2), ("AU", "Other", 4), ("BR", "Other", 5), ("ZA", "Other", 2),
]
FIRST_NAMES = ["Alex", "Sam", "Jordan", "Taylor", "Morgan", "Riley", "Casey", "Jamie", "Avery", "Quinn",
               "Priya", "Chen", "Maria", "Omar", "Elena", "Kenji", "Fatima", "Lucas", "Nora", "Ravi"]
LAST_NAMES = ["Smith", "Garcia", "Kumar", "Chen", "Müller", "Silva", "Okafor", "Tanaka", "Brown", "Lopez",
              "Patel", "Nguyen", "Rossi", "Kim", "Singh", "Dubois", "Khan", "Olsen", "Costa", "Ito"]
PAYMENT_METHODS = [("credit_card", 46), ("debit_card", 20), ("paypal", 16), ("apple_pay", 8), ("bank_transfer", 6), ("gift_card", 4)]
CHANNELS = [("online", 62), ("mobile_app", 28), ("in_store", 10)]
TRAFFIC_SOURCES = [("organic", 34), ("direct", 24), ("paid", 18), ("social", 14), ("email", 10)]
WEEKDAY_FACTOR = [1.0, 0.97, 0.96, 1.0, 1.1, 1.22, 1.14]  # Mon..Sun
INVESTIGATORS = ["a.rahman", "m.fischer", "s.okoye"]


def _choice(rng: random.Random, pairs: List[Tuple[Any, float]]) -> Any:
    items, weights = zip(*pairs)
    return rng.choices(items, weights=weights, k=1)[0]


def _chunks(rows: List[Dict[str, Any]], size: int = 1500):
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def _bulk_insert(engine: Engine, model, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with engine.begin() as conn:
        for chunk in _chunks(rows):
            conn.execute(insert(model), chunk)


def _season_factor(month: int) -> float:
    boost = {11: 1.25, 12: 1.4, 1: 0.9, 2: 0.88, 7: 0.95}
    return boost.get(month, 1.0)


def seed_database(
    engine: Engine,
    customers: int = 4000,
    days: int = 365,
    seed: int = 42,
    base_daily_orders: int = 55,
    event_days: int = 30,
    session_conversion: float = 0.08,
    reset: bool = True,
    log=print,
) -> Dict[str, int]:
    """Create the schema and fill it. Returns row counts per table."""
    rng = random.Random(seed)
    now = datetime.utcnow().replace(microsecond=0)
    start = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0)

    if reset:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    # ---- categories & suppliers -------------------------------------------------
    cat_rows = [
        {"category_id": c[0], "name": c[1], "description": f"{c[1]} products",
         "level": 1, "sort_order": i, "status": "active"}
        for i, c in enumerate(CATEGORIES)
    ]
    _bulk_insert(engine, Category, cat_rows)
    supplier_rows = [
        {"supplier_id": f"sup_{i:02d}", "name": f"{BRANDS[i % len(BRANDS)]} Supply Co.",
         "email": f"orders@supplier{i}.example.com", "rating": round(rng.uniform(3.6, 4.9), 2),
         "status": "active"}
        for i in range(8)
    ]
    _bulk_insert(engine, Supplier, supplier_rows)

    # ---- products --------------------------------------------------------------
    products: List[Dict[str, Any]] = []
    products_by_cat: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for cat_id, cat_name, (pmin, pmax), (mmin, mmax), _, _ in CATEGORIES:
        for i in range(60):
            price = round(math.exp(rng.uniform(math.log(pmin), math.log(pmax))), 2)
            margin = rng.uniform(mmin, mmax)
            noun = NOUNS[cat_id][i % 8]
            brand = BRANDS[(i * 7 + len(cat_id)) % len(BRANDS)]
            product = {
                "product_id": f"prod_{cat_id[4:8]}_{i:03d}",
                "name": f"{brand} {ADJECTIVES[(i // 8) % len(ADJECTIVES)]} {noun}",
                "description": f"{cat_name}: {noun}",
                "sku": f"SKU-{cat_id[4:8].upper()}-{i:04d}",
                "category_id": cat_id, "category_name": cat_name,
                "subcategory": noun, "brand": brand,
                "price": price, "cost": round(price * (1 - margin), 2),
                "inventory_count": rng.choice([0, 0, 3, 8, 15, 40, 90, 160, 320]) if i % 17 == 0
                else rng.randint(12, 400),
                "status": "active",
                "created_at": start - timedelta(days=rng.randint(10, 400)),
                "rating_avg": round(min(4.9, max(3.0, rng.gauss(4.1, 0.45))), 2),
                "review_count": int(rng.paretovariate(1.3) * 8),
                "supplier_id": f"sup_{rng.randrange(8):02d}",
            }
            products.append(product)
            products_by_cat[cat_id].append(product)
    _bulk_insert(engine, Product, products)
    # Zipf-like popularity inside each category
    cat_weights = {c[0]: c[4] for c in CATEGORIES}
    SEASON = {c[0]: c[5] for c in CATEGORIES}
    prod_cum: Dict[str, List[float]] = {
        cat: list(accumulate(1 / (rank + 1) ** 0.9 for rank in range(len(items))))
        for cat, items in products_by_cat.items()
    }

    # ---- customers ---------------------------------------------------------------
    horizon = days + 365  # customers registered up to a year before the window
    cust_rows: List[Dict[str, Any]] = []
    for i in range(customers):
        offset = horizon * (rng.random() ** 0.62)  # more recent registrations
        reg = start - timedelta(days=365) + timedelta(days=offset, seconds=rng.randint(0, 86399))
        reg = min(reg, now - timedelta(hours=2)).replace(microsecond=0)
        country, region, _ = _choice(rng, [(c, c[2]) for c in COUNTRIES])
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        cust_rows.append({
            "user_id": f"cust_{i:05d}",
            "email": f"{first.lower()}.{last.lower()}{i}@example.com",
            "first_name": first, "last_name": last,
            "registration_date": reg,
            "account_status": "active" if rng.random() > 0.04 else "inactive",
            "customer_tier": "bronze", "lifetime_value": 0, "total_orders": 0,
            "country": country, "region": region,
            "preferred_language": "en", "marketing_consent": rng.random() < 0.55,
            "address": {"country": country, "city": f"City {rng.randint(1, 60)}"},
            "_activity": min(30.0, rng.paretovariate(1.25)),
            "_fav": rng.sample([c[0] for c in CATEGORIES], 2),
        })
    cust_by_id = {c["user_id"]: c for c in cust_rows}

    # ---- orders -----------------------------------------------------------------
    orders: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []
    order_seq = 0

    def make_order(cust: Dict[str, Any], ts: datetime, forced_amount: Optional[Tuple[float, float]] = None,
                   fraud: bool = False, payment_failed: bool = False, hour_override: Optional[int] = None) -> Dict[str, Any]:
        nonlocal order_seq
        order_seq += 1
        oid = f"ord_{order_seq:07d}"
        if hour_override is not None:
            ts = ts.replace(hour=hour_override)
            if ts <= cust["registration_date"]:  # never before the account existed
                ts += timedelta(days=1)
        n_items = rng.choices([1, 2, 3, 4], weights=[52, 28, 14, 6])[0]
        cats = [c for c in cust["_fav"] if rng.random() < 0.7] or [None]
        lines: List[Dict[str, Any]] = []
        subtotal = 0.0
        for _ in range(n_items):
            if cats[0] is not None:
                cat = rng.choice(cats)
            else:  # popularity x seasonal lift for this month
                cat = rng.choices(
                    list(cat_weights),
                    weights=[v * SEASON[k].get(ts.month, 1.0) for k, v in cat_weights.items()],
                )[0]
            cum = prod_cum[cat]
            prod = products_by_cat[cat][bisect_right(cum, rng.random() * cum[-1]) % len(cum)]
            qty = rng.choices([1, 2, 3], weights=[72, 20, 8])[0]
            if forced_amount:
                prod = max(products_by_cat["cat_electronics"], key=lambda p: p["price"] * rng.random())
                qty = rng.choice([1, 2])
            line_total = round(prod["price"] * qty, 2)
            subtotal += line_total
            lines.append({"prod": prod, "qty": qty, "line_total": line_total})
        if forced_amount:
            target = rng.uniform(*forced_amount)
            scale = target / max(subtotal, 1)
            for ln in lines:
                ln["line_total"] = round(ln["line_total"] * scale, 2)
            subtotal = sum(ln["line_total"] for ln in lines)
        discount = round(subtotal * rng.choice([0.05, 0.1, 0.15]), 2) if rng.random() < 0.22 else 0.0
        tax = round((subtotal - discount) * 0.08, 2)
        shipping = 0.0 if subtotal > 75 else rng.choice([5.99, 7.99, 9.99])
        total = round(subtotal - discount + tax + shipping, 2)

        age_days = (now - ts).total_seconds() / 86400
        if payment_failed:
            status, pay_status = "cancelled", "failed"
        elif age_days < 0.4:
            status = rng.choice(["pending", "confirmed", "processing"])
            pay_status = "paid" if status != "pending" else "pending"
        elif age_days < 4:
            status = rng.choices(["processing", "shipped", "delivered"], weights=[25, 45, 30])[0]
            pay_status = "paid"
        else:
            status = rng.choices(["delivered", "cancelled", "refunded"], weights=[92, 4, 4])[0]
            pay_status = {"delivered": "paid", "cancelled": "cancelled", "refunded": "refunded"}[status]
        channel = _choice(rng, CHANNELS)
        order = {
            "order_id": oid, "user_id": cust["user_id"], "order_date": ts,
            "order_status": status, "subtotal": round(subtotal, 2), "tax_amount": tax,
            "shipping_cost": shipping, "discount_amount": discount, "total_amount": total,
            "payment_method": _choice(rng, PAYMENT_METHODS), "payment_status": pay_status,
            "payment_date": ts if pay_status in ("paid", "refunded") else None,
            "shipping_method": rng.choice(["standard", "express", "pickup"]),
            "shipped_date": ts + timedelta(days=1) if status in ("shipped", "delivered") else None,
            "delivered_date": ts + timedelta(days=rng.randint(3, 6)) if status == "delivered" else None,
            "currency": "USD", "channel": channel, "is_fraudulent": fraud,
            "created_at": ts, "updated_at": ts,
        }
        orders.append(order)
        for ln in lines:
            p = ln["prod"]
            items.append({
                "order_item_id": f"{oid}_{len(items)}", "order_id": oid, "product_id": p["product_id"],
                "quantity": ln["qty"], "unit_price": round(ln["line_total"] / ln["qty"], 2),
                "discount_amount": 0.0, "line_total": ln["line_total"], "product_name": p["name"],
                "product_sku": p["sku"], "product_category": p["category_name"], "created_at": ts,
            })
        return order

    window_customers = sorted(cust_rows, key=lambda c: c["registration_date"])
    ordered_so_far: List[Dict[str, Any]] = [c for c in window_customers if c["registration_date"] < start]
    cum_activity = list(accumulate(c["_activity"] for c in ordered_so_far))
    pending_new = [c for c in window_customers if c["registration_date"] >= start]
    first_order_at = {
        c["user_id"]: c["registration_date"] + timedelta(days=rng.expovariate(1 / 4), seconds=rng.randint(0, 3600))
        for c in pending_new if rng.random() < 0.82
    }
    first_by_day: Dict[datetime, List[Dict[str, Any]]] = defaultdict(list)
    for uid, ts in first_order_at.items():
        if ts < now:
            first_by_day[ts.replace(hour=0, minute=0, second=0, microsecond=0)].append(
                (cust_by_id[uid], ts.replace(microsecond=0))
            )

    def sample_repeat_customer() -> Dict[str, Any]:
        return ordered_so_far[bisect_right(cum_activity, rng.random() * cum_activity[-1]) % len(ordered_so_far)]

    for d in range(days + 1):
        day = start + timedelta(days=d)
        t = d / max(days, 1)
        expected = base_daily_orders * (1 + 0.7 * t) * WEEKDAY_FACTOR[day.weekday()] * _season_factor(day.month)
        expected *= math.exp(rng.gauss(0, 0.1))
        todays_first = first_by_day.get(day, [])
        n_repeat = max(0, int(round(expected)) - len(todays_first))
        for cust, ts in todays_first:
            make_order(cust, ts)
            ordered_so_far.append(cust)
            cum_activity.append((cum_activity[-1] if cum_activity else 0) + cust["_activity"])
        for _ in range(n_repeat):
            if not ordered_so_far:
                break
            cust = sample_repeat_customer()
            hour_weights = [1, 1, 1, 1, 1, 2, 3, 5, 7, 8, 8, 8, 9, 8, 7, 7, 8, 9, 10, 11, 10, 7, 4, 2]
            hour = rng.choices(range(24), weights=hour_weights)[0]
            ts = day + timedelta(hours=hour, minutes=rng.randint(0, 59), seconds=rng.randint(0, 59))
            if ts > now or ts < cust["registration_date"]:
                continue
            make_order(cust, ts)

    # ---- injected fraud patterns (ground truth for evaluating the rules) --------
    def pick_established() -> Dict[str, Any]:
        return rng.choice([c for c in cust_rows if c["registration_date"] < now - timedelta(days=90)])

    def ts_for(cust: Dict[str, Any], recent_hours: Optional[float] = None) -> datetime:
        """A time for an injected incident that is after the customer registered."""
        if recent_hours is not None:
            return now - timedelta(hours=rng.uniform(2, recent_hours))
        lo = max(cust["registration_date"] + timedelta(days=1), now - timedelta(days=days - 2))
        return lo + timedelta(seconds=rng.random() * (now - timedelta(hours=6) - lo).total_seconds())

    def velocity_burst(recent_hours: Optional[float] = None) -> None:
        cust, is_fraud = pick_established(), rng.random() < 0.75
        base = ts_for(cust, recent_hours)
        for k in range(rng.randint(4, 6)):
            make_order(cust, base + timedelta(minutes=k * rng.uniform(0.8, 2.2)), forced_amount=(380, 1300), fraud=is_fraud)

    def new_account_big_order(n: int, recent_hours: Optional[float] = None) -> None:
        reg = now - timedelta(hours=rng.uniform(14, 60)) if recent_hours else now - timedelta(
            hours=12) - timedelta(seconds=rng.random() * (days - 5) * 86400)
        cust = {"user_id": f"cust_{customers + n:05d}", "email": f"new.user{n}@tempmail.example",
                "first_name": "New", "last_name": f"User{n}", "registration_date": reg.replace(microsecond=0),
                "account_status": "active", "customer_tier": "bronze", "lifetime_value": 0,
                "total_orders": 0, "country": "US", "region": "North America", "preferred_language": "en",
                "marketing_consent": False, "address": {"country": "US"},
                "_activity": 0.1, "_fav": ["cat_electronics", "cat_electronics"]}
        cust_rows.append(cust)
        cust_by_id[cust["user_id"]] = cust
        ts = now - timedelta(hours=rng.uniform(2, recent_hours)) if recent_hours else reg + timedelta(hours=rng.uniform(0.2, 40))
        make_order(cust, ts, forced_amount=(1500, 4800), fraud=rng.random() < 0.8,
                   hour_override=None if recent_hours else rng.choice([2, 3, 4, 23]))

    def amount_outlier(recent_hours: Optional[float] = None) -> None:
        cust = pick_established()
        make_order(cust, ts_for(cust, recent_hours), forced_amount=(2600, 4200), fraud=rng.random() < 0.5)

    def card_testing(recent_hours: Optional[float] = None) -> None:
        cust, is_fraud = pick_established(), rng.random() < 0.85
        base = ts_for(cust, recent_hours)
        for k in range(rng.randint(2, 4)):
            make_order(cust, base + timedelta(minutes=k * 3), forced_amount=(60, 220), fraud=is_fraud, payment_failed=True)
        make_order(cust, base + timedelta(minutes=14), forced_amount=(500, 1400), fraud=is_fraud)

    n_new = 0
    for _ in range(26):
        velocity_burst()
    for _ in range(32):
        new_account_big_order(n_new); n_new += 1
    for _ in range(22):
        amount_outlier()
    for _ in range(14):
        card_testing()
    # a few incidents in the last day so "recent" views are never empty
    velocity_burst(recent_hours=60)
    new_account_big_order(n_new, recent_hours=40); n_new += 1
    amount_outlier(recent_hours=30)
    card_testing(recent_hours=70)

    # ---- customer rollups --------------------------------------------------------
    spend: Dict[str, float] = defaultdict(float)
    count: Dict[str, int] = defaultdict(int)
    last_order: Dict[str, datetime] = {}
    for o in orders:
        if o["order_status"] in ("cancelled", "refunded"):
            continue
        spend[o["user_id"]] += o["total_amount"]
        count[o["user_id"]] += 1
        if o["user_id"] not in last_order or o["order_date"] > last_order[o["user_id"]]:
            last_order[o["user_id"]] = o["order_date"]
    ltv_sorted = sorted(spend.values())
    def pct(p: float) -> float:
        return ltv_sorted[int(p * (len(ltv_sorted) - 1))] if ltv_sorted else 0
    p50, p80, p95 = pct(0.5), pct(0.8), pct(0.95)
    for c in cust_rows:
        uid = c["user_id"]
        c["lifetime_value"] = round(spend.get(uid, 0), 2)
        c["total_orders"] = count.get(uid, 0)
        c["last_login"] = last_order.get(uid, c["registration_date"])
        c["customer_tier"] = ("platinum" if c["lifetime_value"] >= p95 else "gold" if c["lifetime_value"] >= p80
                              else "silver" if c["lifetime_value"] >= p50 else "bronze")
        c.pop("_activity", None)
        c.pop("_fav", None)
    # Insert order matters for foreign keys: customers -> orders -> items -> events -> alerts
    _bulk_insert(engine, Customer, cust_rows)
    _bulk_insert(engine, Order, orders)
    _bulk_insert(engine, OrderItem, items)

    # ---- clickstream events (last `event_days` days) -----------------------------
    events: List[Dict[str, Any]] = []
    ev_start = now - timedelta(days=event_days)
    all_product_ids = [p["product_id"] for p in products]

    def add_event(sess: str, uid: Optional[str], etype: str, ts: datetime, device: str, source: str, pid=None):
        events.append({"event_id": uuid.uuid4().hex, "user_id": uid, "session_id": sess, "event_type": etype,
                       "product_id": pid, "device_type": device, "traffic_source": source, "event_timestamp": ts})

    window_orders = [o for o in orders if o["order_date"] >= ev_start and o["payment_status"] in ("paid", "refunded")]
    products_of_order: Dict[str, List[str]] = defaultdict(list)
    for it in items:
        products_of_order[it["order_id"]].append(it["product_id"])
    for o in window_orders:  # converting sessions follow the whole funnel
        sess = uuid.uuid4().hex
        device = {"mobile_app": "mobile", "online": rng.choice(["desktop", "desktop", "mobile", "tablet"]),
                  "in_store": "pos"}[o["channel"]]
        src = _choice(rng, TRAFFIC_SOURCES)
        ts = o["order_date"]
        bought = products_of_order[o["order_id"]]
        for etype, back in (("page_view", 14), ("product_view", 10), ("add_to_cart", 6), ("checkout_start", 3), ("purchase", 0)):
            # Converting sessions touch the products that were actually bought
            for pid in ([None] if etype == "page_view" else bought if etype != "checkout_start" else [bought[0]]):
                add_event(sess, o["user_id"], etype, ts - timedelta(minutes=back * rng.uniform(0.7, 1.1)),
                          device, src, pid)
    n_sessions = int(len(window_orders) / session_conversion)
    depth_choices = [(1, 45), (2, 30), (3, 15), (4, 10)]  # bounce, viewed, carted, checkout abandon
    types = ["page_view", "product_view", "add_to_cart", "checkout_start"]
    for _ in range(n_sessions - len(window_orders)):
        ts = ev_start + timedelta(seconds=rng.random() * (now - ev_start).total_seconds())
        sess = uuid.uuid4().hex
        uid = rng.choice(cust_rows)["user_id"] if rng.random() < 0.55 else None
        device, src = rng.choice(["desktop", "mobile", "mobile", "tablet"]), _choice(rng, TRAFFIC_SOURCES)
        for step in range(_choice(rng, depth_choices)):
            add_event(sess, uid, types[step], ts + timedelta(seconds=step * rng.randint(20, 150)), device, src,
                      rng.choice(all_product_ids) if step else None)
    _bulk_insert(engine, UserEvent, events)

    # ---- fraud alerts: run the rules engine over the stored orders ---------------
    alert_rows = score_orders(orders, cust_by_id, now, rng, INVESTIGATORS)
    _bulk_insert(engine, FraudAlert, alert_rows)

    counts = {"categories": len(cat_rows), "suppliers": len(supplier_rows), "products": len(products),
              "customers": len(cust_rows), "orders": len(orders), "order_items": len(items),
              "user_events": len(events), "fraud_alerts": len(alert_rows)}
    log("seeded: " + ", ".join(f"{k}={v}" for k, v in counts.items()))
    return counts
