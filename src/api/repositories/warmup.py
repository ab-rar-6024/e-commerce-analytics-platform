"""Pre-compute the heavy dashboard queries so the first page load is not cold."""

import logging
import time

from . import analytics, customers, fraud, operations, realtime

logger = logging.getLogger(__name__)


def warm_up(db) -> float:
    """Run the main dashboard queries once (results land in the in-process cache). Returns seconds taken."""
    started = time.time()
    steps = [
        ("revenue", lambda: analytics.revenue_analytics(db, None, None, "daily", None, None)),
        ("executive", lambda: analytics.executive_overview(db, 90)),
        ("segments", lambda: analytics.customer_segments(db)),
        ("cohorts", lambda: analytics.cohort_analysis(db)),
        ("funnel", lambda: analytics.funnel_analysis(db, "purchase", "30d")),
        ("products", lambda: analytics.product_performance(db, None, "revenue", "30d")),
        ("customers", lambda: customers.list_customers(db, None, None, "lifetime_value", 1, 100)),
        ("fraud", lambda: (fraud.fraud_dashboard(db, "24h"), fraud.detection_models(db, "90d"))),
        ("data quality", lambda: operations.data_quality(db)),
        ("real-time", lambda: (analytics.realtime_metrics(db, "1h"), realtime.business_metrics(db, "24h"), realtime.activity(db, 24, 10))),
    ]
    for name, step in steps:
        try:
            step()
        except Exception as exc:  # warming is best-effort; never block startup
            logger.warning("Cache warm-up step '%s' failed: %s", name, exc)
    return time.time() - started
