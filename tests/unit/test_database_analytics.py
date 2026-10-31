"""
Tests for the database-backed analytics layer.

A small database is generated with the project's seeder, then every repository
function is checked against independent SQL totals or structural invariants - nothing
is compared with hard-coded expected numbers.
"""

from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from src.api.auth.security import create_access_token
from src.api.dependencies import get_database_session
from src.api.main import app
from src.api.repositories import analytics, customers, fraud, operations, realtime
from src.api.repositories.common import clear_cache, data_now
from src.database.demo_seed import seed_database
from src.database.fraud_scoring import ALERT_THRESHOLD, RULES, combine, priority_for, triggered_rules
from src.database.models import Customer, FraudAlert, Order, OrderItem, UserEvent


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    path = tmp_path_factory.mktemp("db") / "analytics.db"
    eng = create_engine(f"sqlite:///{path.as_posix()}")
    seed_database(eng, customers=400, days=150, seed=7, base_daily_orders=30, event_days=21, log=lambda *_: None)
    clear_cache()
    yield eng
    eng.dispose()


@pytest.fixture
def db(engine):
    with Session(engine) as session:
        yield session


# ----------------------------------------------------------------------------- seeding
class TestSeededData:
    def test_every_table_is_populated(self, db):
        for model in (Customer, Order, OrderItem, UserEvent, FraudAlert):
            assert db.execute(select(func.count()).select_from(model)).scalar() > 0

    def test_seed_is_deterministic(self, tmp_path):
        counts = []
        for name in ("a.db", "b.db"):
            eng = create_engine(f"sqlite:///{(tmp_path / name).as_posix()}")
            counts.append(seed_database(eng, customers=120, days=60, seed=99, event_days=10, log=lambda *_: None))
            eng.dispose()
        assert counts[0] == counts[1]

    def test_no_integrity_violations(self, db):
        quality = operations.data_quality(db)

        assert quality["rows_failed"] == 0, quality["checks"]
        assert quality["accuracy_score"] == 1.0

    def test_orders_total_matches_components(self, db):
        bad = db.execute(
            select(func.count()).where(
                func.abs(Order.total_amount - (Order.subtotal - Order.discount_amount + Order.tax_amount + Order.shipping_cost)) > 0.02
            )
        ).scalar()

        assert bad == 0


# ----------------------------------------------------------------------------- revenue
class TestRevenue:
    def test_summary_matches_independent_sql(self, db):
        end = data_now(db).date()
        start = end - timedelta(days=29)
        result = analytics.revenue_analytics(db, start.isoformat(), end.isoformat(), "daily", None, None)

        expected = db.execute(
            select(func.sum(Order.subtotal - Order.discount_amount), func.count()).where(
                Order.payment_status == "paid",
                Order.order_date >= datetime.combine(start, datetime.min.time()),
                Order.order_date < datetime.combine(end + timedelta(days=1), datetime.min.time()),
            )
        ).one()
        assert result["summary"]["total_revenue"] == pytest.approx(float(expected[0]), abs=0.01)
        assert result["summary"]["total_orders"] == expected[1]

    def test_time_series_adds_up_to_the_summary(self, db):
        end = data_now(db).date()
        result = analytics.revenue_analytics(db, (end - timedelta(days=44)).isoformat(), end.isoformat(), "weekly", None, None)

        assert sum(p["revenue"] for p in result["time_series"]) == pytest.approx(result["summary"]["total_revenue"], abs=0.05)
        assert sum(p["orders"] for p in result["time_series"]) == result["summary"]["total_orders"]

    def test_breakdowns_cover_the_whole_revenue(self, db):
        end = data_now(db).date()
        result = analytics.revenue_analytics(db, (end - timedelta(days=29)).isoformat(), end.isoformat(), "daily", None, None)

        for key in ("by_category", "by_region", "by_channel"):
            assert sum(row["percentage"] for row in result["breakdown"][key]) == pytest.approx(100, abs=0.5)

    def test_region_filter_reduces_revenue(self, db):
        end = data_now(db).date()
        args = ((end - timedelta(days=29)).isoformat(), end.isoformat(), "daily")
        everything = analytics.revenue_analytics(db, *args, None, None)["summary"]["total_revenue"]
        europe = analytics.revenue_analytics(db, *args, "Europe", None)["summary"]["total_revenue"]

        assert 0 < europe < everything

    def test_forecast_has_a_backtest(self, db):
        end = data_now(db).date()
        forecast = analytics.revenue_analytics(db, (end - timedelta(days=59)).isoformat(), end.isoformat(), "daily", None, None)["forecast"]

        assert forecast["confidence_interval"]["lower"] <= forecast["next_period_revenue"] <= forecast["confidence_interval"]["upper"]
        assert 0.0 <= forecast["backtest_accuracy"] <= 1.0


# ----------------------------------------------------------------------------- segments / cohorts / funnel
class TestCustomerAnalytics:
    def test_segments_partition_the_customers(self, db):
        result = analytics.customer_segments(db)

        assert sum(s["count"] for s in result["segments"]) == result["total_customers"]
        assert sum(s["characteristics"]["revenue_contribution"] for s in result["segments"]) == pytest.approx(1.0, abs=0.01)

    def test_rfm_scores_are_in_range(self, db):
        rows = analytics.customer_rfm(db)

        assert rows and all(1 <= r["r"] <= 5 and 1 <= r["f"] <= 5 and 1 <= r["m"] <= 5 for r in rows)

    def test_cohort_retention_is_a_fraction_and_starts_at_one(self, db):
        result = analytics.cohort_analysis(db)

        for cohort in result["cohorts"]:
            assert cohort["periods"][0]["value"] == 1.0
            assert all(0.0 <= p["value"] <= 1.0 for p in cohort["periods"])

    def test_funnel_never_grows(self, db):
        steps = analytics.funnel_analysis(db, "purchase", "30d")["steps"]

        assert [s["users"] for s in steps] == sorted((s["users"] for s in steps), reverse=True)
        assert steps[0]["conversion_rate"] == 1.0

    def test_executive_kpis_are_consistent(self, db):
        kpis = analytics.executive_overview(db, 60)["executive_kpis"]

        assert kpis["average_order_value"] == pytest.approx(kpis["total_revenue"] / kpis["total_orders"], abs=0.01)


class TestCustomers:
    def test_pagination(self, db):
        first = customers.list_customers(db, None, None, "lifetime_value", 1, 10)
        second = customers.list_customers(db, None, None, "lifetime_value", 2, 10)

        assert len(first["items"]) == 10
        assert first["items"][0]["analytics"]["lifetime_value"] >= first["items"][-1]["analytics"]["lifetime_value"]
        assert {c["id"] for c in first["items"]}.isdisjoint({c["id"] for c in second["items"]})

    def test_segment_filter(self, db):
        result = customers.list_customers(db, "Champions", None, "lifetime_value", 1, 50)

        assert all(c["analytics"]["rfm_segment"] == "Champions" for c in result["items"])

    def test_detail_and_unknown_customer(self, db):
        some = customers.list_customers(db, None, None, "lifetime_value", 1, 1)["items"][0]["id"]

        detail = customers.customer_detail(db, some)
        assert detail["id"] == some and len(detail["purchase_history"]) <= 10
        assert customers.customer_detail(db, "does-not-exist") is None

    def test_churn_risk_grows_when_overdue(self):
        assert customers.churn_risk(5, 30) == 0.0
        assert customers.churn_risk(200, 30) > customers.churn_risk(60, 30) > 0

    def test_recommendations_exclude_owned_products(self, db):
        some = customers.list_customers(db, None, None, "lifetime_value", 1, 1)["items"][0]["id"]
        owned = {r[0] for r in db.execute(select(OrderItem.product_id).join(Order).where(Order.user_id == some))}

        recs = customers.recommendations(db, some, 5)["recommendations"]

        assert recs and not owned & {r["product_id"] for r in recs}


# ----------------------------------------------------------------------------- fraud
class TestFraudRules:
    NOW = datetime(2026, 1, 15, 12, 0, 0)

    def _order(self, oid, minutes_ago, amount, status="paid", user="u1"):
        return {"order_id": oid, "user_id": user, "order_date": self.NOW - timedelta(minutes=minutes_ago),
                "total_amount": amount, "payment_status": status}

    def test_velocity_rule(self):
        orders = [self._order(f"o{i}", 10 - 3 * i, 100.0) for i in range(4)]

        hits = triggered_rules(orders, {"u1": self.NOW - timedelta(days=400)})

        assert "velocity" in hits["o3"]

    def test_new_account_high_value(self):
        hits = triggered_rules([self._order("o1", 5, 2000.0)], {"u1": self.NOW - timedelta(days=1)})

        assert "new_account_high_value" in hits["o1"]

    def test_ordinary_order_is_not_flagged(self):
        assert triggered_rules([self._order("o1", 5, 80.0)], {"u1": self.NOW - timedelta(days=200)}) == {}

    def test_failed_payments_then_success(self):
        orders = [self._order("f1", 30, 90.0, "failed"), self._order("f2", 20, 90.0, "failed"), self._order("ok", 5, 700.0)]

        assert "payment_failures" in triggered_rules(orders, {"u1": self.NOW - timedelta(days=300)})["ok"]

    def test_score_combination_and_priority(self):
        assert combine([]) == 0.0
        assert combine([0.5, 0.5]) == 0.75
        assert [priority_for(s) for s in (0.9, 0.75, 0.62, 0.5)] == ["critical", "high", "medium", "low"]


class TestFraudAnalytics:
    def test_alerts_match_the_rules_engine(self, db):
        rows = db.execute(select(Order.order_id, Order.user_id, Order.order_date, Order.total_amount, Order.payment_status)).mappings().all()
        registered = dict(db.execute(select(Customer.user_id, Customer.registration_date)).all())
        hits = triggered_rules([dict(r) for r in rows], registered)
        expected = {oid for oid, rules in hits.items() if combine([RULES[r][0] for r in rules]) >= ALERT_THRESHOLD}

        stored = {oid for (oid,) in db.execute(select(FraudAlert.order_id))}

        assert stored == expected

    def test_dashboard_counts_are_consistent(self, db):
        dash = fraud.fraud_dashboard(db, "90d")
        alerts = dash["alerts"]

        assert alerts["open"] + alerts["investigating"] + alerts["resolved"] + alerts["escalated"] + alerts["false_positives"] == alerts["total"]
        assert sum(alerts["by_priority"].values()) == alerts["total"]
        assert 0 <= dash["performance"]["auc_roc"] <= 1

    def test_rule_quality_is_a_proper_measurement(self, db):
        result = fraud.detection_models(db, "90d")

        for model in result["models"]:
            assert 0 <= model["precision"] <= 1 and 0 <= model["recall"] <= 1
        ensemble = result["ensemble_performance"]
        assert ensemble["orders_evaluated"] > 0
        assert ensemble["confirmed_fraud_orders"] <= db.execute(select(func.count()).where(Order.is_fraudulent.is_(True))).scalar()

    def test_listing_filters_and_paging(self, db):
        page = fraud.list_alerts(db, "open", None, None, None, None, None, None, 1, 5)

        assert all(a["status"] == "open" for a in page["alerts"])
        assert page["pagination"]["total"] == page["summary"]["total_alerts"]

    def test_cases_group_by_customer(self, db):
        result = fraud.list_cases(db, None, None, None, None, 1, 100)

        assert len({c["customer_id"] for c in result["cases"]}) == len(result["cases"])
        assert all(c["status"] in ("open", "in_progress", "escalated") for c in result["cases"])


# ----------------------------------------------------------------------------- real-time / operations
class TestRealtimeAndOperations:
    def test_realtime_metrics_shape(self, db):
        result = analytics.realtime_metrics(db, "4h")

        assert {"sales", "traffic", "inventory"} <= set(result)
        assert 0.0 <= result["traffic"]["bounce_rate"] <= 1.0

    def test_business_metrics_baseline(self, db):
        result = realtime.business_metrics(db, "all", "24h")

        assert result["sales"]["revenue"]["typical_for_this_time"] >= 0
        assert result["marketing"]["available"] is False  # no source: reported, not invented

    def test_activity_feed(self, db):
        result = realtime.activity(db, 24, 5)

        assert len(result["recent_transactions"]) == 5 and len(result["hourly"]) == 25

    def test_unconfigured_services_are_reported_unavailable(self, db):
        health = realtime.system_health(db)

        for name in ("kafka", "spark"):
            assert health["core_metrics"][name]["status"] in ("not_configured", "healthy")
        assert health["core_metrics"]["database"]["status"] == "healthy"

    def test_database_health_is_measured(self, db):
        health = operations.database_health(db, pings=3)

        assert health["engine"] == "sqlite" and health["latency_p95_ms"] >= 0 and health["size_mb"] > 0

    def test_host_metrics_are_real_ranges(self):
        host = operations.host_metrics()

        assert 0 <= host["cpu_percent"] <= 100 and 0 <= host["memory_percent"] <= 100


# ----------------------------------------------------------------------------- HTTP layer
class TestEndpoints:
    @pytest.fixture
    def client(self, engine):
        def session():
            with Session(engine) as s:
                yield s

        app.dependency_overrides[get_database_session] = session
        yield TestClient(app)
        app.dependency_overrides.pop(get_database_session, None)

    @pytest.fixture
    def headers(self):
        token = create_access_token(data={"sub": "admin", "user_id": 1, "role": "admin", "permissions": []})
        return {"Authorization": f"Bearer {token}"}

    @pytest.mark.parametrize(
        "path",
        [
            "analytics/revenue", "analytics/customers/segments", "analytics/products/performance", "analytics/real-time/metrics",
            "analytics/cohort-analysis", "analytics/funnels", "analytics/executive", "customers/", "fraud/alerts",
            "fraud/dashboard", "fraud/models", "fraud/cases", "fraud/risk-assessment", "realtime/system-health",
            "realtime/stream-metrics", "realtime/business-metrics", "realtime/performance-benchmarks", "realtime/activity",
            "operations/fraud-monitoring", "operations/data-quality", "operations/alerts", "operations/performance",
            "operations/streaming-health",
        ],
    )
    def test_endpoint_responds_with_json(self, client, headers, path):
        response = client.get(f"/api/v1/{path}", headers=headers)

        assert response.status_code == 200, response.text[:200]
        assert isinstance(response.json(), dict)

    def test_unknown_customer_is_404(self, client, headers):
        assert client.get("/api/v1/customers/nobody", headers=headers).status_code == 404

    def test_invalid_granularity_is_rejected(self, client, headers):
        assert client.get("/api/v1/analytics/revenue?granularity=hourly", headers=headers).status_code in (400, 422)

    def test_requires_authentication(self, client):
        assert client.get("/api/v1/fraud/alerts").status_code in (401, 403)
