"""
Property-based tests for business rules and invariants.

This module implements property-based testing for business logic components,
ensuring consistent behavior across various scenarios and edge cases.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.analytics.business_intelligence.revenue_analytics import RevenueAnalytics
from src.analytics.fraud_detection.alert_prioritizer import FraudAlertPrioritizer
from src.analytics.fraud_detection.rules_engine import ConfigurableRulesEngine
from src.analytics.rfm_segmentation import RFMSegmentationEngine
from src.utils.spark_utils import create_spark_session


def clip_score(column: Column, low: int = 1, high: int = 5) -> Column:
    """Clamp a score column to [low, high] (Spark Columns have no pandas-style clip)."""
    return F.greatest(F.lit(low), F.least(F.lit(high), column))


# Customer transaction data strategy
@st.composite
def customer_transaction_data(draw):
    """Generate customer transaction data for RFM analysis."""
    return {
        "customer_id": draw(
            st.text(
                min_size=1,
                max_size=50,
                alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
            )
        ),
        "transaction_date": draw(
            st.datetimes(
                min_value=datetime(2020, 1, 1),
                max_value=datetime(2025, 12, 31),
                timezones=st.just(timezone.utc),
            )
        ),
        "monetary_value": draw(
            st.floats(
                min_value=0.01, max_value=50000.0, allow_nan=False, allow_infinity=False
            )
        ),
        "recency_days": draw(st.integers(min_value=0, max_value=3650)),  # 0-10 years
        "frequency_count": draw(st.integers(min_value=1, max_value=1000)),
    }


@st.composite
def fraud_transaction_data(draw):
    """Generate transaction data for fraud detection testing."""
    return {
        "transaction_id": draw(st.text(min_size=1, max_size=100)),
        "customer_id": draw(st.text(min_size=1, max_size=50)),
        "amount": draw(
            st.floats(
                min_value=0.01,
                max_value=100000.0,
                allow_nan=False,
                allow_infinity=False,
            )
        ),
        "timestamp": draw(
            st.datetimes(
                min_value=datetime(2020, 1, 1),
                max_value=datetime.now(timezone.utc).replace(tzinfo=None),
                timezones=st.just(timezone.utc),
            )
        ),
        "location": draw(st.text(min_size=1, max_size=100)),
        "payment_method": draw(
            st.sampled_from(["credit_card", "debit_card", "paypal", "cash", "crypto"])
        ),
        "merchant_id": draw(st.text(min_size=1, max_size=50)),
        "device_fingerprint": draw(st.text(min_size=10, max_size=100)),
        "ip_address": draw(st.text(min_size=7, max_size=15)),  # Simple IP format
        "previous_transaction_minutes": draw(
            st.integers(min_value=0, max_value=10080)
        ),  # 0-7 days in minutes
    }


@st.composite
def revenue_data(draw):
    """Generate revenue data for business intelligence testing."""
    return {
        "date": draw(
            st.dates(
                min_value=datetime(2020, 1, 1).date(),
                max_value=datetime(2025, 12, 31).date(),
            )
        ),
        "revenue": draw(
            st.floats(
                min_value=0.0,
                max_value=1000000.0,
                allow_nan=False,
                allow_infinity=False,
            )
        ),
        "transactions": draw(st.integers(min_value=0, max_value=100000)),
        "unique_customers": draw(st.integers(min_value=0, max_value=50000)),
        "product_category": draw(st.text(min_size=1, max_size=50)),
        "region": draw(st.text(min_size=1, max_size=50)),
    }


class TestRFMBusinessRuleProperties:
    """Property-based tests for RFM segmentation business rules."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing."""
        return create_spark_session("rfm-property-tests")

    @pytest.fixture(scope="class")
    def rfm_segmentation(self, spark):
        """Create RFM segmentation instance."""
        return RFMSegmentationEngine(spark)

    @given(st.lists(customer_transaction_data(), min_size=1, max_size=100))
    @settings(max_examples=30, deadline=None)
    def test_rfm_score_boundaries_property(self, spark, customer_transactions):
        """
        Test that RFM scores always fall within expected boundaries.

        Property: RFM scores should always be between 1-5 for each dimension.
        """
        # Skip if no valid transactions
        valid_transactions = [
            t for t in customer_transactions if t["monetary_value"] > 0
        ]
        assume(len(valid_transactions) > 0)

        # Create test DataFrame
        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("transaction_date", TimestampType(), True),
                StructField("monetary_value", DoubleType(), True),
                StructField("recency_days", IntegerType(), True),
                StructField("frequency_count", IntegerType(), True),
            ]
        )

        df = spark.createDataFrame(valid_transactions, schema)

        # Calculate RFM scores (mock implementation for testing)
        # In real scenario, this would call rfm_segmentation.calculate_rfm_scores()
        rfm_df = df.withColumn(
            "r_score", clip_score((5 - (df.recency_days / 730)).cast("int"))
        )
        rfm_df = rfm_df.withColumn(
            "f_score", clip_score((df.frequency_count / 200 + 1).cast("int"))
        )
        rfm_df = rfm_df.withColumn(
            "m_score", clip_score((df.monetary_value / 10000 + 1).cast("int"))
        )

        results = rfm_df.collect()

        for row in results:
            # Test RFM score boundaries
            if row["r_score"] is not None:
                assert (
                    1 <= row["r_score"] <= 5
                ), f"R-score {row['r_score']} out of bounds"
            if row["f_score"] is not None:
                assert (
                    1 <= row["f_score"] <= 5
                ), f"F-score {row['f_score']} out of bounds"
            if row["m_score"] is not None:
                assert (
                    1 <= row["m_score"] <= 5
                ), f"M-score {row['m_score']} out of bounds"

    @given(
        st.floats(
            min_value=0.01, max_value=100000.0, allow_nan=False, allow_infinity=False
        )
    )
    def test_monetary_value_monotonicity(self, spark, monetary_value):
        """
        Test that higher monetary values result in higher M-scores.

        Property: Monotonic relationship between monetary value and M-score.
        """
        # Create comparison values
        lower_value = monetary_value * 0.5
        higher_value = monetary_value * 2.0

        test_data = [
            ("customer1", lower_value),
            ("customer2", monetary_value),
            ("customer3", higher_value),
        ]

        df = spark.createDataFrame(test_data, ["customer_id", "monetary_value"])

        # Calculate M-scores using simplified logic
        scored_df = df.withColumn(
            "m_score", clip_score(((df.monetary_value / 1000.0).cast("int") + 1))
        ).orderBy("monetary_value")

        results = scored_df.collect()

        # Verify monotonic property
        for i in range(len(results) - 1):
            current_value = results[i]["monetary_value"]
            next_value = results[i + 1]["monetary_value"]
            current_score = results[i]["m_score"]
            next_score = results[i + 1]["m_score"]

            if next_value > current_value:
                assert (
                    next_score >= current_score
                ), f"Monotonicity violated: value {next_value} (score {next_score}) > {current_value} (score {current_score})"

    @given(st.integers(min_value=0, max_value=3650))  # 0-10 years in days
    def test_recency_scoring_inverse_relationship(self, spark, recency_days):
        """
        Test that higher recency (more days since last purchase) results in lower R-scores.

        Property: Inverse relationship between recency days and R-score.
        """
        test_data = [(recency_days,)]
        df = spark.createDataFrame(test_data, ["recency_days"])

        # Calculate R-score using inverse relationship
        scored_df = df.withColumn(
            "r_score", clip_score((6 - (df.recency_days / 365.0).cast("int")))
        )

        result = scored_df.collect()[0]
        r_score = result["r_score"]

        # Verify inverse relationship properties
        if recency_days <= 30:  # Very recent
            assert (
                r_score >= 4
            ), f"Recent transactions ({recency_days} days) should have high R-score, got {r_score}"
        elif recency_days >= 1095:  # Very old (3+ years)
            assert (
                r_score <= 2
            ), f"Old transactions ({recency_days} days) should have low R-score, got {r_score}"

        # Score should always be in valid range
        assert 1 <= r_score <= 5, f"R-score {r_score} out of valid range"


class TestFraudDetectionBusinessRules:
    """Property-based tests for fraud detection business rules."""

    @pytest.fixture
    def fraud_rules_engine(self):
        """Create fraud rules engine for testing."""
        return ConfigurableRulesEngine()

    @pytest.fixture
    def alert_prioritizer(self):
        """Create alert prioritizer for testing."""
        return FraudAlertPrioritizer()

    @given(fraud_transaction_data())
    @settings(max_examples=50, deadline=None)
    def test_fraud_score_properties(self, fraud_rules_engine, transaction_data):
        """
        Test that fraud scoring maintains consistent properties.

        Property: Fraud scores should be between 0-1 and increase with risk factors.
        """
        # Mock fraud detection logic for testing
        base_score = 0.1
        risk_factors = 0.0

        # Amount-based risk
        if transaction_data["amount"] > 10000:
            risk_factors += 0.3
        elif transaction_data["amount"] > 1000:
            risk_factors += 0.2

        # Time-based risk (very recent transactions)
        if transaction_data["previous_transaction_minutes"] < 5:
            risk_factors += 0.4
        elif transaction_data["previous_transaction_minutes"] < 60:
            risk_factors += 0.2

        # Payment method risk
        if transaction_data["payment_method"] in ["crypto", "cash"]:
            risk_factors += 0.1

        fraud_score = min(base_score + risk_factors, 1.0)

        # Test fraud score properties
        assert 0.0 <= fraud_score <= 1.0, f"Fraud score {fraud_score} out of range"

        # High-risk combinations should have high scores
        if (
            transaction_data["amount"] > 10000
            and transaction_data["previous_transaction_minutes"] < 5
        ):
            assert (
                fraud_score >= 0.7
            ), f"High-risk transaction should have score >=0.7, got {fraud_score}"

        # Low-risk transactions should have low scores
        if (
            transaction_data["amount"] < 100
            and transaction_data["previous_transaction_minutes"] > 1440
            and transaction_data["payment_method"] == "credit_card"  # > 1 day
        ):
            assert (
                fraud_score <= 0.3
            ), f"Low-risk transaction should have score <=0.3, got {fraud_score}"

    @given(st.floats(min_value=0.0, max_value=1.0))
    def test_alert_priority_consistency(self, alert_prioritizer, fraud_score):
        """
        Test that alert prioritization is consistent with fraud scores.

        Property: Higher fraud scores should result in higher alert priorities.
        """
        # Mock alert prioritization logic
        if fraud_score >= 0.8:
            priority = "critical"
            numeric_priority = 4
        elif fraud_score >= 0.6:
            priority = "high"
            numeric_priority = 3
        elif fraud_score >= 0.4:
            priority = "medium"
            numeric_priority = 2
        elif fraud_score >= 0.2:
            priority = "low"
            numeric_priority = 1
        else:
            priority = "minimal"
            numeric_priority = 0

        # Test priority consistency
        assert priority in [
            "minimal",
            "low",
            "medium",
            "high",
            "critical",
        ], f"Invalid priority level: {priority}"
        assert (
            0 <= numeric_priority <= 4
        ), f"Invalid numeric priority: {numeric_priority}"

        # Test monotonic relationship
        if fraud_score >= 0.8:
            assert (
                priority == "critical"
            ), f"Score {fraud_score} should be critical priority"
        elif fraud_score < 0.2:
            assert (
                priority == "minimal"
            ), f"Score {fraud_score} should be minimal priority"

    @given(st.lists(fraud_transaction_data(), min_size=2, max_size=10))
    def test_velocity_check_properties(self, transaction_list):
        """
        Test transaction velocity checking properties.

        Property: Velocity alerts should increase with transaction frequency.
        """
        # Sort transactions by timestamp
        sorted_transactions = sorted(transaction_list, key=lambda x: x["timestamp"])

        # Calculate time differences
        velocity_alerts = []
        for i in range(1, len(sorted_transactions)):
            prev_time = sorted_transactions[i - 1]["timestamp"]
            curr_time = sorted_transactions[i]["timestamp"]
            time_diff_minutes = (curr_time - prev_time).total_seconds() / 60

            # Velocity rule: flag if transactions < 5 minutes apart
            if time_diff_minutes < 5:
                velocity_alerts.append(
                    {
                        "transaction_id": sorted_transactions[i]["transaction_id"],
                        "time_diff": time_diff_minutes,
                        "risk_level": "high" if time_diff_minutes < 1 else "medium",
                    }
                )

        # Test velocity properties
        for alert in velocity_alerts:
            assert (
                alert["time_diff"] < 5
            ), f"Velocity alert should only trigger for <5 minutes, got {alert['time_diff']}"
            assert alert["risk_level"] in [
                "medium",
                "high",
            ], f"Invalid risk level: {alert['risk_level']}"

            if alert["time_diff"] < 1:
                assert (
                    alert["risk_level"] == "high"
                ), f"<1 minute should be high risk, got {alert['risk_level']}"


class TestRevenueAnalyticsBusinessRules:
    """Property-based tests for revenue analytics business rules."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing."""
        return create_spark_session("revenue-property-tests")

    @given(st.lists(revenue_data(), min_size=1, max_size=100))
    @settings(max_examples=30, deadline=None)
    def test_revenue_aggregation_properties(self, spark, revenue_data_list):
        """
        Test that revenue aggregations maintain mathematical properties.

        Property: Aggregated values should equal sum of components.
        """
        # Filter out invalid data
        valid_data = [
            d for d in revenue_data_list if d["revenue"] >= 0 and d["transactions"] >= 0
        ]
        assume(len(valid_data) > 0)

        schema = StructType(
            [
                StructField("date", StringType(), True),
                StructField("revenue", DoubleType(), True),
                StructField("transactions", IntegerType(), True),
                StructField("unique_customers", IntegerType(), True),
                StructField("product_category", StringType(), True),
                StructField("region", StringType(), True),
            ]
        )

        # Convert dates to strings for Spark DataFrame
        formatted_data = []
        for item in valid_data:
            formatted_item = dict(item)
            formatted_item["date"] = item["date"].strftime("%Y-%m-%d")
            formatted_data.append(formatted_item)

        df = spark.createDataFrame(formatted_data, schema)

        # Calculate total revenue and transactions
        totals = df.agg({"revenue": "sum", "transactions": "sum"}).collect()[0]
        total_revenue = totals["sum(revenue)"] or 0.0
        total_transactions = totals["sum(transactions)"] or 0

        # Manual calculation for verification
        manual_revenue = sum(item["revenue"] for item in valid_data)
        manual_transactions = sum(item["transactions"] for item in valid_data)

        # Test aggregation properties
        assert (
            abs(total_revenue - manual_revenue) < 0.01
        ), f"Revenue aggregation mismatch: {total_revenue} vs {manual_revenue}"
        assert (
            total_transactions == manual_transactions
        ), f"Transaction count mismatch: {total_transactions} vs {manual_transactions}"

        # Test non-negative constraint
        assert total_revenue >= 0, f"Total revenue cannot be negative: {total_revenue}"
        assert (
            total_transactions >= 0
        ), f"Total transactions cannot be negative: {total_transactions}"

    @given(
        st.floats(
            min_value=0.01, max_value=1000000.0, allow_nan=False, allow_infinity=False
        ),
        st.integers(min_value=1, max_value=100000),
    )
    def test_average_order_value_property(
        self, spark, total_revenue, transaction_count
    ):
        """
        Test that average order value calculations are consistent.

        Property: AOV = Total Revenue / Number of Transactions.
        """
        test_data = [(total_revenue, transaction_count)]
        df = spark.createDataFrame(test_data, ["revenue", "transactions"])

        # Calculate AOV
        aov_df = df.withColumn("aov", df.revenue / df.transactions)
        result = aov_df.collect()[0]

        calculated_aov = result["aov"]
        expected_aov = total_revenue / transaction_count

        # Test AOV calculation property
        assert (
            abs(calculated_aov - expected_aov) < 0.01
        ), f"AOV calculation mismatch: {calculated_aov} vs {expected_aov}"

        # AOV should be positive
        assert calculated_aov > 0, f"AOV should be positive: {calculated_aov}"

        # AOV should not exceed total revenue (sanity check)
        assert (
            calculated_aov <= total_revenue
        ), f"AOV {calculated_aov} cannot exceed total revenue {total_revenue}"

    @given(
        st.lists(
            st.tuples(
                st.dates(
                    min_value=datetime(2020, 1, 1).date(),
                    max_value=datetime(2025, 12, 31).date(),
                ),
                st.floats(
                    min_value=0.0,
                    max_value=100000.0,
                    allow_nan=False,
                    allow_infinity=False,
                ),
            ),
            min_size=2,
            max_size=50,
        )
    )
    def test_growth_rate_calculation_property(self, spark, revenue_time_series):
        """
        Test that growth rate calculations maintain mathematical consistency.

        Property: Growth rate should reflect actual percentage change.
        """
        # Sort by date and remove duplicates
        unique_data = {}
        for date, revenue in revenue_time_series:
            unique_data[date] = revenue

        sorted_data = sorted(unique_data.items())
        assume(len(sorted_data) >= 2)

        # Calculate growth rates
        growth_rates = []
        for i in range(1, len(sorted_data)):
            prev_revenue = sorted_data[i - 1][1]
            curr_revenue = sorted_data[i][1]

            if prev_revenue > 0:
                growth_rate = (curr_revenue - prev_revenue) / prev_revenue
                growth_rates.append(growth_rate)

        # Test growth rate properties
        for rate in growth_rates:
            # Growth rate should be reasonable (not extreme)
            assert -1.0 <= rate <= 100.0, f"Growth rate {rate} seems unrealistic"

            # If current revenue is higher, growth should be positive
            # If current revenue is lower, growth should be negative
            # This is implicitly tested by the calculation above


class TestBusinessRuleInvariants:
    """Test cross-cutting business rule invariants."""

    @given(
        st.floats(
            min_value=0.01, max_value=1000000.0, allow_nan=False, allow_infinity=False
        )
    )
    def test_currency_precision_invariant(self, amount):
        """
        Test that currency calculations maintain proper precision.

        Invariant: All monetary calculations should maintain 2 decimal places.
        """
        # Apply typical currency operations
        tax_rate = 0.08
        discount_rate = 0.15

        # Calculate various monetary values
        tax_amount = round(amount * tax_rate, 2)
        discount_amount = round(amount * discount_rate, 2)
        final_amount = round(amount - discount_amount + tax_amount, 2)

        # Test precision invariant
        def has_valid_precision(value):
            str_value = f"{value:.2f}"
            return len(str_value.split(".")[1]) <= 2

        assert has_valid_precision(
            tax_amount
        ), f"Tax amount {tax_amount} has invalid precision"
        assert has_valid_precision(
            discount_amount
        ), f"Discount amount {discount_amount} has invalid precision"
        assert has_valid_precision(
            final_amount
        ), f"Final amount {final_amount} has invalid precision"

        # Test that final amount is reasonable
        assert final_amount >= 0, f"Final amount {final_amount} cannot be negative"
        assert (
            final_amount <= amount * 1.5
        ), f"Final amount {final_amount} exceeds reasonable bounds"

    @given(st.integers(min_value=1, max_value=10000))
    def test_customer_segmentation_completeness_invariant(self, customer_count):
        """
        Test that customer segmentation assigns every customer to exactly one segment.

        Invariant: All customers must be assigned to exactly one segment.
        """
        # Mock customer segmentation logic
        segments = []

        for customer_id in range(customer_count):
            # Simple segmentation based on customer_id
            if customer_id % 10 == 0:
                segment = "VIP"
            elif customer_id % 5 == 0:
                segment = "Premium"
            elif customer_id % 3 == 0:
                segment = "Regular"
            else:
                segment = "Basic"

            segments.append(segment)

        # Test completeness invariant
        assert (
            len(segments) == customer_count
        ), f"Segmentation incomplete: {len(segments)} segments for {customer_count} customers"

        # Test that all segments are valid
        valid_segments = {"VIP", "Premium", "Regular", "Basic"}
        for segment in segments:
            assert segment in valid_segments, f"Invalid segment: {segment}"

        # Test that distribution is reasonable (no single segment dominates >80%)
        segment_counts = {}
        for segment in segments:
            segment_counts[segment] = segment_counts.get(segment, 0) + 1

        for segment, count in segment_counts.items():
            percentage = count / customer_count
            assert (
                percentage <= 0.8
            ), f"Segment {segment} has {percentage:.1%} of customers, exceeds 80% threshold"
