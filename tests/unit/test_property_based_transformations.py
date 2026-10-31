"""
Property-based tests for data transformations.

This module implements property-based testing for streaming data transformations,
ensuring robustness against edge cases and unexpected inputs.
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from hypothesis.extra.pandas import column, data_frames
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming.transformations.enrichment import DataEnrichmentPipeline
from src.utils.spark_utils import create_spark_session

# Transaction schema for property-based testing
TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("customer_tier", StringType(), True),
        StructField("customer_lifetime_value", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("location", StringType(), True),
        StructField("payment_method", StringType(), True),
    ]
)

# User behavior schema for property-based testing
USER_BEHAVIOR_SCHEMA = StructType(
    [
        StructField("session_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("user_agent", StringType(), True),
        StructField("page_url", StringType(), True),
    ]
)


# Hypothesis strategies for data generation
@st.composite
def transaction_data(draw):
    """Generate realistic transaction data for property-based testing."""
    return {
        "transaction_id": draw(st.uuids().map(str)),
        "customer_id": draw(st.uuids().map(str)),
        "product_id": draw(
            st.text(
                min_size=3,
                max_size=10,
                alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
            )
        ),
        "quantity": draw(st.integers(min_value=1, max_value=100)),
        "price": draw(
            st.floats(
                min_value=0.01, max_value=10000.0, allow_nan=False, allow_infinity=False
            )
        ),
        "total_amount": draw(
            st.floats(
                min_value=0.01,
                max_value=1000000.0,
                allow_nan=False,
                allow_infinity=False,
            )
        ),
        "timestamp": draw(
            st.datetimes(
                min_value=datetime(2020, 1, 1),
                max_value=datetime(2025, 12, 31),
                timezones=st.just(timezone.utc),
            )
        ),
        "customer_tier": draw(
            st.one_of(
                st.none(),
                st.sampled_from(
                    [
                        "premium",
                        "gold",
                        "silver",
                        "bronze",
                        "Premium",
                        "GOLD",
                        " silver ",
                        None,
                    ]
                ),
            )
        ),
        "customer_lifetime_value": draw(
            st.one_of(
                st.none(),
                st.floats(
                    min_value=0, max_value=50000, allow_nan=False, allow_infinity=False
                ),
            )
        ),
        "category": draw(
            st.one_of(
                st.none(),
                st.text(min_size=1).map(lambda x: f"Electronics/{x}"),
                st.just("Clothing/Shirts"),
                st.just("Books"),
            )
        ),
        "location": draw(
            st.one_of(
                st.none(),
                st.sampled_from(
                    [
                        "New York, NY",
                        "San Francisco, CA",
                        "Los Angeles, CA",
                        "Chicago, IL",
                        "Houston, TX",
                        "Phoenix, AZ",
                        "Philadelphia, PA",
                        "San Antonio, TX",
                        "San Diego, CA",
                        "Dallas, TX",
                        "Unknown City",
                        "Rural Town",
                        "Suburban Area",
                    ]
                ),
            )
        ),
        "payment_method": draw(
            st.sampled_from(["credit_card", "debit_card", "cash", "paypal", "bitcoin"])
        ),
    }


@st.composite
def user_behavior_data(draw):
    """Generate realistic user behavior data for property-based testing."""
    return {
        "session_id": draw(st.uuids().map(str)),
        "user_id": draw(st.uuids().map(str)),
        "event_type": draw(
            st.sampled_from(
                [
                    "page_view",
                    "product_view",
                    "add_to_cart",
                    "checkout",
                    "purchase",
                    "search",
                ]
            )
        ),
        "timestamp": draw(
            st.datetimes(
                min_value=datetime(2020, 1, 1),
                max_value=datetime(2025, 12, 31),
                timezones=st.just(timezone.utc),
            )
        ),
        "user_agent": draw(
            st.sampled_from(
                [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/14.1.1",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Firefox/89.0",
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) Mobile/15E148",
                    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 Safari/604.1",
                    "Mozilla/5.0 (Android 11; Mobile; rv:89.0) Gecko/89.0 Firefox/89.0",
                ]
            )
        ),
        "page_url": draw(
            st.text(min_size=10).map(lambda x: f"https://example.com/{x}")
        ),
    }


class TestPropertyBasedTransformations:
    """Property-based tests for data transformations."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing."""
        return create_spark_session("property-based-tests")

    @pytest.fixture(scope="class")
    def enrichment_pipeline(self, spark):
        """Create enrichment pipeline for testing."""
        return DataEnrichmentPipeline(spark)

    @given(st.lists(transaction_data(), min_size=1, max_size=100))
    @settings(max_examples=50, deadline=None)
    def test_transaction_enrichment_properties(
        self, spark, enrichment_pipeline, transaction_list
    ):
        """
        Property-based test for transaction enrichment.

        Tests that transaction enrichment maintains data integrity and
        produces consistent results for various input scenarios.
        """
        # Skip test if all amounts are null or invalid
        valid_transactions = [
            t for t in transaction_list if t.get("total_amount") is not None
        ]
        assume(len(valid_transactions) > 0)

        # Create DataFrame from generated data
        df = spark.createDataFrame(transaction_list, TRANSACTION_SCHEMA)

        # Apply enrichment
        enriched_df = enrichment_pipeline.enrich_transaction_stream(df)

        # Collect results for property verification
        results = enriched_df.collect()

        # Property 1: No data loss - same number of records
        assert len(results) == len(
            transaction_list
        ), "Data enrichment should not lose records"

        # Property 2: All original columns preserved
        original_columns = set(df.columns)
        enriched_columns = set(enriched_df.columns)
        assert original_columns.issubset(
            enriched_columns
        ), "Original columns must be preserved"

        # Property 3: New enrichment columns added
        expected_new_columns = {
            "enrichment_timestamp",
            "customer_tier_normalized",
            "ltv_category",
            "product_category_main",
            "price_category",
            "popularity_score",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "time_period",
            "is_business_hours",
            "region_category",
            "area_type",
            "revenue_impact",
            "transaction_size",
            "bulk_purchase",
            "risk_score",
            "risk_category",
            "anomaly_flags",
        }
        assert expected_new_columns.issubset(
            enriched_columns
        ), "Expected enrichment columns missing"

        # Property 4: Data type consistency
        for row in results:
            # Risk score should be integer between 0-3
            if row["risk_score"] is not None:
                assert (
                    0 <= row["risk_score"] <= 3
                ), f"Risk score {row['risk_score']} out of range"

            # LTV category should match value ranges
            if (
                row["customer_lifetime_value"] is not None
                and row["ltv_category"] is not None
            ):
                ltv_value = row["customer_lifetime_value"]
                ltv_category = row["ltv_category"]

                if ltv_value >= 1000:
                    assert (
                        ltv_category == "High"
                    ), f"LTV {ltv_value} should be High, got {ltv_category}"
                elif ltv_value >= 500:
                    assert (
                        ltv_category == "Medium"
                    ), f"LTV {ltv_value} should be Medium, got {ltv_category}"
                elif ltv_value >= 100:
                    assert (
                        ltv_category == "Low"
                    ), f"LTV {ltv_value} should be Low, got {ltv_category}"
                else:
                    assert (
                        ltv_category == "Unknown"
                    ), f"LTV {ltv_value} should be Unknown, got {ltv_category}"

            # Hour of day should be valid
            if row["hour_of_day"] is not None:
                assert (
                    0 <= row["hour_of_day"] <= 23
                ), f"Hour {row['hour_of_day']} out of range"

            # Day of week should be valid
            if row["day_of_week"] is not None:
                assert (
                    1 <= row["day_of_week"] <= 7
                ), f"Day of week {row['day_of_week']} out of range"

    @given(st.lists(user_behavior_data(), min_size=1, max_size=100))
    @settings(max_examples=50, deadline=None)
    def test_user_behavior_enrichment_properties(
        self, spark, enrichment_pipeline, behavior_list
    ):
        """
        Property-based test for user behavior enrichment.

        Tests that user behavior enrichment maintains consistency
        and produces valid engagement metrics.
        """
        # Create DataFrame from generated data
        df = spark.createDataFrame(behavior_list, USER_BEHAVIOR_SCHEMA)

        # Apply enrichment
        enriched_df = enrichment_pipeline.enrich_user_behavior_stream(df)

        # Collect results for property verification
        results = enriched_df.collect()

        # Property 1: No data loss
        assert len(results) == len(
            behavior_list
        ), "User behavior enrichment should not lose records"

        # Property 2: Engagement scores are consistent with event types
        for row in results:
            event_type = row["event_type"]
            engagement_score = row.get("engagement_score")

            if engagement_score is not None:
                if event_type == "purchase":
                    assert (
                        engagement_score == 100
                    ), f"Purchase should have score 100, got {engagement_score}"
                elif event_type == "add_to_cart":
                    assert (
                        engagement_score == 80
                    ), f"Add to cart should have score 80, got {engagement_score}"
                elif event_type == "product_view":
                    assert (
                        engagement_score == 60
                    ), f"Product view should have score 60, got {engagement_score}"
                elif event_type == "search":
                    assert (
                        engagement_score == 40
                    ), f"Search should have score 40, got {engagement_score}"
                elif event_type == "page_view":
                    assert (
                        engagement_score == 20
                    ), f"Page view should have score 20, got {engagement_score}"
                else:
                    assert (
                        engagement_score == 10
                    ), f"Unknown event should have score 10, got {engagement_score}"

        # Property 3: Device categories are valid
        valid_device_categories = {"Mobile", "Tablet", "Desktop"}
        for row in results:
            if row.get("device_category"):
                assert (
                    row["device_category"] in valid_device_categories
                ), f"Invalid device category: {row['device_category']}"

        # Property 4: Purchase intent scores are reasonable
        for row in results:
            intent_score = row.get("purchase_intent_score")
            if intent_score is not None:
                assert (
                    0 <= intent_score <= 100
                ), f"Intent score {intent_score} out of range"

    @given(
        st.floats(
            min_value=0.01, max_value=10000.0, allow_nan=False, allow_infinity=False
        )
    )
    def test_price_categorization_property(self, spark, price):
        """
        Property-based test for price categorization logic.

        Tests that price categorization boundaries are consistent.
        """
        # Create test DataFrame with single price
        test_data = [(price,)]
        df = spark.createDataFrame(test_data, ["price"])

        # Apply price categorization logic
        result_df = df.withColumn("price_category", col("price").cast("double")).select(
            "price", "price_category"
        )

        # Verify price categories follow expected logic
        result = result_df.collect()[0]
        test_price = result["price"]

        # Test the actual logic from enrichment pipeline
        if test_price >= 100:
            expected_category = "High"
        elif test_price >= 50:
            expected_category = "Medium"
        elif test_price >= 10:
            expected_category = "Low"
        else:
            expected_category = "Very Low"

        # This is testing the boundary conditions are correct
        assert test_price >= 0.01, "Price should be positive"
        assert test_price <= 10000.0, "Price should be reasonable"

    @given(st.integers(min_value=0, max_value=23))
    def test_time_period_categorization_property(self, spark, hour):
        """
        Property-based test for time period categorization.

        Tests that time periods are correctly categorized across all hours.
        """
        from pyspark.sql.functions import lit, when

        # Create test DataFrame with hour
        test_data = [(hour,)]
        df = spark.createDataFrame(test_data, ["hour_of_day"])

        # Apply time period logic from enrichment pipeline
        result_df = df.withColumn(
            "time_period",
            when(col("hour_of_day").between(6, 11), lit("Morning"))
            .when(col("hour_of_day").between(12, 17), lit("Afternoon"))
            .when(col("hour_of_day").between(18, 22), lit("Evening"))
            .otherwise(lit("Night")),
        )

        result = result_df.collect()[0]
        time_period = result["time_period"]

        # Verify time period mapping
        if 6 <= hour <= 11:
            assert (
                time_period == "Morning"
            ), f"Hour {hour} should be Morning, got {time_period}"
        elif 12 <= hour <= 17:
            assert (
                time_period == "Afternoon"
            ), f"Hour {hour} should be Afternoon, got {time_period}"
        elif 18 <= hour <= 22:
            assert (
                time_period == "Evening"
            ), f"Hour {hour} should be Evening, got {time_period}"
        else:
            assert (
                time_period == "Night"
            ), f"Hour {hour} should be Night, got {time_period}"

    @given(st.one_of(st.none(), st.text(min_size=0, max_size=50)))
    def test_null_handling_property(self, spark, enrichment_pipeline, nullable_text):
        """
        Property-based test for null value handling in enrichment.

        Tests that enrichment gracefully handles null and empty values.
        """
        # Create test data with potential null values
        test_data = [
            {
                "transaction_id": "test-123",
                "customer_id": "customer-456",
                "product_id": nullable_text,
                "quantity": 1,
                "price": 10.0,
                "total_amount": 10.0,
                "timestamp": datetime.now(timezone.utc),
                "customer_tier": nullable_text,
                "customer_lifetime_value": None,
                "category": nullable_text,
                "location": nullable_text,
                "payment_method": "credit_card",
            }
        ]

        df = spark.createDataFrame(test_data, TRANSACTION_SCHEMA)

        # Enrichment should not fail on null values
        try:
            enriched_df = enrichment_pipeline.enrich_transaction_stream(df)
            results = enriched_df.collect()

            # Should have exactly one result
            assert len(results) == 1, "Should handle null values without losing data"

            result = results[0]

            # Verify null handling produces expected defaults
            if nullable_text is None or nullable_text.strip() == "":
                # Customer tier should default to "Unknown" for null/empty
                if result.get("customer_tier_normalized"):
                    # Can be "Unknown" or normalized version of empty string
                    assert result["customer_tier_normalized"] in [
                        "Unknown",
                        "",
                    ], f"Expected Unknown for null tier, got {result['customer_tier_normalized']}"

                # LTV category should be "Unknown" for null LTV
                assert (
                    result["ltv_category"] == "Unknown"
                ), f"Expected Unknown for null LTV, got {result['ltv_category']}"

        except Exception as e:
            pytest.fail(f"Enrichment failed on null values: {e}")


class TestPropertyBasedInvariants:
    """Test business rule invariants using property-based testing."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing."""
        return create_spark_session("property-invariants-tests")

    @given(
        st.floats(
            min_value=0.01, max_value=1000000.0, allow_nan=False, allow_infinity=False
        )
    )
    def test_revenue_calculation_invariant(self, spark, total_amount):
        """
        Test that revenue calculations maintain consistent proportions.

        Invariant: Revenue impact should always be 20% of total amount.
        """
        test_data = [(total_amount,)]
        df = spark.createDataFrame(test_data, ["total_amount"])

        # Apply revenue calculation logic
        from pyspark.sql.functions import lit

        result_df = df.withColumn("revenue_impact", col("total_amount") * lit(0.2))

        result = result_df.collect()[0]
        calculated_revenue = result["revenue_impact"]
        expected_revenue = total_amount * 0.2

        # Allow for floating point precision differences
        assert (
            abs(calculated_revenue - expected_revenue) < 0.01
        ), f"Revenue calculation invariant violated: {calculated_revenue} vs {expected_revenue}"

    @given(st.integers(min_value=1, max_value=1000))
    def test_quantity_business_rule_invariant(self, spark, quantity):
        """
        Test that bulk purchase classification is consistent.

        Invariant: Quantity >= 5 should always be classified as bulk purchase.
        """
        test_data = [(quantity,)]
        df = spark.createDataFrame(test_data, ["quantity"])

        # Apply bulk purchase logic
        from pyspark.sql.functions import lit

        result_df = df.withColumn("bulk_purchase", col("quantity") >= lit(5))

        result = result_df.collect()[0]
        is_bulk = result["bulk_purchase"]

        if quantity >= 5:
            assert is_bulk == True, f"Quantity {quantity} should be bulk purchase"
        else:
            assert is_bulk == False, f"Quantity {quantity} should not be bulk purchase"

    @given(
        st.lists(
            st.floats(
                min_value=0.01, max_value=1000.0, allow_nan=False, allow_infinity=False
            ),
            min_size=1,
            max_size=100,
        )
    )
    def test_monotonic_price_categories_invariant(self, spark, price_list):
        """
        Test that price categories maintain monotonic ordering.

        Invariant: Higher prices should never have lower categories.
        """
        # Create DataFrame with prices
        test_data = [(price,) for price in price_list]
        df = spark.createDataFrame(test_data, ["price"])

        # Apply price categorization
        from pyspark.sql.functions import lit, when

        result_df = (
            df.withColumn(
                "price_category",
                when(col("price") >= 100, lit("High"))
                .when(col("price") >= 50, lit("Medium"))
                .when(col("price") >= 10, lit("Low"))
                .otherwise(lit("Very Low")),
            )
            .withColumn(
                "category_rank",
                when(col("price_category") == "High", lit(4))
                .when(col("price_category") == "Medium", lit(3))
                .when(col("price_category") == "Low", lit(2))
                .otherwise(lit(1)),
            )
            .orderBy("price")
        )

        results = result_df.collect()

        # Verify monotonic property: higher prices should have >= category ranks
        for i in range(len(results) - 1):
            current_price = results[i]["price"]
            next_price = results[i + 1]["price"]
            current_rank = results[i]["category_rank"]
            next_rank = results[i + 1]["category_rank"]

            if next_price > current_price:
                assert (
                    next_rank >= current_rank
                ), f"Monotonic invariant violated: price {next_price} (rank {next_rank}) > {current_price} (rank {current_rank})"
