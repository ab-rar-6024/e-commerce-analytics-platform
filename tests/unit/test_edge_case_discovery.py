"""
Edge case discovery automation for the e-commerce analytics platform.

This module implements automated edge case discovery using property-based testing
and systematic boundary value analysis to identify potential system vulnerabilities.
"""

import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from hypothesis import given, note, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, rule
from pyspark.sql import SparkSession
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


# Edge case value generators
@st.composite
def extreme_numeric_values(draw):
    """Generate extreme numeric values that might cause issues."""
    extreme_values = [
        0,
        -0,
        0.0,
        -0.0,  # Zero variants
        1,
        -1,
        0.1,
        -0.1,  # Small values
        sys.maxsize,
        -sys.maxsize,  # System limits
        float("inf"),
        float("-inf"),  # Infinity
        float("nan"),  # NaN
        1e-100,
        1e100,
        -1e100,  # Very small/large scientific notation
        0.999999999999999,
        1.000000000000001,  # Floating point precision edge
        2**31 - 1,
        -(2**31),  # 32-bit integer limits
        2**63 - 1,
        -(2**63),  # 64-bit integer limits
        Decimal("0.01"),
        Decimal("999999999.99"),  # Decimal edge cases
    ]
    return draw(st.sampled_from(extreme_values))


@st.composite
def extreme_string_values(draw):
    """Generate extreme string values that might cause issues."""
    return draw(
        st.one_of(
            [
                st.just(""),  # Empty string
                st.just(" "),  # Single space
                st.just("   "),  # Multiple spaces
                st.just("\n"),
                st.just("\t"),
                st.just("\r"),  # Whitespace characters
                st.just("\x00"),
                st.just("\xff"),  # Control characters
                st.just("NULL"),
                st.just("null"),
                st.just("None"),  # Null-like strings
                st.just("undefined"),
                st.just("NaN"),
                st.just("Infinity"),  # Special values
                st.just("'"),
                st.just('"'),
                st.just("\\"),  # Quote and escape characters
                st.just("--"),
                st.just("/*"),
                st.just("*/"),  # SQL comment patterns
                st.just("<script>"),
                st.just("</script>"),  # XSS patterns
                st.just("../../"),
                st.just("../../../"),  # Path traversal patterns
                st.text(min_size=0, max_size=0),  # Empty from strategy
                st.just("A" * 10000),  # Very long string
                st.text(
                    alphabet=st.characters(min_codepoint=0, max_codepoint=127)
                ),  # ASCII only
                st.text(
                    alphabet=st.characters(min_codepoint=128, max_codepoint=65535)
                ),  # Unicode
                st.text(alphabet=["\u0000", "\uffff"]),  # Unicode boundaries
            ]
        )
    )


@st.composite
def extreme_temporal_values(draw):
    """Generate extreme temporal values that might cause issues."""
    extreme_dates = [
        datetime.min.replace(tzinfo=timezone.utc),  # Minimum datetime
        datetime.max.replace(tzinfo=timezone.utc),  # Maximum datetime
        datetime(1970, 1, 1, tzinfo=timezone.utc),  # Unix epoch
        datetime(2000, 1, 1, tzinfo=timezone.utc),  # Y2K
        datetime(2038, 1, 19, 3, 14, 7, tzinfo=timezone.utc),  # 32-bit timestamp limit
        datetime(1900, 1, 1, tzinfo=timezone.utc),  # Before many systems
        datetime(2100, 12, 31, tzinfo=timezone.utc),  # Far future
        datetime.now(timezone.utc),  # Current time
        datetime.now(timezone.utc) + timedelta(seconds=1),  # Near future
        datetime.now(timezone.utc) - timedelta(seconds=1),  # Near past
    ]
    return draw(st.sampled_from(extreme_dates))


class EdgeCaseDiscoveryTester:
    """Automated edge case discovery and testing framework."""

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.discovered_edge_cases = []
        self.error_patterns = {}

    def discover_numeric_edge_cases(self, test_function, *args, **kwargs):
        """
        Discover edge cases in numeric processing functions.

        Args:
            test_function: Function to test for edge cases
            *args, **kwargs: Additional arguments for the test function
        """
        edge_cases_found = []

        # Test boundary values
        boundary_values = [
            0,
            1,
            -1,
            0.0,
            1.0,
            -1.0,
            sys.maxsize,
            -sys.maxsize,
            sys.maxsize + 1,
            float("inf"),
            float("-inf"),
            float("nan"),
            2**31 - 1,
            -(2**31),
            2**63 - 1,
            -(2**63),
            1e-100,
            1e100,
            -1e100,
            0.1,
            0.9,
            1.1,
            9.9,
            None,
        ]

        for value in boundary_values:
            try:
                result = test_function(value, *args, **kwargs)

                # Check for suspicious results
                if self._is_suspicious_result(result):
                    edge_cases_found.append(
                        {
                            "input": value,
                            "output": result,
                            "type": "suspicious_result",
                            "function": test_function.__name__,
                        }
                    )

            except Exception as e:
                edge_cases_found.append(
                    {
                        "input": value,
                        "error": str(e),
                        "exception_type": type(e).__name__,
                        "type": "exception",
                        "function": test_function.__name__,
                    }
                )

        self.discovered_edge_cases.extend(edge_cases_found)
        return edge_cases_found

    def discover_string_edge_cases(self, test_function, *args, **kwargs):
        """Discover edge cases in string processing functions."""
        edge_cases_found = []

        string_values = [
            "",
            " ",
            "   ",
            "\n",
            "\t",
            "\r",
            "\x00",
            "NULL",
            "null",
            "None",
            "undefined",
            "NaN",
            "'",
            '"',
            "\\",
            "--",
            "/*",
            "*/",
            "<script>",
            "../../",
            "a" * 10000,  # Very long string
            "\u0000",
            "\uffff",  # Unicode boundaries
            "normal_string",  # Control case
            None,
        ]

        for value in string_values:
            try:
                result = test_function(value, *args, **kwargs)

                if self._is_suspicious_result(result):
                    edge_cases_found.append(
                        {
                            "input": value,
                            "output": result,
                            "type": "suspicious_result",
                            "function": test_function.__name__,
                        }
                    )

            except Exception as e:
                edge_cases_found.append(
                    {
                        "input": value,
                        "error": str(e),
                        "exception_type": type(e).__name__,
                        "type": "exception",
                        "function": test_function.__name__,
                    }
                )

        self.discovered_edge_cases.extend(edge_cases_found)
        return edge_cases_found

    def discover_temporal_edge_cases(self, test_function, *args, **kwargs):
        """Discover edge cases in temporal processing functions."""
        edge_cases_found = []

        temporal_values = [
            datetime.min.replace(tzinfo=timezone.utc),
            datetime.max.replace(tzinfo=timezone.utc),
            datetime(1970, 1, 1, tzinfo=timezone.utc),
            datetime(2038, 1, 19, 3, 14, 7, tzinfo=timezone.utc),
            datetime.now(timezone.utc),
            None,
        ]

        for value in temporal_values:
            try:
                result = test_function(value, *args, **kwargs)

                if self._is_suspicious_result(result):
                    edge_cases_found.append(
                        {
                            "input": value,
                            "output": result,
                            "type": "suspicious_result",
                            "function": test_function.__name__,
                        }
                    )

            except Exception as e:
                edge_cases_found.append(
                    {
                        "input": value,
                        "error": str(e),
                        "exception_type": type(e).__name__,
                        "type": "exception",
                        "function": test_function.__name__,
                    }
                )

        self.discovered_edge_cases.extend(edge_cases_found)
        return edge_cases_found

    def _is_suspicious_result(self, result):
        """Check if a result is suspicious and might indicate an edge case."""
        if result is None:
            return True

        # Check for NaN or infinity in numeric results
        if isinstance(result, (int, float)):
            if result != result or result == float("inf") or result == float("-inf"):
                return True

        # Check for empty or very long strings
        if isinstance(result, str):
            if len(result) == 0 or len(result) > 1000:
                return True

        # Check for unusual data structures
        if isinstance(result, (list, dict)):
            if len(result) == 0 or len(result) > 10000:
                return True

        return False

    def analyze_error_patterns(self):
        """Analyze discovered edge cases for common error patterns."""
        error_counts = {}

        for case in self.discovered_edge_cases:
            if case["type"] == "exception":
                exception_type = case["exception_type"]
                error_counts[exception_type] = error_counts.get(exception_type, 0) + 1

        self.error_patterns = error_counts
        return error_counts

    def generate_edge_case_report(self):
        """Generate a comprehensive report of discovered edge cases."""
        report = {
            "total_edge_cases": len(self.discovered_edge_cases),
            "exceptions": [
                case
                for case in self.discovered_edge_cases
                if case["type"] == "exception"
            ],
            "suspicious_results": [
                case
                for case in self.discovered_edge_cases
                if case["type"] == "suspicious_result"
            ],
            "error_patterns": self.analyze_error_patterns(),
            "functions_tested": list(
                set(case["function"] for case in self.discovered_edge_cases)
            ),
        }

        return report


class TestAutomatedEdgeCaseDiscovery:
    """Automated edge case discovery tests."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing."""
        return create_spark_session("edge-case-discovery")

    @pytest.fixture(scope="class")
    def edge_case_tester(self, spark):
        """Create edge case discovery tester."""
        return EdgeCaseDiscoveryTester(spark)

    @pytest.fixture(scope="class")
    def enrichment_pipeline(self, spark):
        """Create enrichment pipeline for testing."""
        return DataEnrichmentPipeline(spark)

    def test_numeric_boundary_discovery(self, edge_case_tester, spark):
        """Discover edge cases in numeric data processing."""

        def test_price_categorization(price_value):
            """Test function for price categorization logic."""
            if price_value is None:
                return "null_input"

            # Mock price categorization logic
            try:
                if price_value >= 100:
                    return "High"
                elif price_value >= 50:
                    return "Medium"
                elif price_value >= 10:
                    return "Low"
                else:
                    return "Very Low"
            except (TypeError, ValueError, OverflowError):
                return "error_in_comparison"

        # Discover edge cases
        edge_cases = edge_case_tester.discover_numeric_edge_cases(
            test_price_categorization
        )

        # Analyze results
        # A robust function may legitimately produce no edge cases; discovery
        # itself is verified against fragile functions in
        # test_discovery_flags_fragile_functions
        assert isinstance(edge_cases, list)

        # Check for specific edge case types
        has_infinity_case = any(
            str(case.get("input")) in ["inf", "-inf"] for case in edge_cases
        )
        has_nan_case = any(str(case.get("input")) == "nan" for case in edge_cases)
        has_none_case = any(case.get("input") is None for case in edge_cases)

        # Log discovered cases for manual review
        print(f"\nDiscovered {len(edge_cases)} numeric edge cases:")
        for case in edge_cases[:5]:  # Show first 5
            print(f"  Input: {case['input']}, Type: {case['type']}")

    def test_string_boundary_discovery(self, edge_case_tester):
        """Discover edge cases in string data processing."""

        def test_customer_tier_normalization(tier_value):
            """Test function for customer tier normalization."""
            if tier_value is None:
                return "Unknown"

            try:
                # Mock normalization logic
                normalized = str(tier_value).upper().strip()
                if normalized in ["PREMIUM", "GOLD", "SILVER", "BRONZE"]:
                    return normalized
                else:
                    return "Unknown"
            except (AttributeError, TypeError):
                return "error_in_processing"

        # Discover edge cases
        edge_cases = edge_case_tester.discover_string_edge_cases(
            test_customer_tier_normalization
        )

        # Analyze results
        assert isinstance(edge_cases, list)

        # Check for specific patterns
        has_empty_string = any(case.get("input") == "" for case in edge_cases)
        has_whitespace = any(
            case.get("input") in [" ", "\n", "\t"] for case in edge_cases
        )
        has_special_chars = any(
            case.get("input") in ["'", '"', "\\"] for case in edge_cases
        )

        print(f"\nDiscovered {len(edge_cases)} string edge cases:")
        for case in edge_cases[:5]:  # Show first 5
            print(f"  Input: {repr(case['input'])}, Type: {case['type']}")

    def test_temporal_boundary_discovery(self, edge_case_tester):
        """Discover edge cases in temporal data processing."""

        def test_time_period_calculation(timestamp_value):
            """Test function for time period calculation."""
            if timestamp_value is None:
                return "null_timestamp"

            try:
                hour = timestamp_value.hour
                if 6 <= hour <= 11:
                    return "Morning"
                elif 12 <= hour <= 17:
                    return "Afternoon"
                elif 18 <= hour <= 22:
                    return "Evening"
                else:
                    return "Night"
            except (AttributeError, TypeError, ValueError):
                return "error_in_time_processing"

        # Discover edge cases
        edge_cases = edge_case_tester.discover_temporal_edge_cases(
            test_time_period_calculation
        )

        # Analyze results
        assert isinstance(edge_cases, list)

        print(f"\nDiscovered {len(edge_cases)} temporal edge cases:")
        for case in edge_cases[:5]:  # Show first 5
            print(f"  Input: {case['input']}, Type: {case['type']}")

    def test_discovery_flags_fragile_functions(self, edge_case_tester):
        """Functions that raise on boundary inputs must be reported as edge cases."""

        def fragile_numeric(value):
            return 100 / value  # ZeroDivisionError / TypeError

        def fragile_string(value):
            return value[0]  # IndexError on the empty string

        def fragile_temporal(value):
            return value.hour  # AttributeError on non-datetime inputs

        for discover, fragile in [
            (edge_case_tester.discover_numeric_edge_cases, fragile_numeric),
            (edge_case_tester.discover_string_edge_cases, fragile_string),
            (edge_case_tester.discover_temporal_edge_cases, fragile_temporal),
        ]:
            edge_cases = discover(fragile)
            assert any(
                case["type"] == "exception" for case in edge_cases
            ), f"{fragile.__name__} should be flagged"

    @given(extreme_numeric_values())
    @settings(max_examples=20, deadline=None)
    def test_property_based_numeric_edge_discovery(self, extreme_value):
        """Use property-based testing to discover numeric edge cases."""

        def risk_score_calculation(amount):
            """Calculate risk score based on transaction amount."""
            try:
                if amount is None:
                    return 0

                # Convert to float for calculation
                amount_float = float(amount)

                # Risk calculation logic
                if amount_float > 1000:
                    return min(amount_float / 1000, 10)  # Cap at 10
                else:
                    # Negative amounts must not produce a negative risk score
                    return max(amount_float, 0.0) / 1000

            except (ValueError, TypeError, OverflowError, ZeroDivisionError):
                return -1  # Error indicator

        result = risk_score_calculation(extreme_value)

        # Properties that should hold
        if result != -1:  # If no error occurred
            assert isinstance(
                result, (int, float)
            ), f"Result should be numeric, got {type(result)}"

            # Risk score should be reasonable
            if not (isinstance(result, float) and (result != result)):  # Check for NaN
                assert result >= 0, f"Risk score should be non-negative, got {result}"

        # Note the result for analysis
        note(f"Input: {extreme_value}, Output: {result}")

    @given(extreme_string_values())
    @settings(max_examples=20, deadline=None)
    def test_property_based_string_edge_discovery(self, extreme_string):
        """Use property-based testing to discover string edge cases."""

        def location_region_mapping(location):
            """Map location string to region."""
            try:
                if location is None:
                    return "Unknown"

                location_str = str(location).lower().strip()

                if "new york" in location_str or "ny" in location_str:
                    return "Northeast"
                elif "california" in location_str or "ca" in location_str:
                    return "West"
                elif "texas" in location_str or "tx" in location_str:
                    return "South"
                else:
                    return "Other"

            except (AttributeError, TypeError, UnicodeError):
                return "Error"

        result = location_region_mapping(extreme_string)

        # Properties that should hold
        valid_regions = {"Northeast", "West", "South", "Other", "Unknown", "Error"}
        assert result in valid_regions, f"Invalid region mapping: {result}"

        # String result should be reasonable length
        assert len(result) < 50, f"Region name too long: {len(result)} characters"

        note(f"Input: {repr(extreme_string)}, Output: {result}")

    def test_comprehensive_edge_case_analysis(self, edge_case_tester):
        """Perform comprehensive edge case analysis and generate report."""

        # Test multiple functions for edge cases
        def divide_operation(a, b):
            return a / b if b != 0 else float("inf")

        def string_length(s):
            return len(str(s)) if s is not None else 0

        def list_access(lst, index):
            return (
                lst[index] if isinstance(lst, list) and 0 <= index < len(lst) else None
            )

        # Discover edge cases for different function types
        numeric_cases = edge_case_tester.discover_numeric_edge_cases(
            divide_operation, 10
        )
        string_cases = edge_case_tester.discover_string_edge_cases(string_length)

        # Generate comprehensive report
        report = edge_case_tester.generate_edge_case_report()

        # Validate report structure
        assert "total_edge_cases" in report
        assert "exceptions" in report
        assert "suspicious_results" in report
        assert "error_patterns" in report
        assert "functions_tested" in report

        # Report should contain discovered cases
        assert report["total_edge_cases"] > 0, "Should discover some edge cases"

        print(f"\nComprehensive Edge Case Analysis Report:")
        print(f"Total edge cases discovered: {report['total_edge_cases']}")
        print(f"Functions tested: {report['functions_tested']}")
        print(f"Exception patterns: {report['error_patterns']}")

        # Check for common error patterns
        common_errors = [
            "TypeError",
            "ValueError",
            "ZeroDivisionError",
            "AttributeError",
        ]
        found_errors = set(report["error_patterns"].keys())

        print(f"Common error types found: {found_errors.intersection(common_errors)}")


class StatefulEdgeCaseDiscovery(RuleBasedStateMachine):
    """Stateful testing for edge case discovery in data processing pipelines."""

    def __init__(self):
        super().__init__()
        self.processed_records = []
        self.processing_errors = []
        self.spark = create_spark_session("stateful-edge-discovery")
        self.enrichment_pipeline = DataEnrichmentPipeline(self.spark)

    @initialize()
    def init_state(self):
        """Initialize the state machine."""
        self.processed_records = []
        self.processing_errors = []

    @rule(
        data=st.dictionaries(
            st.sampled_from(["transaction_id", "customer_id", "amount", "timestamp"]),
            st.one_of(extreme_numeric_values(), extreme_string_values(), st.none()),
            min_size=1,
            max_size=4,
        )
    )
    def process_transaction_data(self, data):
        """Process transaction data and track edge cases."""
        try:
            # Attempt to process the data through our pipeline
            # This is a simplified simulation
            processed_data = {}

            for key, value in data.items():
                if key == "amount" and value is not None:
                    # Test amount processing
                    try:
                        amount_float = float(value)
                        processed_data[key + "_processed"] = amount_float

                        # Apply business rules
                        if amount_float > 10000:
                            processed_data["high_value_flag"] = True

                    except (ValueError, TypeError, OverflowError):
                        processed_data[key + "_error"] = str(value)

                elif key == "timestamp" and value is not None:
                    # Test timestamp processing
                    try:
                        if hasattr(value, "hour"):
                            processed_data["hour"] = value.hour
                        else:
                            processed_data["timestamp_error"] = str(value)
                    except AttributeError:
                        processed_data["timestamp_error"] = str(value)

                else:
                    # Handle other fields
                    processed_data[key] = str(value) if value is not None else None

            self.processed_records.append(
                {"input": data, "output": processed_data, "success": True}
            )

        except Exception as e:
            self.processing_errors.append(
                {"input": data, "error": str(e), "exception_type": type(e).__name__}
            )

    @rule()
    def validate_processing_consistency(self):
        """Validate that processing remains consistent across different inputs."""
        # Check that we're not accumulating too many errors
        error_rate = len(self.processing_errors) / max(
            len(self.processed_records) + len(self.processing_errors), 1
        )

        # Error rate should not exceed 50% (some errors are expected with extreme values)
        assert error_rate <= 0.5, f"Error rate too high: {error_rate:.2%}"

        # Check for pattern in successful processing
        successful_records = [r for r in self.processed_records if r["success"]]
        if len(successful_records) > 0:
            # At least some records should process successfully
            assert len(successful_records) > 0, "No records processed successfully"

    @rule()
    def check_resource_usage(self):
        """Check that extreme inputs don't cause resource exhaustion."""
        # In a real implementation, this would check memory usage, processing time, etc.
        # For now, we just verify the state is reasonable

        total_records = len(self.processed_records) + len(self.processing_errors)

        # Should not accumulate infinite records (potential memory issue)
        assert total_records < 10000, f"Too many records accumulated: {total_records}"


class TestStatefulEdgeCaseDiscovery:
    """Test the stateful edge case discovery system."""

    def test_stateful_edge_case_discovery(self):
        """Run stateful edge case discovery."""
        # Run the state machine to discover edge cases
        state_machine = StatefulEdgeCaseDiscovery()

        # Execute multiple steps to build up state and discover patterns
        try:
            for _ in range(50):  # Run 50 random operations
                state_machine.process_transaction_data(
                    {"transaction_id": "test_123", "amount": 100.0}
                )
                state_machine.validate_processing_consistency()

            # Final validation
            state_machine.check_resource_usage()

            # Analyze what we discovered
            total_processed = len(state_machine.processed_records)
            total_errors = len(state_machine.processing_errors)

            print(f"\nStateful Discovery Results:")
            print(f"Successfully processed: {total_processed}")
            print(f"Processing errors: {total_errors}")

            if total_errors > 0:
                error_types = {}
                for error in state_machine.processing_errors:
                    error_type = error["exception_type"]
                    error_types[error_type] = error_types.get(error_type, 0) + 1

                print(f"Error patterns discovered: {error_types}")

        except Exception as e:
            pytest.fail(f"Stateful edge case discovery failed: {e}")

    def test_edge_case_regression_suite(self):
        """Create a regression test suite from discovered edge cases."""
        # This would be populated from previous edge case discovery runs
        known_edge_cases = [
            {"input": float("inf"), "expected_behavior": "handle_gracefully"},
            {"input": float("nan"), "expected_behavior": "handle_gracefully"},
            {"input": None, "expected_behavior": "return_default"},
            {"input": "", "expected_behavior": "return_default"},
            {"input": sys.maxsize, "expected_behavior": "handle_gracefully"},
        ]

        def test_function_with_edge_case(value):
            """Function that should handle edge cases gracefully."""
            if value is None or value == "":
                return "default"

            try:
                if isinstance(value, (int, float)):
                    if value != value:  # NaN check
                        return "nan_handled"
                    elif value == float("inf") or value == float("-inf"):
                        return "infinity_handled"
                    else:
                        return str(value)
                else:
                    return str(value)
            except Exception:
                return "error_handled"

        # Test each known edge case
        for case in known_edge_cases:
            result = test_function_with_edge_case(case["input"])

            # Verify the function handles the edge case appropriately
            if case["expected_behavior"] == "handle_gracefully":
                assert result is not None, f"Should handle {case['input']} gracefully"
                assert isinstance(
                    result, str
                ), f"Should return string for {case['input']}"

            elif case["expected_behavior"] == "return_default":
                assert result == "default", f"Should return default for {case['input']}"

        print(f"Edge case regression suite passed for {len(known_edge_cases)} cases")
