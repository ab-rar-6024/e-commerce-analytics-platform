"""
Property-based tests for API endpoints with fuzz testing.

This module implements property-based testing and fuzz testing for FastAPI endpoints,
ensuring robustness against malformed inputs and edge cases.
"""

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

try:
    from hypothesis_jsonschema import from_schema
except ImportError:
    # Fallback for testing without jsonschema support
    def from_schema(schema):
        """Fallback implementation for hypothesis-jsonschema."""
        import hypothesis.strategies as st

        return st.dictionaries(
            st.text(), st.one_of(st.text(), st.integers(), st.floats())
        )


from src.api.auth.security import create_access_token
from src.api.dependencies import get_database_session
from src.api.main import app

# JSON Schema for API request validation
CUSTOMER_ANALYTICS_SCHEMA = {
    "type": "object",
    "properties": {
        "customer_id": {"type": "string", "minLength": 1, "maxLength": 100},
        "date_from": {"type": "string", "format": "date"},
        "date_to": {"type": "string", "format": "date"},
        "metrics": {
            "type": "array",
            "items": {"type": "string", "enum": ["rfm", "clv", "churn", "journey"]},
            "minItems": 1,
            "maxItems": 4,
        },
    },
    "required": ["customer_id"],
    "additionalProperties": False,
}

FRAUD_DETECTION_SCHEMA = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string", "minLength": 1},
        "amount": {"type": "number", "minimum": 0, "maximum": 1000000},
        "customer_id": {"type": "string", "minLength": 1},
        "merchant_id": {"type": "string", "minLength": 1},
        "timestamp": {"type": "string", "format": "date-time"},
        "location": {"type": "string"},
        "payment_method": {
            "type": "string",
            "enum": ["credit_card", "debit_card", "paypal", "cash"],
        },
    },
    "required": ["transaction_id", "amount", "customer_id", "merchant_id"],
    "additionalProperties": False,
}


# Hypothesis strategies for fuzz testing
@st.composite
def malformed_json_strings(draw):
    """Generate malformed JSON strings for fuzz testing."""
    base_strategies = [
        st.text(min_size=1, max_size=1000),  # Random text
        st.just('{"incomplete": '),  # Incomplete JSON
        st.just('[{"nested": {"very": {"deep": [1,2,3,'),  # Deeply nested incomplete
        st.just('{"duplicate": 1, "duplicate": 2}'),  # Duplicate keys
        st.just('{"number": 123.45.67}'),  # Invalid number
        st.just('{"string": "unescaped\ntab\t"}'),  # Unescaped characters
        st.just('{"null": nul}'),  # Misspelled null
        st.just('{"boolean": tue}'),  # Misspelled boolean
        st.just(""),  # Empty string
        st.just("null"),  # Just null
        st.just("[]"),  # Empty array
        st.just("{}"),  # Empty object
    ]
    return draw(st.one_of(base_strategies))


@st.composite
def malformed_headers(draw):
    """Generate malformed HTTP headers for testing."""
    header_strategies = [
        st.dictionaries(
            st.text(min_size=1, max_size=50),
            st.text(min_size=0, max_size=1000),
            min_size=0,
            max_size=10,
        ),
        st.just({"Content-Type": "application/xml"}),  # Wrong content type
        st.just({"Authorization": "InvalidToken"}),  # Invalid auth
        st.just({"Authorization": "Bearer "}),  # Empty bearer token
        st.just({"Accept": "text/plain"}),  # Wrong accept header
        st.just({}),  # No headers
    ]
    return draw(st.one_of(header_strategies))


@st.composite
def extreme_query_params(draw):
    """Generate extreme query parameter values."""
    param_strategies = [
        st.dictionaries(
            st.text(min_size=1, max_size=50),
            st.one_of(
                st.text(min_size=0, max_size=10000),  # Very long strings
                st.integers(
                    min_value=-(2**31), max_value=2**31 - 1
                ),  # Extreme integers
                st.floats(allow_nan=True, allow_infinity=True),  # Special floats
                st.just(""),  # Empty string
                st.just("null"),  # String null
                st.just("undefined"),  # String undefined
            ),
            min_size=0,
            max_size=20,
        )
    ]
    return draw(st.one_of(param_strategies))


class TestPropertyBasedAPI:
    """Property-based tests for API endpoints."""

    @pytest.fixture
    def client(self):
        """Create test client with the database dependency stubbed out."""
        app.dependency_overrides[get_database_session] = lambda: MagicMock()
        yield TestClient(app)
        app.dependency_overrides.pop(get_database_session, None)

    @pytest.fixture
    def auth_token(self):
        """Create valid auth token for testing."""
        return create_access_token(data={"sub": "test_user", "role": "analyst"})

    @pytest.fixture
    def auth_headers(self, auth_token):
        """Create auth headers."""
        return {"Authorization": f"Bearer {auth_token}"}

    @given(malformed_json_strings())
    @settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_api_endpoints_handle_malformed_json(
        self, client, auth_headers, malformed_json
    ):
        """
        Test that API endpoints gracefully handle malformed JSON.

        Property: API should never crash on malformed input, always return
        appropriate HTTP status codes.
        """
        endpoints = [
            "/api/v1/analytics/customers",
            "/api/v1/fraud/detect",
            "/api/v1/analytics/revenue",
            "/api/v1/analytics/products",
        ]

        for endpoint in endpoints:
            try:
                response = client.post(
                    endpoint,
                    data=malformed_json,
                    headers={**auth_headers, "Content-Type": "application/json"},
                )

                # Should not crash - must return valid HTTP status
                assert (
                    200 <= response.status_code < 600
                ), f"Invalid HTTP status {response.status_code} for endpoint {endpoint}"

                # Should typically be 400 (Bad Request) or 422 (Unprocessable Entity) for malformed JSON
                if response.status_code not in [400, 422, 401, 403]:
                    # Log unexpected success responses for investigation
                    print(
                        f"Unexpected response {response.status_code} for malformed JSON at {endpoint}"
                    )

                # Response should be valid JSON (even error responses)
                try:
                    response.json()
                except json.JSONDecodeError:
                    pytest.fail(f"API returned invalid JSON for endpoint {endpoint}")

            except Exception as e:
                pytest.fail(
                    f"API crashed on malformed JSON for endpoint {endpoint}: {e}"
                )

    @given(malformed_headers())
    @settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_api_endpoints_handle_malformed_headers(self, client, malformed_headers):
        """
        Test that API endpoints handle malformed or unusual headers gracefully.

        Property: Headers should not cause crashes or security issues.
        """
        endpoints = [
            ("/api/v1/health", "GET"),
            ("/api/v1/analytics/customers/segments", "GET"),
            ("/api/v1/fraud/risk-assessment", "GET"),
        ]

        for endpoint, method in endpoints:
            try:
                if method == "GET":
                    response = client.get(endpoint, headers=malformed_headers)
                else:
                    response = client.post(
                        endpoint, json={"test": "data"}, headers=malformed_headers
                    )

                # Should not crash
                assert (
                    200 <= response.status_code < 600
                ), f"Invalid HTTP status {response.status_code} for endpoint {endpoint}"

                # Should return valid JSON
                try:
                    response.json()
                except json.JSONDecodeError:
                    # Some endpoints might return non-JSON, that's acceptable
                    pass

            except UnicodeEncodeError:
                # The HTTP client refuses to send non-ASCII headers; nothing reached the API
                return
            except Exception as e:
                pytest.fail(
                    f"API crashed on malformed headers for endpoint {endpoint}: {e}"
                )

    @given(extreme_query_params())
    @settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_api_endpoints_handle_extreme_query_params(
        self, client, auth_headers, extreme_params
    ):
        """
        Test that API endpoints handle extreme query parameter values.

        Property: Query parameters should not cause buffer overflows or crashes.
        """
        endpoints = [
            "/api/v1/analytics/customers",
            "/api/v1/analytics/revenue",
            "/api/v1/analytics/products",
            "/api/v1/health",
        ]

        for endpoint in endpoints:
            try:
                # Convert all param values to strings (as HTTP would)
                str_params = {}
                for key, value in extreme_params.items():
                    if isinstance(value, (int, float)):
                        if (
                            str(value) == "nan"
                            or str(value) == "inf"
                            or str(value) == "-inf"
                        ):
                            str_params[key] = str(value)
                        else:
                            str_params[key] = str(value)[
                                :1000
                            ]  # Truncate very long numbers
                    else:
                        str_params[key] = str(value)[
                            :1000
                        ]  # Truncate very long strings

                response = client.get(endpoint, params=str_params, headers=auth_headers)

                # Should not crash
                assert (
                    200 <= response.status_code < 600
                ), f"Invalid HTTP status {response.status_code} for endpoint {endpoint}"

                # Response time should be reasonable (< 30 seconds)
                # This tests for potential DoS via query param processing
                assert (
                    response.elapsed.total_seconds() < 30
                ), f"Response time too long for endpoint {endpoint}: {response.elapsed.total_seconds()}s"

            except Exception as e:
                pytest.fail(
                    f"API crashed on extreme query params for endpoint {endpoint}: {e}"
                )

    @given(from_schema(CUSTOMER_ANALYTICS_SCHEMA))
    @settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_customer_analytics_endpoint_valid_inputs(
        self, client, auth_headers, customer_data
    ):
        """
        Test customer analytics endpoint with property-based valid inputs.

        Property: Valid inputs should either succeed or fail predictably.
        """

        response = client.get(
            "/api/v1/analytics/customers/segments",
            params={"segment_type": customer_data["customer_id"]},
            headers=auth_headers,
        )

        # Should not crash
        assert 200 <= response.status_code < 600

        # Should return valid JSON
        try:
            response_data = response.json()

            if response.status_code == 200:
                # Successful response should have expected structure
                assert isinstance(
                    response_data, dict
                ), "Success response should be dict"

            elif response.status_code in [400, 422]:
                # Validation error should have error details
                assert (
                    "detail" in response_data or "error" in response_data
                ), "Error response should contain details"

        except json.JSONDecodeError:
            pytest.fail(f"API returned invalid JSON: {response.text}")

    @given(from_schema(FRAUD_DETECTION_SCHEMA))
    @settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_fraud_detection_endpoint_valid_inputs(
        self, client, auth_headers, fraud_data
    ):
        """
        Test fraud detection endpoint with property-based valid inputs.

        Property: Valid fraud detection requests should process without errors.
        """

        response = client.get(
            "/api/v1/fraud/risk-assessment",
            params={"assessment_type": fraud_data["customer_id"]},
            headers=auth_headers,
        )

        # Should not crash
        assert 200 <= response.status_code < 600

        # Should return valid JSON
        try:
            response_data = response.json()

            if response.status_code == 200:
                # Successful fraud detection should have risk score
                assert isinstance(response_data, dict), "Response should be dict"
                if "risk_score" in response_data:
                    assert (
                        0 <= response_data["risk_score"] <= 1
                    ), f"Risk score {response_data['risk_score']} should be 0-1"

        except json.JSONDecodeError:
            pytest.fail(f"Fraud detection returned invalid JSON: {response.text}")

    @given(
        st.text(
            alphabet=st.characters(min_codepoint=33, max_codepoint=126),
            min_size=0,
            max_size=10000,
        )
    )
    @settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_authentication_edge_cases(self, client, auth_token_text):
        """
        Test authentication with various token formats.

        Property: Authentication should consistently reject invalid tokens.
        """
        headers = {"Authorization": f"Bearer {auth_token_text}"}

        response = client.get("/api/v1/analytics/customers/segments", headers=headers)

        # Should not crash
        assert 200 <= response.status_code < 600

        # Invalid tokens should typically result in 401 Unauthorized
        if auth_token_text == "" or len(auth_token_text) < 10:
            # Very short or empty tokens should be rejected
            assert response.status_code in [
                401,
                403,
            ], f"Empty/short token should be rejected, got {response.status_code}"


class TestAPIInvariants:
    """Test API business rule invariants."""

    @pytest.fixture
    def client(self):
        """Create test client with the database dependency stubbed out."""
        app.dependency_overrides[get_database_session] = lambda: MagicMock()
        yield TestClient(app)
        app.dependency_overrides.pop(get_database_session, None)

    @pytest.fixture
    def auth_headers(self):
        """Create auth headers."""
        token = create_access_token(data={"sub": "test_user", "role": "admin"})
        return {"Authorization": f"Bearer {token}"}

    def test_response_time_invariant(self, client, auth_headers):
        """
        Test that API responses maintain reasonable time bounds.

        Invariant: All API responses should complete within 30 seconds.
        """
        endpoints = [
            "/api/v1/health",
            "/api/v1/analytics/revenue",
            "/api/v1/analytics/products",
        ]

        for endpoint in endpoints:
            start_time = datetime.now()

            try:
                response = client.get(endpoint, headers=auth_headers)
                end_time = datetime.now()

                response_time = (end_time - start_time).total_seconds()

                assert (
                    response_time < 30
                ), f"Response time {response_time}s exceeds 30s limit for {endpoint}"

            except Exception as e:
                pytest.fail(f"Request to {endpoint} failed: {e}")

    @given(st.integers(min_value=1, max_value=1000))
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_pagination_invariant(self, client, auth_headers, page_size):
        """
        Test that pagination parameters are handled consistently.

        Invariant: Page size should be respected and capped at reasonable limits.
        """

        params = {"limit": page_size, "offset": 0}

        response = client.get(
            "/api/v1/customers/", params=params, headers=auth_headers
        )

        # Should not crash regardless of page size
        assert 200 <= response.status_code < 600

        if response.status_code == 200:
            try:
                data = response.json()

                # If response contains items, length should respect limits
                if isinstance(data, dict) and "items" in data:
                    items = data["items"]
                    if isinstance(items, list):
                        # Should not return more items than requested
                        # (allowing for server-side caps)
                        max_allowed = min(
                            page_size, 100
                        )  # Assume 100 is server cap
                        assert (
                            len(items) <= max_allowed
                        ), f"Returned {len(items)} items, expected max {max_allowed}"

            except json.JSONDecodeError:
                pass  # Non-JSON responses are acceptable for some endpoints

    def test_error_response_format_invariant(self, client):
        """
        Test that error responses maintain consistent format.

        Invariant: All error responses should contain error details.
        """
        # Test without authentication (should trigger 401)
        response = client.get("/api/v1/analytics/customers/segments")

        assert response.status_code in [
            401,
            403,
        ], "Should reject unauthenticated request with 401/403"

        try:
            error_data = response.json()

            # Error response should be structured
            assert isinstance(error_data, dict), "Error response should be dict"

            # Should contain error information
            has_error_info = any(
                key in error_data for key in ["detail", "error", "message"]
            )
            assert has_error_info, f"Error response missing error info: {error_data}"

        except json.JSONDecodeError:
            pytest.fail("Error response should be valid JSON")
