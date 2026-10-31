"""
Regression tests for the dashboard/API integration fixes:

- SmartCompressionResponse sent a stale content-length (real clients got truncated JSON)
- the /api/v1/operations endpoints used by the operational dashboard did not exist
- fraud alert counting assumed a list while the API returns aggregate counts
- APIClient.authenticate faked a token instead of calling the login endpoint
- sidebar quick-select date ranges
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
from fastapi.testclient import TestClient

# The dashboard package is imported as `dashboard.*` (src/ is on its path)
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from dashboard.components.alerts import _alert_counts_by_priority  # noqa: E402
from dashboard.components.sidebar import _quick_range_dates  # noqa: E402
from dashboard.utils.api_client import APIClient  # noqa: E402

from src.api.auth.security import create_access_token  # noqa: E402
from src.api.dependencies import get_database_session  # noqa: E402
from src.api.main import app  # noqa: E402
from src.api.middleware.compression import create_optimized_response  # noqa: E402


class TestSmartCompressionResponse:
    def test_content_length_matches_body(self):
        data = {"items": [{"i": i, "name": "x" * 20} for i in range(200)], "none": None}

        response = create_optimized_response(data)

        assert len(response.body) > 4  # not the placeholder body ("null")
        assert int(response.headers["content-length"]) == len(response.body)

    def test_null_fields_are_removed(self):
        response = create_optimized_response({"keep": 1, "drop": None})

        assert b"drop" not in response.body
        assert b"keep" in response.body


class TestOperationsEndpoints:
    @pytest.fixture
    def client(self):
        app.dependency_overrides[get_database_session] = lambda: MagicMock()
        yield TestClient(app)
        app.dependency_overrides.pop(get_database_session, None)

    @pytest.fixture
    def headers(self):
        token = create_access_token(
            data={"sub": "admin", "user_id": 1, "role": "admin", "permissions": []}
        )
        return {"Authorization": f"Bearer {token}"}

    @pytest.mark.parametrize(
        "path, keys",
        [
            ("fraud-monitoring", {"monitoring", "model_health"}),
            ("data-quality", {"overview", "pipeline_health", "issue_distribution"}),
            ("alerts", {"summary", "alerts_by_category", "recent_alerts"}),
            ("performance", {"metrics"}),
            ("streaming-health", {"pipeline_metrics", "topic_health"}),
        ],
    )
    def test_endpoint_returns_expected_shape(self, client, headers, path, keys):
        response = client.get(f"/api/v1/operations/{path}", headers=headers)

        assert response.status_code == 200
        assert keys <= set(response.json())

    def test_requires_authentication(self, client):
        response = client.get("/api/v1/operations/performance")

        assert response.status_code in (401, 403)

    def test_scores_are_fractions(self, client, headers):
        overview = client.get("/api/v1/operations/data-quality", headers=headers).json()[
            "overview"
        ]

        for key in ("overall_quality_score", "completeness_score", "accuracy_score"):
            assert 0 <= overview[key] <= 1


class TestAlertCounts:
    def test_aggregate_dict_from_api(self):
        alerts = {"total": 95, "by_priority": {"critical": 2, "high": 11}}

        assert _alert_counts_by_priority(alerts) == {"critical": 2, "high": 11}

    def test_list_of_alert_records(self):
        alerts = [
            {"priority": "critical"},
            {"priority": "high"},
            {"priority": "high"},
            "not-a-dict",
        ]

        assert _alert_counts_by_priority(alerts) == {"critical": 1, "high": 2}

    @pytest.mark.parametrize("empty", [None, [], {}])
    def test_empty_inputs(self, empty):
        assert _alert_counts_by_priority(empty) == {}


class TestApiClientAuthentication:
    def _client(self):
        return APIClient("http://api.test")

    def test_successful_login_stores_token(self):
        client = self._client()
        response = MagicMock(status_code=200)
        response.json.return_value = {"access_token": "jwt-123", "expires_in": 600}

        with patch.object(client.session, "post", return_value=response) as post:
            assert client.authenticate("admin", "secret") is True

        assert post.call_args.args[0] == "http://api.test/api/v1/auth/login"
        assert post.call_args.kwargs["json"] == {
            "username": "admin",
            "password": "secret",
        }
        assert client.auth_token == "jwt-123"
        assert client._get_headers()["Authorization"] == "Bearer jwt-123"
        assert client._is_token_valid()

    def test_rejected_login(self):
        client = self._client()

        with patch.object(
            client.session, "post", return_value=MagicMock(status_code=401)
        ):
            assert client.authenticate("admin", "wrong") is False

        assert client.auth_token is None

    def test_connection_error(self):
        client = self._client()

        with patch.object(
            client.session, "post", side_effect=requests.ConnectionError("down")
        ), patch("dashboard.utils.api_client.st"):
            assert client.authenticate("admin", "secret") is False


class TestSidebarQuickRanges:
    def test_relative_ranges(self):
        today = datetime.now().date()

        assert _quick_range_dates("Last 7 days") == (today - timedelta(days=7), today)
        assert _quick_range_dates("Last 30 days") == (today - timedelta(days=30), today)
        assert _quick_range_dates("Last 90 days") == (today - timedelta(days=90), today)

    def test_year_to_date(self):
        today = datetime.now().date()

        assert _quick_range_dates("Year to date") == (today.replace(month=1, day=1), today)

    def test_custom_has_no_preset(self):
        assert _quick_range_dates("Custom") is None
