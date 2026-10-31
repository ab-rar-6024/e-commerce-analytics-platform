"""
API client for FastAPI integration.

This module provides a comprehensive API client for communicating with the
FastAPI backend, including authentication, error handling, and caching.
"""

import asyncio
import json
import time
from typing import Any, Dict, List, Optional, Union

import requests
import streamlit as st
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class APIError(Exception):
    """Custom exception for API errors."""

    def __init__(
        self, message: str, status_code: int = None, response_data: dict = None
    ):
        self.message = message
        self.status_code = status_code
        self.response_data = response_data
        super().__init__(self.message)


class APIClient:
    """
    API client for communicating with the FastAPI backend.

    Provides methods for authentication, data retrieval, error handling,
    and response caching.
    """

    def __init__(self, base_url: str, timeout: int = 30, retries: int = 3):
        """
        Initialize the API client.

        Args:
            base_url: Base URL of the API
            timeout: Request timeout in seconds
            retries: Number of retry attempts
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.session = self._create_session()
        self.auth_token = None
        self.token_expires_at = None

        # Cache for API responses
        self._cache = {}
        self._cache_ttl = 300  # 5 minutes default

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy."""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers including authentication."""
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        return headers

    def _is_token_valid(self) -> bool:
        """Check if the current token is valid."""
        if not self.auth_token or not self.token_expires_at:
            return False
        return time.time() < self.token_expires_at

    def _cache_key(self, endpoint: str, params: dict = None) -> str:
        """Generate cache key for endpoint and parameters."""
        key_parts = [endpoint]
        if params:
            # Sort params for consistent cache keys
            sorted_params = sorted(params.items())
            key_parts.append(json.dumps(sorted_params, sort_keys=True))
        return "|".join(key_parts)

    def _get_cached_response(self, cache_key: str) -> Optional[dict]:
        """Get cached response if still valid."""
        if cache_key in self._cache:
            cached_data, timestamp = self._cache[cache_key]
            if time.time() - timestamp < self._cache_ttl:
                return cached_data
            else:
                # Remove expired cache entry
                del self._cache[cache_key]
        return None

    def _cache_response(self, cache_key: str, data: dict):
        """Cache API response."""
        self._cache[cache_key] = (data, time.time())

    def authenticate(self, username: str, password: str) -> bool:
        """
        Authenticate with the API and store the access token.

        Args:
            username: Username for authentication
            password: Password for authentication

        Returns:
            bool: True if authentication successful
        """
        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/auth/login",
                json={"username": username, "password": password},
                timeout=self.timeout,
            )
            if response.status_code != 200:
                return False

            token = response.json()
            self.auth_token = token["access_token"]
            self.token_expires_at = time.time() + int(token.get("expires_in", 3600))
            return True

        except (requests.RequestException, ValueError, KeyError) as e:
            st.error(f"Authentication failed: {str(e)}")
            return False

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: dict = None,
        data: dict = None,
        use_cache: bool = True,
    ) -> dict:
        """
        Make HTTP request to the API.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            use_cache: Whether to use caching for GET requests

        Returns:
            dict: API response data

        Raises:
            APIError: If request fails
        """
        url = f"{self.base_url}{endpoint}"

        # Check cache for GET requests
        if method.upper() == "GET" and use_cache:
            cache_key = self._cache_key(endpoint, params)
            cached_response = self._get_cached_response(cache_key)
            if cached_response:
                return cached_response

        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=self._get_headers(),
                params=params,
                json=data,
                timeout=self.timeout,
            )

            # Handle different response status codes
            if response.status_code == 200:
                response_data = response.json()

                # Cache successful GET responses
                if method.upper() == "GET" and use_cache:
                    cache_key = self._cache_key(endpoint, params)
                    self._cache_response(cache_key, response_data)

                return response_data

            elif response.status_code == 401:
                # Token expired or invalid
                self.auth_token = None
                self.token_expires_at = None
                raise APIError("Authentication required", response.status_code)

            elif response.status_code == 404:
                raise APIError("Resource not found", response.status_code)

            elif response.status_code >= 500:
                raise APIError("Server error", response.status_code)

            else:
                error_data = response.json() if response.content else {}
                raise APIError(
                    f"Request failed: {response.status_code}",
                    response.status_code,
                    error_data,
                )

        except requests.exceptions.Timeout:
            raise APIError("Request timeout")
        except requests.exceptions.ConnectionError:
            raise APIError("Connection error")
        except requests.exceptions.RequestException as e:
            raise APIError(f"Request failed: {str(e)}")
        except json.JSONDecodeError:
            raise APIError("Invalid JSON response")

    def get(self, endpoint: str, params: dict = None, use_cache: bool = True) -> dict:
        """Make GET request."""
        return self._make_request("GET", endpoint, params=params, use_cache=use_cache)

    def post(self, endpoint: str, data: dict = None, params: dict = None) -> dict:
        """Make POST request."""
        return self._make_request(
            "POST", endpoint, params=params, data=data, use_cache=False
        )

    def put(self, endpoint: str, data: dict = None, params: dict = None) -> dict:
        """Make PUT request."""
        return self._make_request(
            "PUT", endpoint, params=params, data=data, use_cache=False
        )

    def delete(self, endpoint: str, params: dict = None) -> dict:
        """Make DELETE request."""
        return self._make_request("DELETE", endpoint, params=params, use_cache=False)

    # Convenience methods for specific API endpoints

    def get_activity(self, **kwargs) -> dict:
        """Get recent orders, hourly volume and regional split."""
        return self.get("/api/v1/realtime/activity", params=kwargs)

    def get_executive_overview(self, **kwargs) -> dict:
        """Get executive KPIs and breakdowns."""
        return self.get("/api/v1/analytics/executive", params=kwargs)

    def get_revenue_analytics(self, **kwargs) -> dict:
        """Get revenue analytics data."""
        return self.get("/api/v1/analytics/revenue", params=kwargs)

    def get_customer_segments(self, **kwargs) -> dict:
        """Get customer segmentation data."""
        return self.get("/api/v1/analytics/customers/segments", params=kwargs)

    def get_product_performance(self, **kwargs) -> dict:
        """Get product performance data."""
        return self.get("/api/v1/analytics/products/performance", params=kwargs)

    def get_realtime_metrics(self, **kwargs) -> dict:
        """Get real-time metrics."""
        return self.get("/api/v1/analytics/real-time/metrics", params=kwargs)

    def get_fraud_alerts(self, **kwargs) -> dict:
        """Get fraud alerts."""
        return self.get("/api/v1/fraud/alerts", params=kwargs)

    def get_fraud_dashboard(self, **kwargs) -> dict:
        """Get fraud dashboard data."""
        return self.get("/api/v1/fraud/dashboard", params=kwargs)

    def get_system_health(self, **kwargs) -> dict:
        """Get system health metrics."""
        return self.get("/api/v1/realtime/system-health", params=kwargs)

    def get_stream_metrics(self, **kwargs) -> dict:
        """Get streaming metrics."""
        return self.get("/api/v1/realtime/stream-metrics", params=kwargs)

    def get_business_metrics(self, **kwargs) -> dict:
        """Get business metrics."""
        return self.get("/api/v1/realtime/business-metrics", params=kwargs)

    def get_customers(self, **kwargs) -> dict:
        """Get customer list."""
        return self.get("/api/v1/customers/", params=kwargs)

    def get_customer_detail(self, customer_id: str, **kwargs) -> dict:
        """Get customer details."""
        return self.get(f"/api/v1/customers/{customer_id}", params=kwargs)

    def get_cohort_analysis(self, **kwargs) -> dict:
        """Get cohort analysis data."""
        return self.get("/api/v1/analytics/cohort-analysis", params=kwargs)

    def get_funnel_analysis(self, **kwargs) -> dict:
        """Get funnel analysis data."""
        return self.get("/api/v1/analytics/funnels", params=kwargs)

    def health_check(self) -> dict:
        """Check API health."""
        return self.get("/health", use_cache=False)

    def clear_cache(self):
        """Clear the response cache."""
        self._cache.clear()

    def set_cache_ttl(self, ttl: int):
        """Set cache time-to-live in seconds."""
        self._cache_ttl = ttl


# Utility functions for Streamlit integration


@st.cache_data(ttl=300)  # Cache for 5 minutes
def cached_api_call(client: APIClient, method_name: str, **kwargs):
    """
    Cached API call for Streamlit.

    Args:
        client: APIClient instance
        method_name: Name of the API method to call
        **kwargs: Arguments to pass to the API method

    Returns:
        API response data
    """
    method = getattr(client, method_name)
    return method(**kwargs)


def handle_api_error(func):
    """
    Decorator to handle API errors in Streamlit.

    Args:
        func: Function to wrap

    Returns:
        Wrapped function with error handling
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            if e.status_code == 401:
                st.error("Authentication required. Please log in again.")
                st.session_state.authenticated = False
                st.rerun()
            else:
                st.error(f"API Error: {e.message}")
                if e.response_data:
                    st.json(e.response_data)
            return None
        except Exception as e:
            st.error(f"Unexpected error: {str(e)}")
            return None

    return wrapper
