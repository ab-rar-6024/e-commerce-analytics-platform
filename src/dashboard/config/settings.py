"""
Dashboard configuration settings.

This module provides configuration management for the Streamlit dashboard,
including API endpoints, authentication, and display settings.
"""

import os
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class DashboardSettings:
    """Dashboard configuration settings."""

    # API Configuration
    api_base_url: str = "http://localhost:8000"
    api_timeout: int = 30
    api_retries: int = 3

    # Authentication
    auth_enabled: bool = True
    default_username: str = "admin"
    default_password: str = "admin123"
    # Account the dashboard uses to call the API (the API's development user)
    api_username: str = "admin"
    api_password: str = "secret"
    session_timeout: int = 3600  # 1 hour

    # Dashboard Configuration
    page_title: str = "E-Commerce Analytics Platform"
    page_icon: str = "📊"
    layout: str = "wide"

    # Refresh Settings
    default_refresh_interval: int = 30  # seconds
    max_refresh_interval: int = 300  # 5 minutes
    min_refresh_interval: int = 10  # 10 seconds

    # Data Display
    default_page_size: int = 20
    max_page_size: int = 100
    date_format: str = "%Y-%m-%d"
    datetime_format: str = "%Y-%m-%d %H:%M:%S"

    # Chart Configuration
    chart_height: int = 400
    chart_theme: str = "streamlit"
    color_palette: List[str] = None

    # Cache Settings
    cache_ttl: int = 300  # 5 minutes
    enable_caching: bool = True

    # Feature Flags
    enable_real_time: bool = True
    enable_export: bool = True
    enable_alerts: bool = True
    enable_dark_mode: bool = False

    def __post_init__(self):
        """Post-initialization to set defaults and environment overrides."""
        # Override with environment variables if available
        self.api_base_url = os.getenv("DASHBOARD_API_BASE_URL", self.api_base_url)
        self.api_timeout = int(os.getenv("DASHBOARD_API_TIMEOUT", self.api_timeout))

        # Authentication settings
        self.auth_enabled = (
            os.getenv("DASHBOARD_AUTH_ENABLED", "true").lower() == "true"
        )
        self.default_username = os.getenv("DASHBOARD_USERNAME", self.default_username)
        self.default_password = os.getenv("DASHBOARD_PASSWORD", self.default_password)
        self.api_username = os.getenv("DASHBOARD_API_USERNAME", self.api_username)
        self.api_password = os.getenv("DASHBOARD_API_PASSWORD", self.api_password)

        # Set default color palette if not provided
        if self.color_palette is None:
            self.color_palette = [
                "#1f77b4",
                "#ff7f0e",
                "#2ca02c",
                "#d62728",
                "#9467bd",
                "#8c564b",
                "#e377c2",
                "#7f7f7f",
                "#bcbd22",
                "#17becf",
            ]

    @property
    def api_endpoints(self) -> dict:
        """Get API endpoint URLs."""
        base = self.api_base_url.rstrip("/")
        return {
            # Analytics endpoints
            "analytics_revenue": f"{base}/api/v1/analytics/revenue",
            "analytics_segments": f"{base}/api/v1/analytics/customers/segments",
            "analytics_products": f"{base}/api/v1/analytics/products/performance",
            "analytics_realtime": f"{base}/api/v1/analytics/real-time/metrics",
            "analytics_cohort": f"{base}/api/v1/analytics/cohort-analysis",
            "analytics_funnel": f"{base}/api/v1/analytics/funnels",
            # Customer endpoints
            "customers_list": f"{base}/api/v1/customers/",
            "customers_detail": f"{base}/api/v1/customers/{{customer_id}}",
            "customers_recommendations": f"{base}/api/v1/customers/{{customer_id}}/recommendations",
            "customers_value_prediction": f"{base}/api/v1/customers/{{customer_id}}/value-prediction",
            # Fraud endpoints
            "fraud_alerts": f"{base}/api/v1/fraud/alerts",
            "fraud_dashboard": f"{base}/api/v1/fraud/dashboard",
            "fraud_models": f"{base}/api/v1/fraud/models",
            "fraud_cases": f"{base}/api/v1/fraud/cases",
            "fraud_risk_assessment": f"{base}/api/v1/fraud/risk-assessment",
            # Real-time endpoints
            "realtime_system_health": f"{base}/api/v1/realtime/system-health",
            "realtime_stream_metrics": f"{base}/api/v1/realtime/stream-metrics",
            "realtime_business_metrics": f"{base}/api/v1/realtime/business-metrics",
            "realtime_performance": f"{base}/api/v1/realtime/performance-benchmarks",
            # Authentication
            "auth_login": f"{base}/api/v1/auth/login",
            "auth_refresh": f"{base}/api/v1/auth/refresh",
            # Health check
            "health": f"{base}/health",
        }

    @property
    def pages(self) -> List[dict]:
        """Get dashboard page configuration."""
        return [
            {
                "name": "Executive Dashboard",
                "icon": "📈",
                "description": "High-level business metrics and KPIs",
                "enabled": True,
            },
            {
                "name": "Customer Analytics",
                "icon": "👥",
                "description": "Customer segmentation and lifetime value analysis",
                "enabled": True,
            },
            {
                "name": "Revenue Analytics",
                "icon": "💰",
                "description": "Revenue analysis and forecasting",
                "enabled": True,
            },
            {
                "name": "Fraud Detection",
                "icon": "🛡️",
                "description": "Fraud monitoring and case management",
                "enabled": True,
            },
            {
                "name": "Operational Dashboard",
                "icon": "🎛️",
                "description": "Operations monitoring with system health, data quality, and alerts",
                "enabled": True,
            },
            {
                "name": "Real-time Monitoring",
                "icon": "⚡",
                "description": "Live system metrics and performance",
                "enabled": self.enable_real_time,
            },
        ]

    def get_chart_config(self) -> dict:
        """Get default chart configuration."""
        return {
            "height": self.chart_height,
            "theme": self.chart_theme,
            "color_discrete_sequence": self.color_palette,
            "template": "plotly_white" if not self.enable_dark_mode else "plotly_dark",
        }

    def get_table_config(self) -> dict:
        """Get default table configuration."""
        return {
            "page_size": self.default_page_size,
            "height": 400,
            "use_container_width": True,
            "hide_index": True,
        }


# Global settings instance
_settings = None


def get_dashboard_settings() -> DashboardSettings:
    """Get the global dashboard settings instance."""
    global _settings
    if _settings is None:
        _settings = DashboardSettings()
    return _settings
