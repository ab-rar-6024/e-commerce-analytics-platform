"""
Real-time Monitoring Dashboard page.

This module provides real-time monitoring of transactions, system performance,
fraud detection, and error rates with live updates and interactive visualizations.
"""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from dashboard.components import (
    render_alert,
    render_bar_chart,
    render_gauge_chart,
    render_kpi_row,
    render_line_chart,
    render_metric_card,
    render_pie_chart,
    render_status_indicator,
    render_time_series_chart,
)
from dashboard.utils.api_client import APIError, handle_api_error
from dashboard.utils.data_processing import (
    format_currency,
    format_large_number,
    format_number,
    format_percentage,
)


@handle_api_error
def load_real_time_data() -> Dict[str, Any]:
    """Load real-time monitoring data."""
    api_client = st.session_state.get("api_client")
    if not api_client:
        st.error("API client not available")
        return {}

    data = {}

    try:
        # Load system health metrics
        data["system_health"] = api_client.get_system_health(include_details=True)

        # Load streaming metrics
        data["stream_metrics"] = api_client.get_stream_metrics(
            stream_type="all", time_window="5m"
        )

        # Load business metrics
        data["business_metrics"] = api_client.get_business_metrics(
            metric_category="all", time_window="1h"
        )

        # Load performance benchmarks
        data["performance"] = api_client.get(
            "/api/v1/realtime/performance-benchmarks",
            params={"benchmark_type": "all", "time_range": "24h"},
        )

        # Load fraud dashboard for alerts
        data["fraud_dashboard"] = api_client.get_fraud_dashboard(
            time_range="1h", include_predictions=False
        )

    except APIError as e:
        st.error(f"Failed to load real-time data: {e.message}")
        return {}

    return data


def render_live_transaction_feed(business_data: Dict[str, Any]) -> None:
    """Render live transaction feed with key metrics."""
    st.header("💳 Live Transaction Feed")

    if not business_data:
        st.warning("No transaction data available")
        return

    # Extract sales metrics
    sales = business_data.get("sales", {})
    orders = sales.get("orders", {})

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        current_revenue = sales.get("revenue", {}).get("current", 0)
        render_metric_card(
            title="Current Revenue",
            value=current_revenue,
            format_type="currency",
            help_text="Revenue in current period",
        )

    with col2:
        orders_count = orders.get("count", 0)
        render_metric_card(
            title="Orders",
            value=orders_count,
            format_type="large_number",
            help_text="Number of orders in current period",
        )

    with col3:
        conversion_rate = orders.get("conversion_rate", 0)
        render_metric_card(
            title="Conversion Rate",
            value=conversion_rate,
            format_type="percentage",
            help_text="Current conversion rate",
        )

    with col4:
        avg_order_value = sales.get("orders", {}).get("average_value", 0)
        render_metric_card(
            title="Avg Order Value",
            value=avg_order_value,
            format_type="currency",
            help_text="Average order value",
        )

    # Recent transactions table (simulated)
    st.subheader("Recent Transactions")

    # Generate sample transaction data
    transactions = []
    for i in range(10):
        transaction = {
            "Transaction ID": f"TRX{i+1:05d}",
            "Amount": round(current_revenue * (0.01 + 0.05 * (i % 5)), 2),
            "Status": "Completed" if i % 4 != 0 else "Pending",
            "Payment Method": [
                "Credit Card",
                "PayPal",
                "Bank Transfer",
                "Digital Wallet",
            ][i % 4],
            "Time": (datetime.now() - timedelta(minutes=i * 2)).strftime("%H:%M:%S"),
        }
        transactions.append(transaction)

    df = pd.DataFrame(transactions)

    st.dataframe(
        df,
        column_config={
            "Amount": st.column_config.NumberColumn("Amount", format="$%.2f")
        },
        hide_index=True,
        use_container_width=True,
    )


def render_fraud_detection_alerts(fraud_data: Dict[str, Any]) -> None:
    """Render fraud detection alerts and notifications."""
    st.header("🛡️ Fraud Detection Alerts")

    if not fraud_data:
        st.warning("No fraud data available")
        return

    # Fraud metrics
    overview = fraud_data.get("overview", {})

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        alert_stats = fraud_data.get("alerts")
        total_alerts = overview.get(
            "total_alerts",
            alert_stats.get("total", 0) if isinstance(alert_stats, dict) else 0,
        )
        render_metric_card(
            title="Total Alerts",
            value=total_alerts,
            format_type="number",
            help_text="Total fraud alerts generated",
        )

    with col2:
        fraud_rate = overview.get("fraud_rate", 0)
        render_metric_card(
            title="Fraud Rate",
            value=fraud_rate,
            format_type="percentage",
            delta_color="inverse",
            help_text="Percentage of transactions flagged as fraudulent",
        )

    with col3:
        blocked_amount = overview.get("total_blocked_amount", 0)
        render_metric_card(
            title="Amount Blocked",
            value=blocked_amount,
            format_type="currency",
            help_text="Total amount blocked due to fraud",
        )

    with col4:
        fp_rate = overview.get("false_positive_rate", 0)
        render_metric_card(
            title="False Positive Rate",
            value=fp_rate,
            format_type="percentage",
            delta_color="inverse",
            help_text="Rate of false positive alerts",
        )

    # Fraud alerts table
    st.subheader("Recent Fraud Alerts")

    # Extract alerts from fraud data; the dashboard endpoint only returns
    # aggregate counts (a dict), in which case there are no records to list
    alerts = fraud_data.get("alerts", [])
    if isinstance(alerts, dict):
        alerts = []

    if alerts:
        # Prepare alert data for table
        alert_table_data = []
        for alert in alerts[:10]:  # Show top 10
            row = {
                "Alert ID": alert.get("alert_id", ""),
                "Type": alert.get("alert_type", "").replace("_", " ").title(),
                "Priority": alert.get("priority", "").title(),
                "Risk Score": alert.get("risk_score", 0),
                "Amount": alert.get("amount", 0),
                "Status": alert.get("status", "").replace("_", " ").title(),
                "Created": alert.get("created_at", ""),
            }
            alert_table_data.append(row)

        df = pd.DataFrame(alert_table_data)

        st.dataframe(
            df,
            column_config={
                "Risk Score": st.column_config.NumberColumn(
                    "Risk Score", format="%.1f%%"
                ),
                "Amount": st.column_config.NumberColumn("Amount", format="$%.2f"),
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No individual alert records in this view (see Fraud Detection)")


def render_system_performance(
    system_data: Dict[str, Any], performance_data: Dict[str, Any]
) -> None:
    """Render system performance metrics."""
    st.header("⚡ System Performance")

    if not system_data:
        st.warning("No system data available")
        return

    # Core metrics
    core_metrics = system_data.get("core_metrics", {})
    api_server = core_metrics.get("api_server", {})
    database = core_metrics.get("database", {})
    kafka = core_metrics.get("kafka", {})

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        response_time = api_server.get("response_time_p95_ms", 0)
        render_metric_card(
            title="API Response Time",
            value=response_time,
            format_type="number",
            suffix="ms",
            help_text="95th percentile API response time",
        )

    with col2:
        throughput = system_data.get("throughput_rps", 0)
        render_metric_card(
            title="Throughput",
            value=throughput,
            format_type="large_number",
            suffix=" RPS",
            help_text="Requests per second",
        )

    with col3:
        error_rate = system_data.get("error_rate", 0)
        render_metric_card(
            title="Error Rate",
            value=error_rate,
            format_type="percentage",
            delta_color="inverse",
            help_text="System error rate",
        )

    with col4:
        uptime = system_data.get("uptime_seconds", 0) / 86400  # Convert to days
        render_metric_card(
            title="Uptime",
            value=uptime,
            format_type="number",
            suffix=" days",
            help_text="System uptime in days",
        )

    # Performance charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Component Health")

        # Component status indicators
        components = [
            {"name": "API Server", "status": api_server.get("status", "unknown")},
            {"name": "Database", "status": database.get("status", "unknown")},
            {"name": "Kafka", "status": kafka.get("status", "unknown")},
            {
                "name": "Redis",
                "status": core_metrics.get("redis", {}).get("status", "unknown"),
            },
        ]

        for component in components:
            render_status_indicator(
                status=component["status"], label=component["name"], show_text=True
            )

    with col2:
        st.subheader("Performance Metrics")

        if performance_data:
            latency = performance_data.get("latency", {})
            api_times = latency.get("api_response_times", {})

            # Response time gauge
            p95_time = api_times.get("p95_ms", 0)
            sla_target = api_times.get("sla_target_ms", 200)

            # Scale value to 0-100 for gauge
            gauge_value = min(100, max(0, (p95_time / sla_target) * 100))

            render_gauge_chart(
                value=gauge_value,
                title="API Response Time",
                min_value=0,
                max_value=100,
                threshold_ranges=[
                    {"range": [0, 70], "color": "green"},
                    {"range": [70, 90], "color": "yellow"},
                    {"range": [90, 100], "color": "red"},
                ],
                height=300,
            )


def render_error_rate_monitoring(performance_data: Dict[str, Any]) -> None:
    """Render error rate monitoring."""
    st.header("⚠️ Error Rate Monitoring")

    if not performance_data:
        st.warning("No performance data available")
        return

    reliability = performance_data.get("reliability", {})
    error_rates = reliability.get("error_rates", {})

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        http_4xx_rate = error_rates.get("http_4xx_rate", 0)
        render_metric_card(
            title="4xx Errors",
            value=http_4xx_rate,
            format_type="percentage",
            delta_color="inverse",
            help_text="Client error rate",
        )

    with col2:
        http_5xx_rate = error_rates.get("http_5xx_rate", 0)
        render_metric_card(
            title="5xx Errors",
            value=http_5xx_rate,
            format_type="percentage",
            delta_color="inverse",
            help_text="Server error rate",
        )

    with col3:
        timeout_rate = error_rates.get("timeout_rate", 0)
        render_metric_card(
            title="Timeout Rate",
            value=timeout_rate,
            format_type="percentage",
            delta_color="inverse",
            help_text="Request timeout rate",
        )

    with col4:
        db_error_rate = error_rates.get("database_error_rate", 0)
        render_metric_card(
            title="DB Errors",
            value=db_error_rate,
            format_type="percentage",
            delta_color="inverse",
            help_text="Database error rate",
        )

    # Error rate trend chart
    st.subheader("Error Rate Trends")

    # Generate sample time series data for error rates
    hours = list(range(24))
    error_data = []
    for hour in hours:
        error_data.append(
            {
                "hour": f"{hour:02d}:00",
                "4xx_errors": http_4xx_rate * (0.8 + 0.4 * (hour % 5)),
                "5xx_errors": http_5xx_rate * (0.7 + 0.5 * (hour % 4)),
                "timeouts": timeout_rate * (0.9 + 0.3 * (hour % 3)),
            }
        )

    render_line_chart(
        data=error_data,
        x_column="hour",
        y_column="4xx_errors",
        title="Error Rate Over Time",
        color_column=None,
        height=400,
    )


def render_transaction_volume_chart(business_data: Dict[str, Any]) -> None:
    """Render real-time charts showing transaction volume over time."""
    st.header("📈 Transaction Volume")

    if not business_data:
        st.warning("No business data available")
        return

    # Generate sample time series data for transaction volume
    hours = list(range(24))
    volume_data = []
    base_volume = 1000
    for hour in hours:
        volume_data.append(
            {
                "hour": f"{hour:02d}:00",
                "transactions": base_volume * (0.7 + 0.6 * (hour % 8) / 8),
            }
        )

    render_bar_chart(
        data=volume_data,
        x_column="hour",
        y_column="transactions",
        title="Transaction Volume by Hour",
        height=400,
    )


def render_geographical_distribution() -> None:
    """Render geographical distribution of transactions."""
    st.header("🌍 Geographical Distribution")

    # Sample geographical data
    geo_data = [
        {"region": "North America", "transactions": 45000, "percentage": 0.35},
        {"region": "Europe", "transactions": 32000, "percentage": 0.25},
        {"region": "Asia", "transactions": 28000, "percentage": 0.22},
        {"region": "South America", "transactions": 12000, "percentage": 0.09},
        {"region": "Africa", "transactions": 8000, "percentage": 0.06},
        {"region": "Oceania", "transactions": 3000, "percentage": 0.03},
    ]

    col1, col2 = st.columns(2)

    with col1:
        render_pie_chart(
            data=geo_data,
            values_column="transactions",
            names_column="region",
            title="Transactions by Region",
            height=400,
        )

    with col2:
        df = pd.DataFrame(geo_data)
        st.dataframe(
            df,
            column_config={
                "transactions": st.column_config.NumberColumn(
                    "Transactions", format="%d"
                ),
                "percentage": st.column_config.NumberColumn(
                    "Percentage", format="%.1f%%"
                ),
            },
            hide_index=True,
            use_container_width=True,
        )


def render_fraud_detection_rate(fraud_data: Dict[str, Any]) -> None:
    """Render fraud detection rate visualization."""
    st.header("🛡️ Fraud Detection Rate")

    if not fraud_data:
        st.warning("No fraud data available")
        return

    # Fraud metrics
    overview = fraud_data.get("overview", {})
    trends = fraud_data.get("trends", {})

    col1, col2 = st.columns(2)

    with col1:
        fraud_rate = overview.get("fraud_rate", 0)
        render_gauge_chart(
            value=fraud_rate * 100,  # Convert to percentage for gauge
            title="Current Fraud Rate",
            min_value=0,
            max_value=5,  # Max 5% fraud rate
            threshold_ranges=[
                {"range": [0, 1], "color": "green"},
                {"range": [1, 2], "color": "yellow"},
                {"range": [2, 5], "color": "red"},
            ],
            height=300,
        )

    with col2:
        # Generate sample trend data
        hours = list(range(24))
        trend_data = []
        base_rate = fraud_rate
        for hour in hours:
            trend_data.append(
                {
                    "hour": f"{hour:02d}:00",
                    "fraud_rate": base_rate * (0.8 + 0.4 * (hour % 6) / 6),
                }
            )

        render_line_chart(
            data=trend_data,
            x_column="hour",
            y_column="fraud_rate",
            title="Fraud Rate Trend",
            height=300,
        )


def render_system_health_indicators(system_data: Dict[str, Any]) -> None:
    """Render system health indicators."""
    st.header("🏥 System Health Indicators")

    if not system_data:
        st.warning("No system data available")
        return

    # Overall system status
    overall_status = system_data.get("overall_status", "unknown")

    col1, col2 = st.columns([1, 3])

    with col1:
        render_status_indicator(
            status=overall_status, label="Overall System Health", show_text=True
        )

    with col2:
        # System health metrics
        core_metrics = system_data.get("core_metrics", {})

        metrics_data = []
        for component_name, component_data in core_metrics.items():
            status = component_data.get("status", "unknown")
            metrics_data.append(
                {
                    "Component": component_name.title(),
                    "Status": status.title(),
                    "CPU Usage": f"{component_data.get('cpu_usage_percent', 0)}%"
                    if "cpu_usage_percent" in component_data
                    else "N/A",
                    "Memory Usage": f"{component_data.get('memory_usage_mb', 0)} MB"
                    if "memory_usage_mb" in component_data
                    else "N/A",
                }
            )

        df = pd.DataFrame(metrics_data)
        st.dataframe(df, hide_index=True, use_container_width=True)


def render_filters() -> None:
    """Render filter options for the real-time monitoring page."""
    with st.expander("🔍 Filters", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            # Time range filter
            time_range = st.selectbox(
                "Time Range",
                options=[
                    "Last 15 minutes",
                    "Last 1 hour",
                    "Last 4 hours",
                    "Last 24 hours",
                ],
                index=1,
            )

        with col2:
            # Transaction type filter
            transaction_type = st.selectbox(
                "Transaction Type",
                options=[
                    "All",
                    "Credit Card",
                    "Debit Card",
                    "Digital Wallet",
                    "Bank Transfer",
                ],
                index=0,
            )

        with col3:
            # Region filter
            region = st.selectbox(
                "Region",
                options=[
                    "All",
                    "North America",
                    "Europe",
                    "Asia",
                    "South America",
                    "Africa",
                    "Oceania",
                ],
                index=0,
            )

        # Store filter values in session state
        st.session_state.rt_time_range = time_range
        st.session_state.rt_transaction_type = transaction_type
        st.session_state.rt_region = region


def render() -> None:
    """Render the real-time monitoring dashboard page."""
    st.title("⚡ Real-time Monitoring")
    st.markdown("Live transaction monitoring, system performance, and fraud detection")

    # Add filters
    render_filters()

    # Add refresh controls
    col1, col2, col3 = st.columns([3, 1, 1])

    with col2:
        if st.button("🔄 Refresh Data", help="Refresh real-time data"):
            st.rerun()

    with col3:
        auto_refresh = st.checkbox(
            "Auto-refresh",
            value=st.session_state.get("auto_refresh", False),
            help="Automatically refresh data every 30 seconds",
        )
        st.session_state.auto_refresh = auto_refresh

    # Auto-refresh logic
    if st.session_state.auto_refresh:
        if "last_rt_refresh" not in st.session_state:
            st.session_state.last_rt_refresh = time.time()

        if (
            time.time() - st.session_state.last_rt_refresh > 30
        ):  # Refresh every 30 seconds
            st.session_state.last_rt_refresh = time.time()
            st.rerun()

    # Load data
    with st.spinner("Loading real-time monitoring data..."):
        data = load_real_time_data()

    if not data:
        st.error("Failed to load real-time monitoring data")
        return

    # Extract data components
    system_health = data.get("system_health", {})
    stream_metrics = data.get("stream_metrics", {})
    business_metrics = data.get("business_metrics", {})
    performance = data.get("performance", {})
    fraud_dashboard = data.get("fraud_dashboard", {})

    # Live transaction feed
    render_live_transaction_feed(business_metrics)

    st.markdown("---")

    # Fraud detection alerts
    render_fraud_detection_alerts(fraud_dashboard)

    st.markdown("---")

    # System performance
    render_system_performance(system_health, performance)

    st.markdown("---")

    # Error rate monitoring
    render_error_rate_monitoring(performance)

    st.markdown("---")

    # Transaction volume chart
    render_transaction_volume_chart(business_metrics)

    st.markdown("---")

    # Geographical distribution
    render_geographical_distribution()

    st.markdown("---")

    # Fraud detection rate
    render_fraud_detection_rate(fraud_dashboard)

    st.markdown("---")

    # System health indicators
    render_system_health_indicators(system_health)

    # Footer with last update time
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
