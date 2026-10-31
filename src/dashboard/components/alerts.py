"""
Alert components for notifications and status indicators.

This module provides reusable alert components for displaying
system notifications, warnings, and status messages.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import streamlit as st

from dashboard.utils.data_processing import format_currency, format_percentage


def render_alert(
    message: str,
    alert_type: str = "info",
    icon: Optional[str] = None,
    dismissible: bool = False,
    key: str = None,
) -> None:
    """
    Render a single alert message.

    Args:
        message: Alert message
        alert_type: Type of alert (success, info, warning, error)
        icon: Optional icon to display
        dismissible: Whether the alert can be dismissed
        key: Unique key for the component
    """
    # Map alert types to Streamlit functions
    alert_functions = {
        "success": st.success,
        "info": st.info,
        "warning": st.warning,
        "error": st.error,
    }

    # Add icon to message if provided
    display_message = f"{icon} {message}" if icon else message

    # Show the alert
    alert_func = alert_functions.get(alert_type, st.info)

    if dismissible and key:
        # Check if alert was dismissed
        dismiss_key = f"{key}_dismissed"
        if not st.session_state.get(dismiss_key, False):
            col1, col2 = st.columns([10, 1])
            with col1:
                alert_func(display_message)
            with col2:
                if st.button("✕", key=f"{key}_dismiss_btn", help="Dismiss"):
                    st.session_state[dismiss_key] = True
                    st.rerun()
    else:
        alert_func(display_message)


def render_system_alerts(system_data: Dict[str, Any]) -> None:
    """
    Render system health alerts.

    Args:
        system_data: System health data from API
    """
    if not system_data:
        return

    alerts = system_data.get("alerts", [])
    overall_status = system_data.get("overall_status", "healthy")

    # Show overall system status
    if overall_status == "critical":
        render_alert(
            "🚨 System is experiencing critical issues", "error", key="system_critical"
        )
    elif overall_status == "degraded":
        render_alert(
            "⚠️ System performance is degraded", "warning", key="system_degraded"
        )
    elif overall_status == "healthy":
        render_alert("✅ All systems operational", "success", key="system_healthy")

    # Show individual alerts
    for i, alert in enumerate(alerts):
        level = alert.get("level", "info")
        message = alert.get("message", "")
        component = alert.get("component", "")
        timestamp = alert.get("timestamp", "")

        alert_message = f"**{component.title()}**: {message}"
        if timestamp:
            try:
                alert_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                time_str = alert_time.strftime("%H:%M:%S")
                alert_message += f" (at {time_str})"
            except:
                pass

        render_alert(
            alert_message,
            "error" if level == "error" else "warning",
            key=f"system_alert_{i}",
        )


def _alert_counts_by_priority(alerts: Any) -> Dict[str, int]:
    """Count fraud alerts per priority.

    Accepts either a list of alert dicts or the API's aggregate dict
    (``{"total": ..., "by_priority": {"critical": 2, ...}}``).
    """
    if isinstance(alerts, dict):
        return alerts.get("by_priority", {})

    counts: Dict[str, int] = {}
    for alert in alerts or []:
        if isinstance(alert, dict):
            priority = alert.get("priority")
            counts[priority] = counts.get(priority, 0) + 1
    return counts


def render_fraud_alerts(fraud_data: Dict[str, Any]) -> None:
    """
    Render fraud detection alerts.

    Args:
        fraud_data: Fraud data from API
    """
    if not fraud_data:
        return

    # Get summary data (the API calls it "overview")
    summary = fraud_data.get("summary") or fraud_data.get("overview") or {}
    alerts = fraud_data.get("alerts", [])

    # High-level fraud alerts
    fraud_rate = summary.get("fraud_rate", 0)
    if fraud_rate > 0.02:  # > 2%
        render_alert(
            f"🚨 High fraud rate detected: {format_percentage(fraud_rate)}",
            "error",
            key="high_fraud_rate",
        )
    elif fraud_rate > 0.01:  # > 1%
        render_alert(
            f"⚠️ Elevated fraud rate: {format_percentage(fraud_rate)}",
            "warning",
            key="elevated_fraud_rate",
        )

    # False positive rate alert
    fp_rate = summary.get("false_positive_rate", 0)
    if fp_rate > 0.15:  # > 15%
        render_alert(
            f"⚠️ High false positive rate: {format_percentage(fp_rate)}",
            "warning",
            key="high_fp_rate",
        )

    # Critical priority alerts
    by_priority = _alert_counts_by_priority(alerts)

    critical_count = by_priority.get("critical", 0)
    if critical_count:
        render_alert(
            f"🚨 {critical_count} critical fraud alerts require immediate attention",
            "error",
            key="critical_fraud_alerts",
        )

    # High priority alerts
    high_count = by_priority.get("high", 0)
    if high_count:
        render_alert(
            f"⚠️ {high_count} high priority fraud alerts pending",
            "warning",
            key="high_fraud_alerts",
        )


def render_business_alerts(business_data: Dict[str, Any]) -> None:
    """
    Render business performance alerts.

    Args:
        business_data: Business metrics data from API
    """
    if not business_data:
        return

    # Revenue alerts
    if "sales" in business_data:
        sales = business_data["sales"]
        revenue = sales.get("revenue", {})

        current_revenue = revenue.get("current", 0)
        target_revenue = revenue.get("target", 0)
        growth_rate = revenue.get("growth_rate", 0)

        # Revenue vs target
        if target_revenue > 0:
            revenue_vs_target = current_revenue / target_revenue
            if revenue_vs_target < 0.8:  # < 80% of target
                render_alert(
                    f"📉 Revenue below target: {format_percentage(revenue_vs_target)} of goal",
                    "warning",
                    key="revenue_below_target",
                )
            elif revenue_vs_target > 1.2:  # > 120% of target
                render_alert(
                    f"🎉 Revenue exceeding target: {format_percentage(revenue_vs_target)} of goal",
                    "success",
                    key="revenue_exceeding_target",
                )

        # Negative growth
        if growth_rate < -0.1:  # < -10%
            render_alert(
                f"📉 Significant revenue decline: {format_percentage(growth_rate)}",
                "error",
                key="revenue_decline",
            )

    # Customer alerts
    if "customer" in business_data:
        customer = business_data["customer"]
        engagement = customer.get("engagement", {})

        bounce_rate = engagement.get("bounce_rate", 0)
        if bounce_rate > 0.6:  # > 60%
            render_alert(
                f"⚠️ High bounce rate: {format_percentage(bounce_rate)}",
                "warning",
                key="high_bounce_rate",
            )

    # Inventory alerts
    if "inventory" in business_data:
        inventory = business_data["inventory"]
        stock_levels = inventory.get("stock_levels", {})
        alerts_list = inventory.get("alerts", [])

        out_of_stock = stock_levels.get("out_of_stock", 0)
        if out_of_stock > 50:
            render_alert(
                f"📦 {out_of_stock} products out of stock",
                "warning",
                key="out_of_stock_alert",
            )

        # Show inventory alerts
        for i, alert in enumerate(alerts_list[:3]):  # Show top 3
            if alert.get("urgency") == "high":
                render_alert(
                    f"📦 Low stock: {alert.get('product_id', 'Unknown')} ({alert.get('current_stock', 0)} remaining)",
                    "warning",
                    key=f"inventory_alert_{i}",
                )


def render_performance_alerts(performance_data: Dict[str, Any]) -> None:
    """
    Render system performance alerts.

    Args:
        performance_data: Performance metrics data from API
    """
    if not performance_data:
        return

    # Latency alerts
    if "latency" in performance_data:
        latency = performance_data["latency"]
        api_times = latency.get("api_response_times", {})

        p95_time = api_times.get("p95_ms", 0)
        sla_target = api_times.get("sla_target_ms", 200)

        if p95_time > sla_target * 2:  # > 2x SLA
            render_alert(
                f"🐌 API response time critical: {p95_time}ms (SLA: {sla_target}ms)",
                "error",
                key="api_latency_critical",
            )
        elif p95_time > sla_target:  # > SLA
            render_alert(
                f"⚠️ API response time elevated: {p95_time}ms (SLA: {sla_target}ms)",
                "warning",
                key="api_latency_warning",
            )

    # Throughput alerts
    if "throughput" in performance_data:
        throughput = performance_data["throughput"]
        rps = throughput.get("requests_per_second", {})

        current_rps = rps.get("current", 0)
        capacity_limit = rps.get("capacity_limit", 5000)
        utilization = rps.get("utilization", 0)

        if utilization > 0.9:  # > 90%
            render_alert(
                f"🚨 High system utilization: {format_percentage(utilization)}",
                "error",
                key="high_utilization",
            )
        elif utilization > 0.8:  # > 80%
            render_alert(
                f"⚠️ Elevated system utilization: {format_percentage(utilization)}",
                "warning",
                key="elevated_utilization",
            )

    # Error rate alerts
    if "reliability" in performance_data:
        reliability = performance_data["reliability"]
        error_rates = reliability.get("error_rates", {})

        http_5xx_rate = error_rates.get("http_5xx_rate", 0)
        if http_5xx_rate > 0.01:  # > 1%
            render_alert(
                f"🚨 High server error rate: {format_percentage(http_5xx_rate)}",
                "error",
                key="high_error_rate",
            )
        elif http_5xx_rate > 0.005:  # > 0.5%
            render_alert(
                f"⚠️ Elevated server error rate: {format_percentage(http_5xx_rate)}",
                "warning",
                key="elevated_error_rate",
            )


def render_data_quality_alerts(stream_data: Dict[str, Any]) -> None:
    """
    Render data quality alerts.

    Args:
        stream_data: Streaming data quality metrics
    """
    if not stream_data or "data_quality" not in stream_data:
        return

    data_quality = stream_data["data_quality"]

    # Overall health
    overall_health = data_quality.get("overall_health", "good")
    if overall_health == "poor":
        render_alert("🚨 Data quality issues detected", "error", key="data_quality_poor")
    elif overall_health == "fair":
        render_alert("⚠️ Data quality degraded", "warning", key="data_quality_fair")

    # Anomaly detection
    anomaly_detection = data_quality.get("anomaly_detection", {})
    anomaly_rate = anomaly_detection.get("anomaly_rate", 0)

    if anomaly_rate > 0.05:  # > 5%
        render_alert(
            f"🚨 High anomaly rate: {format_percentage(anomaly_rate)}",
            "error",
            key="high_anomaly_rate",
        )
    elif anomaly_rate > 0.02:  # > 2%
        render_alert(
            f"⚠️ Elevated anomaly rate: {format_percentage(anomaly_rate)}",
            "warning",
            key="elevated_anomaly_rate",
        )

    # Validation results
    validation = data_quality.get("validation_results", {})
    success_rate = validation.get("validation_success_rate", 1.0)

    if success_rate < 0.95:  # < 95%
        render_alert(
            f"⚠️ Data validation issues: {format_percentage(success_rate)} success rate",
            "warning",
            key="validation_issues",
        )


def render_alert_summary(
    system_data: Dict[str, Any] = None,
    fraud_data: Dict[str, Any] = None,
    business_data: Dict[str, Any] = None,
    performance_data: Dict[str, Any] = None,
    stream_data: Dict[str, Any] = None,
) -> None:
    """
    Render a summary of all alerts.

    Args:
        system_data: System health data
        fraud_data: Fraud detection data
        business_data: Business metrics data
        performance_data: Performance metrics data
        stream_data: Streaming data quality metrics
    """
    st.subheader("🚨 System Alerts")

    # Count alerts by type
    alert_counts = {"critical": 0, "warning": 0, "info": 0}

    # Check each data source for alerts
    if system_data:
        render_system_alerts(system_data)

    if fraud_data:
        render_fraud_alerts(fraud_data)

    if business_data:
        render_business_alerts(business_data)

    if performance_data:
        render_performance_alerts(performance_data)

    if stream_data:
        render_data_quality_alerts(stream_data)


def render_notification_banner(
    message: str,
    notification_type: str = "info",
    auto_dismiss: bool = True,
    dismiss_after: int = 5,
    key: str = None,
) -> None:
    """
    Render a notification banner that can auto-dismiss.

    Args:
        message: Notification message
        notification_type: Type of notification
        auto_dismiss: Whether to auto-dismiss
        dismiss_after: Seconds after which to auto-dismiss
        key: Unique key for the component
    """
    if key and auto_dismiss:
        # Check if notification should be shown
        show_key = f"{key}_show"
        timestamp_key = f"{key}_timestamp"

        current_time = datetime.now().timestamp()

        # Initialize if not exists
        if show_key not in st.session_state:
            st.session_state[show_key] = True
            st.session_state[timestamp_key] = current_time

        # Check if should auto-dismiss
        if (
            st.session_state.get(show_key, True)
            and current_time - st.session_state.get(timestamp_key, 0) > dismiss_after
        ):
            st.session_state[show_key] = False

        # Show notification if not dismissed
        if st.session_state.get(show_key, True):
            render_alert(message, notification_type, dismissible=True, key=key)
    else:
        render_alert(message, notification_type, dismissible=True, key=key)


def render_status_indicator(
    status: str, label: str = "", show_text: bool = True
) -> None:
    """
    Render a status indicator.

    Args:
        status: Status value (healthy, warning, critical, etc.)
        label: Label to display
        show_text: Whether to show status text
    """
    # Map status to colors and icons
    status_config = {
        "healthy": {"color": "green", "icon": "🟢", "text": "Healthy"},
        "good": {"color": "green", "icon": "🟢", "text": "Good"},
        "warning": {"color": "orange", "icon": "🟡", "text": "Warning"},
        "degraded": {"color": "orange", "icon": "🟡", "text": "Degraded"},
        "critical": {"color": "red", "icon": "🔴", "text": "Critical"},
        "error": {"color": "red", "icon": "🔴", "text": "Error"},
        "unknown": {"color": "gray", "icon": "⚪", "text": "Unknown"},
    }

    config = status_config.get(status.lower(), status_config["unknown"])

    display_text = f"{config['icon']} {label}" if label else config["icon"]
    if show_text:
        display_text += f" {config['text']}"

    # Use appropriate Streamlit component based on status
    if status.lower() in ["critical", "error"]:
        st.error(display_text)
    elif status.lower() in ["warning", "degraded"]:
        st.warning(display_text)
    elif status.lower() in ["healthy", "good"]:
        st.success(display_text)
    else:
        st.info(display_text)
