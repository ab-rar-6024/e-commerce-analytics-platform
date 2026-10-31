"""
Fraud Detection Dashboard page.

This module provides comprehensive fraud detection monitoring including
real-time alerts, case management, model performance, and risk assessment.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from dashboard.components import (
    render_alert,
    render_bar_chart,
    render_data_table,
    render_fraud_alerts,
    render_fraud_alerts_table,
    render_fraud_metrics,
    render_gauge_chart,
    render_heatmap,
    render_kpi_row,
    render_line_chart,
    render_metric_card,
    render_pie_chart,
    render_status_indicator,
    render_time_series_chart,
)
from dashboard.utils.api_client import APIError, handle_api_error
from dashboard.utils.data_processing import (
    cached_data_processing,
    format_currency,
    format_percentage,
    prepare_alert_data,
)


@handle_api_error
def load_fraud_data() -> Dict[str, Any]:
    """Load data for fraud detection dashboard."""
    api_client = st.session_state.get("api_client")
    if not api_client:
        st.error("API client not available")
        return {}

    data = {}

    try:
        # Load fraud dashboard overview
        data["dashboard"] = api_client.get_fraud_dashboard(
            time_range="24h", include_predictions=True
        )

        # Load fraud alerts
        data["alerts"] = api_client.get_fraud_alerts(
            status=None,  # All statuses
            priority=None,  # All priorities
            page=1,
            size=50,
        )

        # Load fraud cases
        data["cases"] = api_client.get(
            "/api/v1/fraud/cases", params={"status": None, "page": 1, "size": 20}
        )

        # Load model performance
        data["models"] = api_client.get(
            "/api/v1/fraud/models",
            params={"model_type": "all", "time_range": "7d", "include_details": True},
        )

        # Load risk assessment
        data["risk_assessment"] = api_client.get(
            "/api/v1/fraud/risk-assessment",
            params={"assessment_type": "overall", "time_range": "30d"},
        )

    except APIError as e:
        st.error(f"Failed to load fraud data: {e.message}")
        return {}

    return data


def render_fraud_overview(dashboard_data: Dict[str, Any]) -> None:
    """Render fraud detection overview section."""
    st.header("🛡️ Fraud Detection Overview")

    if not dashboard_data:
        st.warning("No fraud dashboard data available")
        return

    # Fraud metrics
    render_fraud_metrics(dashboard_data)

    st.markdown("---")

    # Fraud health indicators
    col1, col2, col3, col4 = st.columns(4)

    overview = dashboard_data.get("overview", {})
    performance = dashboard_data.get("performance", {})

    with col1:
        fraud_rate = overview.get("fraud_rate", 0)
        if fraud_rate < 0.01:
            st.success("🟢 Low Fraud Rate")
        elif fraud_rate < 0.02:
            st.warning("🟡 Moderate Fraud Rate")
        else:
            st.error("🔴 High Fraud Rate")
        st.write(f"Rate: {format_percentage(fraud_rate)}")

    with col2:
        fp_rate = overview.get("false_positive_rate", 0)
        if fp_rate < 0.10:
            st.success("🟢 Low False Positives")
        elif fp_rate < 0.20:
            st.warning("🟡 Moderate False Positives")
        else:
            st.error("🔴 High False Positives")
        st.write(f"Rate: {format_percentage(fp_rate)}")

    with col3:
        model_accuracy = performance.get("model_accuracy", 0)
        if model_accuracy > 0.95:
            st.success("🟢 High Accuracy")
        elif model_accuracy > 0.90:
            st.warning("🟡 Good Accuracy")
        else:
            st.error("🔴 Low Accuracy")
        st.write(f"Accuracy: {format_percentage(model_accuracy)}")

    with col4:
        detection_latency = performance.get("detection_latency_ms", 0)
        if detection_latency < 100:
            st.success("🟢 Fast Detection")
        elif detection_latency < 200:
            st.warning("🟡 Moderate Latency")
        else:
            st.error("🔴 Slow Detection")
        st.write(f"Latency: {detection_latency}ms")


def render_real_time_alerts(alerts_data: Dict[str, Any]) -> None:
    """Render real-time fraud alerts section."""
    st.header("🚨 Real-time Fraud Alerts")

    if not alerts_data:
        st.warning("No alerts data available")
        return

    # Alert summary metrics
    summary = alerts_data.get("summary", {})

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_alerts = summary.get("total_alerts", 0)
        st.metric(
            "Total Alerts", f"{total_alerts:,}", help="Total fraud alerts in the system"
        )

    with col2:
        avg_risk_score = summary.get("average_risk_score", 0)
        st.metric(
            "Avg Risk Score",
            format_percentage(avg_risk_score),
            help="Average risk score of alerts",
        )

    with col3:
        amount_at_risk = summary.get("total_amount_at_risk", 0)
        st.metric(
            "Amount at Risk",
            format_currency(amount_at_risk),
            help="Total transaction amount at risk",
        )

    with col4:
        sla_performance = summary.get("sla_performance", {}).get(
            "on_time_resolution", 0
        )
        st.metric(
            "SLA Performance",
            format_percentage(sla_performance),
            help="Percentage of alerts resolved within SLA",
        )

    # Alert distribution charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Alerts by Priority")

        priority_dist = summary.get("priority_distribution", {})
        if priority_dist:
            priority_data = [
                {"priority": k.title(), "count": v} for k, v in priority_dist.items()
            ]

            render_pie_chart(
                data=priority_data,
                values_column="count",
                names_column="priority",
                title="Alert Distribution by Priority",
                height=350,
            )

    with col2:
        st.subheader("Alerts by Status")

        status_dist = summary.get("status_distribution", {})
        if status_dist:
            status_data = [
                {"status": k.replace("_", " ").title(), "count": v}
                for k, v in status_dist.items()
            ]

            render_bar_chart(
                data=status_data,
                x_column="status",
                y_column="count",
                title="Alert Distribution by Status",
                height=350,
            )

    # Recent alerts table
    st.subheader("Recent High-Priority Alerts")

    alerts = alerts_data.get("alerts", [])
    high_priority_alerts = [
        alert for alert in alerts if alert.get("priority") in ["critical", "high"]
    ][:10]

    if high_priority_alerts:
        # Prepare alert data for table
        alert_table_data = []
        for alert in high_priority_alerts:
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
        st.info("No high-priority alerts at this time")


def render_fraud_trends(dashboard_data: Dict[str, Any]) -> None:
    """Render fraud trends analysis."""
    st.header("📈 Fraud Trends Analysis")

    if not dashboard_data:
        st.warning("No trend data available")
        return

    trends = dashboard_data.get("trends", {})

    # Fraud rate over time
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Daily Fraud Rate Trend")

        daily_fraud_rate = trends.get("daily_fraud_rate", [])
        if daily_fraud_rate:
            render_line_chart(
                data=daily_fraud_rate,
                x_column="date",
                y_column="alert_rate",
                title="Share of Orders Flagged per Day",
                height=400,
            )

    with col2:
        st.subheader("Fraud Pattern Analysis")

        top_patterns = trends.get("top_fraud_patterns", [])
        if top_patterns:
            pattern_data = [
                {
                    "pattern": pattern["pattern"],
                    "frequency": pattern["frequency"],
                    "severity": pattern["severity"],
                }
                for pattern in top_patterns
            ]

            render_bar_chart(
                data=pattern_data,
                x_column="pattern",
                y_column="frequency",
                color_column="severity",
                title="Top Fraud Patterns",
                height=400,
            )

    # Hourly alert distribution
    st.subheader("Alert Distribution by Hour")

    hourly_alerts = trends.get("hourly_alerts", [])
    if hourly_alerts:
        render_bar_chart(
            data=hourly_alerts,
            x_column="hour",
            y_column="count",
            title="Fraud Alerts by Hour of Day",
            height=300,
        )


def render_model_performance(models_data: Dict[str, Any]) -> None:
    """Render fraud detection model performance."""
    st.header("🤖 Model Performance")

    if not models_data:
        st.warning("No model performance data available")
        return

    # Model performance metrics
    if "models" in models_data:
        models = models_data["models"]

        # Performance comparison
        st.subheader("Model Performance Comparison")

        model_comparison_data = []
        for model in models:
            row = {
                "Model": model.get("name", ""),
                "Type": model.get("type", ""),
                "Accuracy": model.get("accuracy", 0),
                "Precision": model.get("precision", 0),
                "Recall": model.get("recall", 0),
                "F1 Score": model.get("f1_score", 0),
                "Response Time": f"{model.get('avg_response_time_ms', 0)}ms",
            }
            model_comparison_data.append(row)

        df = pd.DataFrame(model_comparison_data)

        st.dataframe(
            df,
            column_config={
                "Accuracy": st.column_config.NumberColumn("Accuracy", format="%.1f%%"),
                "Precision": st.column_config.NumberColumn(
                    "Precision", format="%.1f%%"
                ),
                "Recall": st.column_config.NumberColumn("Recall", format="%.1f%%"),
                "F1 Score": st.column_config.NumberColumn("F1 Score", format="%.1f%%"),
            },
            hide_index=True,
            use_container_width=True,
        )

    # Ensemble performance
    if "ensemble_performance" in models_data:
        ensemble = models_data["ensemble_performance"]

        st.subheader("Ensemble Model Performance")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Combined Accuracy",
                format_percentage(ensemble.get("combined_accuracy", 0)),
                help="Overall ensemble model accuracy",
            )

        with col2:
            st.metric(
                "Combined Precision",
                format_percentage(ensemble.get("combined_precision", 0)),
                help="Overall ensemble model precision",
            )

        with col3:
            st.metric(
                "Combined Recall",
                format_percentage(ensemble.get("combined_recall", 0)),
                help="Overall ensemble model recall",
            )

        with col4:
            st.metric(
                "Model Agreement",
                format_percentage(ensemble.get("model_agreement_rate", 0)),
                help="Rate of agreement between models",
            )


def render_case_management(cases_data: Dict[str, Any]) -> None:
    """Render fraud case management section."""
    st.header("📋 Case Management")

    if not cases_data:
        st.warning("No case management data available")
        return

    # Case summary metrics
    summary = cases_data.get("summary", {})

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_cases = summary.get("total_cases", 0)
        st.metric(
            "Total Cases", f"{total_cases:,}", help="Total fraud investigation cases"
        )

    with col2:
        open_cases = summary.get("open_cases", 0)
        st.metric(
            "Open Cases",
            f"{open_cases:,}",
            help="Cases currently open for investigation",
        )

    with col3:
        avg_investigation_time = summary.get("avg_investigation_time_hours", 0)
        st.metric(
            "Avg Investigation Time",
            f"{avg_investigation_time:.1f}h",
            help="Average time to resolve cases",
        )

    with col4:
        sla_compliance = summary.get("sla_compliance_rate", 0)
        st.metric(
            "SLA Compliance",
            format_percentage(sla_compliance),
            help="Percentage of cases resolved within SLA",
        )

    # Case status distribution
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Cases by Status")

        # Cases group the alerts that are still being worked, so closed work is not shown
        case_status_data = [
            {"status": "Open", "count": open_cases},
            {"status": "In Progress", "count": summary.get("in_progress_cases", 0)},
            {"status": "Escalated", "count": summary.get("escalated_cases", 0)},
        ]

        render_pie_chart(
            data=case_status_data,
            values_column="count",
            names_column="status",
            title="Case Distribution by Status",
            height=350,
        )

    with col2:
        st.subheader("Investigation Performance")

        # Investigation metrics gauge
        render_gauge_chart(
            value=sla_compliance * 100,
            title="SLA Compliance Rate",
            min_value=0,
            max_value=100,
            threshold_ranges=[
                {"range": [0, 70], "color": "red"},
                {"range": [70, 90], "color": "yellow"},
                {"range": [90, 100], "color": "green"},
            ],
            height=350,
        )

    # Recent cases table
    st.subheader("Recent Cases")

    cases = cases_data.get("cases", [])
    if cases:
        # Prepare case data for table
        case_table_data = []
        for case in cases[:10]:  # Show top 10
            row = {
                "Case ID": case.get("case_id", ""),
                "Type": case.get("case_type", "").replace("_", " ").title(),
                "Priority": case.get("priority", "").title(),
                "Status": case.get("status", "").replace("_", " ").title(),
                "Amount": case.get("total_amount", 0),
                "Assigned To": case.get("assigned_to", "Unassigned"),
                "Created": case.get("created_at", ""),
            }
            case_table_data.append(row)

        df = pd.DataFrame(case_table_data)

        st.dataframe(
            df,
            column_config={
                "Amount": st.column_config.NumberColumn("Amount", format="$%.2f")
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No recent cases available")


def render_risk_assessment(risk_data: Dict[str, Any]) -> None:
    """Render risk assessment section."""
    st.header("⚠️ Risk Assessment")

    if not risk_data:
        st.warning("No risk assessment data available")
        return

    # Overall risk metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        overall_risk = risk_data.get("overall_risk_level", "unknown")
        risk_score = risk_data.get("risk_score", 0)

        if overall_risk == "low":
            st.success("🟢 Low Risk")
        elif overall_risk == "medium":
            st.warning("🟡 Medium Risk")
        elif overall_risk == "high":
            st.error("🔴 High Risk")
        else:
            st.info("⚪ Unknown Risk")

        st.write(f"Score: {format_percentage(risk_score)}")

    with col2:
        trend = risk_data.get("trend", "stable")
        if trend == "increasing":
            st.error("📈 Risk Increasing")
        elif trend == "decreasing":
            st.success("📉 Risk Decreasing")
        else:
            st.info("➡️ Risk Stable")

    with col3:
        projected_loss = risk_data.get("projected_loss", {}).get("next_30_days", 0)
        st.metric(
            "Projected Loss (30d)",
            format_currency(projected_loss),
            help="Projected loss in next 30 days",
        )

    with col4:
        risk_distribution = risk_data.get("risk_distribution", {})
        high_risk_pct = risk_distribution.get("high_risk", 0)
        st.metric(
            "High Risk %",
            format_percentage(high_risk_pct),
            help="Percentage of high-risk transactions",
        )

    # Risk factors analysis
    if "key_risk_factors" in risk_data:
        st.subheader("Key Risk Factors")

        risk_factors = risk_data["key_risk_factors"]
        factor_data = [
            {
                "factor": factor["factor"].replace("_", " ").title(),
                "impact": factor["impact"],
                "trend": factor["trend"],
            }
            for factor in risk_factors
        ]

        render_bar_chart(
            data=factor_data,
            x_column="factor",
            y_column="impact",
            color_column="trend",
            title="Risk Factor Impact Analysis",
            height=400,
        )


def render() -> None:
    """Render the fraud detection dashboard page."""
    st.title("🛡️ Fraud Detection")
    st.markdown("Comprehensive fraud monitoring, detection, and case management")

    # Load data
    with st.spinner("Loading fraud detection data..."):
        data = load_fraud_data()

    if not data:
        st.error("Failed to load fraud detection data")
        return

    # Show fraud alerts
    render_fraud_alerts(data.get("dashboard", {}))

    # Fraud overview
    render_fraud_overview(data.get("dashboard", {}))

    st.markdown("---")

    # Real-time alerts
    render_real_time_alerts(data.get("alerts", {}))

    st.markdown("---")

    # Fraud trends
    render_fraud_trends(data.get("dashboard", {}))

    st.markdown("---")

    # Model performance
    render_model_performance(data.get("models", {}))

    st.markdown("---")

    # Case management
    render_case_management(data.get("cases", {}))

    st.markdown("---")

    # Risk assessment
    render_risk_assessment(data.get("risk_assessment", {}))

    # Footer with last update time
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
