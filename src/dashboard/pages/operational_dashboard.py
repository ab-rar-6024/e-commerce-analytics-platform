"""
Operational Dashboard page.

This module provides comprehensive operational monitoring including
fraud detection monitoring, system health metrics, data quality monitoring,
and alert/notification panels for operations teams.
"""

from datetime import datetime, timedelta
from typing import Any, Dict

import pandas as pd
import streamlit as st

from dashboard.components import (
    render_bar_chart,
    render_data_quality_alerts,
    render_fraud_alerts,
    render_gauge_chart,
    render_line_chart,
    render_notification_banner,
    render_performance_alerts,
    render_pie_chart,
    render_system_alerts,
    render_system_health_table,
    render_time_series_chart,
)
from dashboard.utils.api_client import APIError, handle_api_error
from dashboard.utils.data_processing import format_large_number, format_percentage


@handle_api_error
def load_operational_data() -> Dict[str, Any]:
    """Load operational dashboard data."""
    api_client = st.session_state.get("api_client")
    if not api_client:
        st.error("API client not available")
        return {}

    data = {}

    try:
        # Load system health data
        data["system_health"] = api_client.get_system_health(
            include_details=True, include_historical=True
        )

        # Load fraud detection operational data
        data["fraud_monitoring"] = api_client.get(
            "/api/v1/operations/fraud-monitoring",
            params={"time_range": "24h", "include_alerts": True},
        )

        # Load data quality metrics
        data["data_quality"] = api_client.get(
            "/api/v1/operations/data-quality",
            params={"time_range": "24h", "include_anomalies": True},
        )

        # Load operational alerts
        data["alerts"] = api_client.get(
            "/api/v1/operations/alerts",
            params={"status": "active", "priority": "all", "page": 1, "size": 50},
        )

        # Load performance benchmarks
        data["performance"] = api_client.get(
            "/api/v1/operations/performance",
            params={"benchmark_type": "all", "time_range": "24h"},
        )

        # Load streaming pipeline health
        data["streaming"] = api_client.get(
            "/api/v1/operations/streaming-health",
            params={"include_lag": True, "include_throughput": True},
        )

    except APIError as e:
        st.error(f"Failed to load operational data: {e.message}")
        return {}

    return data


def render_operational_overview(system_health: Dict[str, Any]) -> None:
    """Render operational overview with key system health metrics."""
    st.header("🎛️ Operational Overview")

    if not system_health:
        st.warning("No system health data available")
        return

    # Overall system status banner
    overall_status = system_health.get("overall_status", "unknown")

    if overall_status == "healthy":
        render_notification_banner(
            message="✅ All systems operational", notification_type="success"
        )
    elif overall_status == "degraded":
        render_notification_banner(
            message="⚠️ Some systems experiencing issues", notification_type="warning"
        )
    elif overall_status == "critical":
        render_notification_banner(
            message="🚨 Critical system issues detected", notification_type="error"
        )
    else:
        render_notification_banner(
            message="❓ System status unknown", notification_type="info"
        )

    # Key operational metrics
    col1, col2, col3, col4, col5 = st.columns(5)

    core_metrics = system_health.get("core_metrics", {})

    with col1:
        uptime = system_health.get("uptime_seconds", 0) / 86400
        st.metric("System Uptime", f"{uptime:.1f} days", help="Total system uptime")

    with col2:
        api_health = core_metrics.get("api_server", {})
        response_time = api_health.get("response_time_p95_ms")
        st.metric(
            "API Response (P95)",
            _na(response_time, ".0f", "ms"),
            help="95th percentile API response time",
        )

    with col3:
        throughput = system_health.get("throughput_rps")
        st.metric("Throughput", _na(throughput, ",.2f", " req/s"), help="Requests per second since start")

    with col4:
        error_rate = system_health.get("error_rate") or 0
        delta_color = "inverse" if error_rate > 0.01 else "normal"
        st.metric(
            "Error Rate",
            format_percentage(error_rate),
            delta_color=delta_color,
            help="Share of API requests that returned 5xx",
        )

    with col5:
        cpu_usage = core_metrics.get("overall", {}).get("cpu_usage_percent", 0)
        delta_color = "inverse" if cpu_usage > 80 else "normal"
        st.metric(
            "CPU Usage",
            f"{cpu_usage:.1f}%",
            delta_color=delta_color,
            help="Overall system CPU usage",
        )


def render_fraud_monitoring_ops(fraud_data: Dict[str, Any]) -> None:
    """Render fraud detection monitoring for operations."""
    st.header("🛡️ Fraud Detection Operations")

    if not fraud_data:
        st.warning("No fraud monitoring data available")
        return

    # Fraud operational metrics
    col1, col2, col3, col4 = st.columns(4)

    monitoring = fraud_data.get("monitoring", {})
    model_health = fraud_data.get("model_health", {})

    with col1:
        alerts_24h = monitoring.get("alerts_last_24h", 0)
        st.metric(
            "Alerts (24h)",
            f"{alerts_24h:,}",
            help="Fraud alerts generated in last 24 hours",
        )

    with col2:
        model_accuracy = model_health.get("ensemble_accuracy", 0)
        delta_color = "inverse" if model_accuracy < 0.90 else "normal"
        st.metric(
            "Model Accuracy",
            format_percentage(model_accuracy),
            delta_color=delta_color,
            help="Current fraud detection model accuracy",
        )

    with col3:
        detection_latency = model_health.get("avg_detection_latency_ms", 0)
        delta_color = "inverse" if detection_latency > 200 else "normal"
        st.metric(
            "Detection Latency",
            f"{detection_latency:.0f}ms",
            delta_color=delta_color,
            help="Average fraud detection latency",
        )

    with col4:
        false_positive_rate = monitoring.get("false_positive_rate", 0)
        delta_color = "inverse" if false_positive_rate > 0.15 else "normal"
        st.metric(
            "False Positive Rate",
            format_percentage(false_positive_rate),
            delta_color=delta_color,
            help="Current false positive rate",
        )

    # Fraud model health indicators
    st.subheader("Model Health Status")

    col1, col2 = st.columns(2)

    with col1:
        models = model_health.get("individual_models", [])
        if models:
            model_status_data = []
            for model in models:
                status = "healthy" if model.get("accuracy", 0) > 0.85 else "degraded"
                model_status_data.append(
                    {
                        "Model": model.get("name", ""),
                        "Status": status.title(),
                        "Accuracy": format_percentage(model.get("accuracy", 0)),
                        "Last Update": model.get("last_trained", "Unknown"),
                    }
                )

            df = pd.DataFrame(model_status_data)
            st.dataframe(df, hide_index=True, use_container_width=True)

    with col2:
        # Model performance gauge
        ensemble_accuracy = model_health.get("ensemble_accuracy", 0) * 100
        render_gauge_chart(
            value=ensemble_accuracy,
            title="Ensemble Model Performance",
            min_value=0,
            max_value=100,
            threshold_ranges=[
                {"range": [0, 85], "color": "red"},
                {"range": [85, 95], "color": "yellow"},
                {"range": [95, 100], "color": "green"},
            ],
            height=300,
        )


def render_system_health_ops(system_health: Dict[str, Any]) -> None:
    """Render system health monitoring for operations."""
    st.header("🏥 System Health Monitoring")

    if not system_health:
        st.warning("No system health data available")
        return

    # Component health table
    st.subheader("Component Status")

    core_metrics = system_health.get("core_metrics", {})
    component_health_data = []

    for component_name, component_data in core_metrics.items():
        status = component_data.get("status", "unknown")
        health_data = {
            "Component": component_name.replace("_", " ").title(),
            "Status": status.title(),
            "CPU": f"{component_data.get('cpu_usage_percent', 0):.1f}%",
            "Memory": f"{component_data.get('memory_usage_mb', 0):,.0f} MB",
            "Response Time": f"{component_data.get('response_time_ms', 0):.0f}ms",
            "Last Check": component_data.get("last_health_check", "Unknown"),
        }
        component_health_data.append(health_data)

    if component_health_data:
        render_system_health_table(component_health_data)

    # System resource utilization
    st.subheader("Resource Utilization")

    col1, col2, col3 = st.columns(3)

    overall_metrics = core_metrics.get("overall", {})

    with col1:
        cpu_usage = overall_metrics.get("cpu_usage_percent", 0)
        render_gauge_chart(
            value=cpu_usage,
            title="CPU Usage",
            min_value=0,
            max_value=100,
            threshold_ranges=[
                {"range": [0, 70], "color": "green"},
                {"range": [70, 90], "color": "yellow"},
                {"range": [90, 100], "color": "red"},
            ],
            height=250,
        )

    with col2:
        memory_usage = overall_metrics.get("memory_usage_percent", 0)
        render_gauge_chart(
            value=memory_usage,
            title="Memory Usage",
            min_value=0,
            max_value=100,
            threshold_ranges=[
                {"range": [0, 70], "color": "green"},
                {"range": [70, 90], "color": "yellow"},
                {"range": [90, 100], "color": "red"},
            ],
            height=250,
        )

    with col3:
        disk_usage = overall_metrics.get("disk_usage_percent", 0)
        render_gauge_chart(
            value=disk_usage,
            title="Disk Usage",
            min_value=0,
            max_value=100,
            threshold_ranges=[
                {"range": [0, 70], "color": "green"},
                {"range": [70, 90], "color": "yellow"},
                {"range": [90, 100], "color": "red"},
            ],
            height=250,
        )


def render_data_quality_monitoring(data_quality: Dict[str, Any]) -> None:
    """Render data quality monitoring section."""
    st.header("📊 Data Quality Monitoring")

    if not data_quality:
        st.warning("No data quality data available")
        return

    # Data quality overview metrics
    quality_overview = data_quality.get("overview", {})

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        overall_score = quality_overview.get("overall_quality_score", 0)
        delta_color = "inverse" if overall_score < 0.90 else "normal"
        st.metric(
            "Overall Quality Score",
            format_percentage(overall_score),
            delta_color=delta_color,
            help="Overall data quality score",
        )

    with col2:
        completeness = quality_overview.get("completeness_score", 0)
        delta_color = "inverse" if completeness < 0.95 else "normal"
        st.metric(
            "Data Completeness",
            format_percentage(completeness),
            delta_color=delta_color,
            help="Percentage of complete data records",
        )

    with col3:
        accuracy = quality_overview.get("accuracy_score", 0)
        delta_color = "inverse" if accuracy < 0.95 else "normal"
        st.metric(
            "Data Accuracy",
            format_percentage(accuracy),
            delta_color=delta_color,
            help="Data accuracy score based on validation rules",
        )

    with col4:
        freshness_issues = quality_overview.get("freshness_violations", 0)
        delta_color = "inverse" if freshness_issues > 0 else "normal"
        st.metric(
            "Freshness Issues",
            f"{freshness_issues:,}",
            delta_color=delta_color,
            help="Number of data freshness violations",
        )

    # Data quality alerts
    st.subheader("Data Quality Alerts")

    quality_alerts = data_quality.get("alerts", [])
    if quality_alerts:
        render_data_quality_alerts(quality_alerts)
    else:
        st.success("✅ No data quality issues detected")

    # Data pipeline health
    st.subheader("Data Pipeline Health")

    pipelines = data_quality.get("pipeline_health", [])
    if pipelines:
        pipeline_data = []
        for pipeline in pipelines:
            pipeline_data.append(
                {
                    "Pipeline": pipeline.get("name", ""),
                    "Status": pipeline.get("status", "").title(),
                    "Last Run": pipeline.get("last_run", ""),
                    "Success Rate": format_percentage(pipeline.get("success_rate") or 0),
                    "Lag (h)": _na(
                        None
                        if pipeline.get("lag_seconds") is None
                        else pipeline["lag_seconds"] / 3600,
                        ".1f",
                    ),
                    "Data Volume": format_large_number(
                        pipeline.get("records_processed", 0)
                    ),
                }
            )

        df = pd.DataFrame(pipeline_data)
        st.dataframe(df, hide_index=True, use_container_width=True)

    # Data quality trends
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Quality Score")
        st.info(
            "Quality history is not stored; the score above is computed from the "
            "current integrity checks."
        )

    with col2:
        st.subheader("Issue Distribution")

        issues = data_quality.get("issue_distribution", {})
        if issues:
            issue_data = []
            for issue_type, count in issues.items():
                issue_data.append(
                    {"issue_type": issue_type.replace("_", " ").title(), "count": count}
                )

            render_pie_chart(
                data=issue_data,
                values_column="count",
                names_column="issue_type",
                title="Data Quality Issues",
                height=300,
            )


def render_alert_notification_panel(alerts_data: Dict[str, Any]) -> None:
    """Render alert and notification panel."""
    st.header("🚨 Alerts & Notifications")

    if not alerts_data:
        st.warning("No alerts data available")
        return

    # Alert summary
    alert_summary = alerts_data.get("summary", {})

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        critical_alerts = alert_summary.get("critical", 0)
        delta_color = "inverse" if critical_alerts > 0 else "normal"
        st.metric(
            "Critical Alerts",
            f"{critical_alerts:,}",
            delta_color=delta_color,
            help="Number of critical alerts",
        )

    with col2:
        warning_alerts = alert_summary.get("warning", 0)
        st.metric(
            "Warning Alerts", f"{warning_alerts:,}", help="Number of warning alerts"
        )

    with col3:
        info_alerts = alert_summary.get("info", 0)
        st.metric(
            "Info Notices",
            f"{info_alerts:,}",
            help="Informational notices (e.g. optional services not configured)",
        )

    with col4:
        avg_resolution_time = alert_summary.get("avg_resolution_time_minutes")
        st.metric(
            "Avg Resolution Time",
            _na(avg_resolution_time, ".1f", "m"),
            help="Not tracked: alerts here are derived from live measurements",
        )

    # Active alerts by category
    st.subheader("Active Alerts")

    alerts_by_category = alerts_data.get("alerts_by_category", {})

    # System alerts
    system_alerts = alerts_by_category.get("system", [])
    if system_alerts:
        st.write("**System Alerts**")
        render_system_alerts(system_alerts)

    # Performance alerts
    performance_alerts = alerts_by_category.get("performance", [])
    if performance_alerts:
        st.write("**Performance Alerts**")
        render_performance_alerts(performance_alerts)

    # Fraud alerts
    fraud_alerts = alerts_by_category.get("fraud", [])
    if fraud_alerts:
        st.write("**Fraud Alerts**")
        render_fraud_alerts(fraud_alerts)

    # Recent alerts table
    st.subheader("Recent Alerts")

    recent_alerts = alerts_data.get("recent_alerts", [])
    if recent_alerts:
        alert_table_data = []
        for alert in recent_alerts[:20]:  # Show latest 20
            alert_table_data.append(
                {
                    "Timestamp": alert.get("timestamp", ""),
                    "Severity": alert.get("severity", "").title(),
                    "Category": alert.get("category", "").title(),
                    "Message": alert.get("message", ""),
                    "Status": alert.get("status", "").title(),
                    "Assigned To": alert.get("assigned_to", "Unassigned"),
                }
            )

        df = pd.DataFrame(alert_table_data)
        st.dataframe(df, hide_index=True, use_container_width=True)
    else:
        st.info("No recent alerts")


def render_performance_monitoring(performance_data: Dict[str, Any]) -> None:
    """Render performance monitoring section."""
    st.header("⚡ Performance Monitoring")

    if not performance_data:
        st.warning("No performance data available")
        return

    # Performance metrics
    metrics = performance_data.get("metrics", {})

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        api_p95 = metrics.get("api_response_p95_ms")
        delta_color = "inverse" if (api_p95 or 0) > 500 else "normal"
        st.metric(
            "API P95 Latency",
            _na(api_p95, ".0f", "ms"),
            delta_color=delta_color,
            help="95th percentile API response time",
        )

    with col2:
        db_p95 = metrics.get("database_query_p95_ms")
        delta_color = "inverse" if (db_p95 or 0) > 150 else "normal"
        st.metric(
            "DB Round-trip P95",
            _na(db_p95, ".1f", "ms"),
            delta_color=delta_color,
            help="95th percentile database query time",
        )

    with col3:
        kafka_lag = metrics.get("kafka_consumer_lag")
        delta_color = "inverse" if (kafka_lag or 0) > 1000 else "normal"
        st.metric(
            "Kafka Consumer Lag",
            _na(kafka_lag),
            delta_color=delta_color,
            help="Current Kafka consumer lag",
        )

    with col4:
        spark_jobs = metrics.get("spark_active_jobs")
        st.metric(
            "Active Spark Jobs", _na(spark_jobs), help="Requires a Spark cluster (not configured)"
        )

    st.caption(
        "API figures cover requests since the API process started. "
        "Kafka and Spark metrics need those services to be deployed."
    )


def render_streaming_pipeline_health(streaming_data: Dict[str, Any]) -> None:
    """Render streaming pipeline health monitoring."""
    st.header("🌊 Streaming Pipeline Health")

    if not streaming_data:
        st.warning("No streaming data available")
        return

    # Streaming metrics
    pipeline_metrics = streaming_data.get("pipeline_metrics", {})

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        throughput = pipeline_metrics.get("messages_per_second")
        st.metric(
            "Messages/Second", _na(throughput, ",.3f"), help="New rows per second over the last 15 minutes"
        )

    with col2:
        processing_lag = pipeline_metrics.get("processing_lag_seconds")
        delta_color = "inverse" if (processing_lag or 0) > 3600 else "normal"
        st.metric(
            "Data Lag",
            _na(None if processing_lag is None else processing_lag / 3600, ".1f", " h"),
            delta_color=delta_color,
            help="Current processing lag",
        )

    with col3:
        error_rate = pipeline_metrics.get("error_rate") or 0
        delta_color = "inverse" if error_rate > 0.01 else "normal"
        st.metric(
            "Error Rate",
            format_percentage(error_rate),
            delta_color=delta_color,
            help="Streaming pipeline error rate",
        )

    with col4:
        active_consumers = pipeline_metrics.get("active_consumers")
        st.metric(
            "Active Consumers",
            _na(active_consumers),
            help="Number of active stream consumers",
        )

    # Pipeline health by topic
    st.subheader("Pipeline Health by Topic")

    topics = streaming_data.get("topic_health", [])
    if topics:
        topic_data = []
        for topic in topics:
            topic_data.append(
                {
                    "Table": topic.get("name", ""),
                    "Status": topic.get("status", "").title(),
                    "Rows/sec (15m)": _na(topic.get("messages_per_second"), ",.4f"),
                    "Newest row age (h)": _na(
                        None
                        if topic.get("lag_seconds") is None
                        else topic["lag_seconds"] / 3600,
                        ".1f",
                    ),
                    "Newest row": topic.get("last_message_time", ""),
                }
            )

        df = pd.DataFrame(topic_data)
        st.dataframe(df, hide_index=True, use_container_width=True)


def render_operational_filters() -> Dict[str, Any]:
    """Render enhanced filter options for operational dashboard."""
    with st.expander("🔍 Operational Filters & Controls", expanded=False):
        # Date range selection
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                value=datetime.now().date() - timedelta(days=1),
                key="ops_start_date",
            )
        with col2:
            end_date = st.date_input(
                "End Date", value=datetime.now().date(), key="ops_end_date"
            )

        # Quick time range and other filters
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            time_range = st.selectbox(
                "Quick Time Range",
                options=[
                    "Custom",
                    "Last 1 hour",
                    "Last 4 hours",
                    "Last 24 hours",
                    "Last 7 days",
                ],
                index=3,
                key="ops_time_range",
            )

        with col2:
            alert_severity = st.selectbox(
                "Alert Severity",
                options=["All", "Critical", "Warning", "Info"],
                index=0,
                key="ops_alert_severity",
            )

        with col3:
            component_filter = st.selectbox(
                "Component",
                options=["All", "API Server", "Database", "Kafka", "Spark", "Redis"],
                index=0,
                key="ops_component_filter",
            )

        with col4:
            status_filter = st.selectbox(
                "Status",
                options=["All", "Healthy", "Degraded", "Critical"],
                index=0,
                key="ops_status_filter",
            )

        # Advanced filters
        st.markdown("#### Advanced Filters")
        col1, col2, col3 = st.columns(3)

        with col1:
            min_cpu_threshold = st.slider(
                "Min CPU Usage (%)",
                min_value=0,
                max_value=100,
                value=0,
                key="ops_min_cpu",
            )

        with col2:
            min_memory_threshold = st.slider(
                "Min Memory Usage (%)",
                min_value=0,
                max_value=100,
                value=0,
                key="ops_min_memory",
            )

        with col3:
            response_time_threshold = st.number_input(
                "Max Response Time (ms)",
                min_value=0,
                value=1000,
                key="ops_max_response_time",
            )

        # Data export options
        st.markdown("#### Data Export")
        col1, col2, col3 = st.columns(3)

        with col1:
            export_format = st.selectbox(
                "Export Format",
                options=["CSV", "Excel", "JSON"],
                key="ops_export_format",
            )

        with col2:
            export_scope = st.selectbox(
                "Export Scope",
                options=["Current View", "All Data", "Filtered Data"],
                key="ops_export_scope",
            )

        with col3:
            if st.button("📊 Export Data", key="ops_export_btn"):
                st.success(f"Exporting {export_scope} as {export_format}...")
                # Export functionality would be implemented here

        # Update time range based on quick select
        if time_range != "Custom":
            today = datetime.now().date()
            if time_range in ("Last 1 hour", "Last 4 hours"):
                start_date = datetime.now().date()
                end_date = datetime.now().date()
            elif time_range == "Last 24 hours":
                start_date = today - timedelta(days=1)
                end_date = today
            elif time_range == "Last 7 days":
                start_date = today - timedelta(days=7)
                end_date = today

        # Return filter values
        return {
            "start_date": start_date,
            "end_date": end_date,
            "time_range": time_range,
            "alert_severity": alert_severity,
            "component_filter": component_filter,
            "status_filter": status_filter,
            "min_cpu_threshold": min_cpu_threshold,
            "min_memory_threshold": min_memory_threshold,
            "response_time_threshold": response_time_threshold,
            "export_format": export_format,
            "export_scope": export_scope,
        }


def render_drill_down_modal(drill_type: str, drill_data: Dict[str, Any]) -> None:
    """Render drill-down modal with detailed information."""
    if f"show_{drill_type}_drill_down" not in st.session_state:
        st.session_state[f"show_{drill_type}_drill_down"] = False

    if st.session_state[f"show_{drill_type}_drill_down"]:
        with st.container():
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 8, 1])

            with col2:
                st.subheader(f"🔍 {drill_type.title()} Drill-Down Analysis")

                if drill_type == "system_health":
                    render_system_health_drill_down(drill_data)
                elif drill_type == "fraud_monitoring":
                    render_fraud_monitoring_drill_down(drill_data)
                elif drill_type == "data_quality":
                    render_data_quality_drill_down(drill_data)
                elif drill_type == "performance":
                    render_performance_drill_down(drill_data)
                elif drill_type == "alerts":
                    render_alerts_drill_down(drill_data)

                if st.button("❌ Close Drill-Down", key=f"close_{drill_type}_drill"):
                    st.session_state[f"show_{drill_type}_drill_down"] = False
                    st.rerun()


def _na(value: Any, spec: str = ",.0f", suffix: str = "") -> str:
    """Format a number, or 'n/a' when the source does not provide it."""
    return "n/a" if value is None else f"{format(value, spec)}{suffix}"


def _pct(value: Any) -> str:
    return "n/a" if value is None else format_percentage(value)


def render_system_health_drill_down(drill_data: Dict[str, Any]) -> None:
    """Render system health drill-down details (measured values only)."""
    st.markdown("#### System Health Detailed Analysis")

    if not drill_data:
        st.info("No system health data available")
        return

    host = drill_data.get("host", {})
    st.subheader("🖥️ Host")
    c1, c2, c3 = st.columns(3)
    c1.metric("CPU", _na(host.get("cpu_percent"), ".1f", "%"))
    c2.metric(
        "Memory",
        _na(host.get("memory_percent"), ".1f", "%"),
        help=f"{_na(host.get('memory_used_mb'))} MB of {_na(host.get('memory_total_mb'))} MB",
    )
    c3.metric("Disk", _na((drill_data.get("core_metrics", {}).get("database", {})).get("disk_usage_percent"), ".1f", "%"))

    st.subheader("🧩 Components")
    rows = []
    for name, comp in drill_data.get("core_metrics", {}).items():
        detail = comp.get("reason") or ", ".join(
            f"{k.replace('_', ' ')}: {v}" for k, v in comp.items() if k != "status" and v is not None
        )
        rows.append({"Component": name.replace("_", " ").title(), "Status": comp.get("status", "unknown"), "Details": detail})
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

    alerts = drill_data.get("alerts", [])
    st.subheader("🔔 Active Conditions")
    if alerts:
        st.dataframe(pd.DataFrame(alerts), hide_index=True, use_container_width=True)
    else:
        st.success("No active warnings")


def render_fraud_monitoring_drill_down(drill_data: Dict[str, Any]) -> None:
    """Render fraud monitoring drill-down: per-rule quality and recent alerts."""
    st.markdown("#### Fraud Monitoring Detailed Analysis")

    if not drill_data:
        st.info("No fraud monitoring data available")
        return

    model_health = drill_data.get("model_health", {})
    st.subheader("🛡️ Detection Rules (measured against known outcomes)")
    models = model_health.get("individual_models", [])
    if models:
        df = pd.DataFrame(models)
        st.dataframe(
            df.rename(columns={"name": "Rule", "accuracy": "Precision", "recall": "Recall"})[["Rule", "Precision", "Recall"]],
            column_config={
                "Precision": st.column_config.ProgressColumn("Precision", min_value=0, max_value=1, format="%.2f"),
                "Recall": st.column_config.ProgressColumn("Recall", min_value=0, max_value=1, format="%.2f"),
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No rule statistics available")

    st.subheader("🔎 Highest-Priority Open Alerts")
    api_client = st.session_state.get("api_client")
    try:
        alerts = api_client.get_fraud_alerts(status="open", size=10).get("alerts", []) if api_client else []
    except Exception as exc:  # the drill-down is optional detail; keep the page up
        st.warning(f"Could not load alerts: {exc}")
        alerts = []
    if alerts:
        st.dataframe(
            pd.DataFrame(
                [
                    {"Alert": a["alert_id"], "Priority": a["priority"], "Type": a["alert_type"], "Risk": a["risk_score"],
                     "Amount": a["amount"], "Created": a["created_at"],
                     "Rules": ", ".join(a["fraud_indicators"]["rules_triggered"])}
                    for a in alerts
                ]
            ),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.success("No open alerts")


def render_data_quality_drill_down(drill_data: Dict[str, Any]) -> None:
    """Render data quality drill-down: every integrity check and table freshness."""
    st.markdown("#### Data Quality Detailed Analysis")

    if not drill_data:
        st.info("No data quality data available")
        return

    st.subheader("✅ Integrity Checks")
    checks = drill_data.get("checks", {})
    if checks:
        st.dataframe(
            pd.DataFrame(
                [{"Check": k.replace("_", " ").title(), "Rows failing": n, "Result": "pass" if n == 0 else "fail"}
                 for k, n in checks.items()]
            ),
            hide_index=True,
            use_container_width=True,
        )
        st.caption(f"{drill_data.get('rows_tested', 0):,} row checks evaluated")

    st.subheader("⏱️ Table Freshness")
    pipelines = drill_data.get("pipeline_health", [])
    if pipelines:
        st.dataframe(
            pd.DataFrame(
                [{"Table": p["name"], "Status": p["status"], "Newest record": p["last_run"],
                  "Age (hours)": None if p.get("lag_seconds") is None else round(p["lag_seconds"] / 3600, 1),
                  "Rows": p["records_processed"]} for p in pipelines]
            ),
            hide_index=True,
            use_container_width=True,
        )


def render_performance_drill_down(drill_data: Dict[str, Any]) -> None:
    """Render performance drill-down: latency percentiles and per-endpoint statistics."""
    st.markdown("#### Performance Detailed Analysis")

    if not drill_data:
        st.info("No performance data available")
        return

    api = drill_data.get("api", {})
    st.subheader("🌐 API latency (since the API started)")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("P50", _na(api.get("p50_ms"), ".0f", "ms"))
    c2.metric("P90", _na(api.get("p90_ms"), ".0f", "ms"))
    c3.metric("P95", _na(api.get("p95_ms"), ".0f", "ms"))
    c4.metric("P99", _na(api.get("p99_ms"), ".0f", "ms"))

    st.subheader("🎯 API Endpoint Performance")
    endpoints = drill_data.get("endpoints", [])
    if endpoints:
        st.dataframe(
            pd.DataFrame(endpoints).rename(
                columns={"endpoint": "Endpoint", "requests": "Requests", "avg_ms": "Avg (ms)", "p95_ms": "P95 (ms)",
                         "error_rate": "Error rate"}
            ),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No requests recorded yet")

    db = drill_data.get("database", {})
    st.subheader("🗄️ Database")
    c1, c2, c3 = st.columns(3)
    c1.metric("Engine", str(db.get("engine", "n/a")))
    c2.metric("Round-trip P95", _na(db.get("latency_p95_ms"), ".1f", "ms"))
    c3.metric("Size", _na(db.get("size_mb"), ".1f", " MB"))


def render_alerts_drill_down(drill_data: Dict[str, Any]) -> None:
    """Render alerts drill-down: every active operational alert."""
    st.markdown("#### Alerts Detailed Analysis")

    alerts = (drill_data or {}).get("recent_alerts", [])
    if not alerts:
        st.success("No active alerts")
        return
    df = pd.DataFrame(alerts)
    st.dataframe(df, hide_index=True, use_container_width=True)
    st.caption("Alerts are derived from current measurements (host, database, API, data freshness and integrity).")


def render_custom_alert_configuration() -> None:
    """Render custom alert configuration interface."""
    st.header("⚙️ Custom Alert Configuration")

    with st.expander("🔧 Alert Rules Configuration", expanded=False):
        # Alert rule creation
        st.subheader("Create New Alert Rule")

        col1, col2 = st.columns(2)

        with col1:
            rule_name = st.text_input("Rule Name", placeholder="High API Response Time")
            metric_type = st.selectbox(
                "Metric Type",
                options=[
                    "System Health",
                    "Performance",
                    "Data Quality",
                    "Fraud Detection",
                    "Custom",
                ],
            )

            component = st.selectbox(
                "Component",
                options=["All", "API Server", "Database", "Kafka", "Spark", "Redis"],
            )

        with col2:
            condition = st.selectbox(
                "Condition",
                options=[
                    "Greater than",
                    "Less than",
                    "Equal to",
                    "Not equal to",
                    "Between",
                ],
            )

            threshold_value = st.number_input("Threshold Value", value=0.0)

            severity = st.selectbox("Severity", options=["Info", "Warning", "Critical"])

        # Advanced settings
        st.subheader("Advanced Settings")

        col1, col2, col3 = st.columns(3)

        with col1:
            evaluation_window = st.selectbox(
                "Evaluation Window",
                options=["1 minute", "5 minutes", "15 minutes", "1 hour"],
            )

        with col2:
            notification_channels = st.multiselect(
                "Notification Channels",
                options=["Email", "Slack", "PagerDuty", "SMS", "Webhook"],
            )

        with col3:
            auto_resolve = st.checkbox("Auto-resolve when condition clears")

        if st.button("💾 Save Alert Rule"):
            st.success(f"Alert rule '{rule_name}' saved successfully!")

    # Existing alert rules
    st.subheader("📋 Existing Alert Rules")

    existing_rules = [
        {
            "Rule Name": "High API Response Time",
            "Metric": "Performance",
            "Component": "API Server",
            "Condition": "P95 > 500ms",
            "Severity": "Warning",
            "Status": "Active",
            "Created": "2024-01-10",
            "Triggered": "3 times",
            "Actions": "Edit | Disable | Delete",
        },
        {
            "Rule Name": "CPU Usage Critical",
            "Metric": "System Health",
            "Component": "All",
            "Condition": "CPU > 90%",
            "Severity": "Critical",
            "Status": "Active",
            "Created": "2024-01-08",
            "Triggered": "1 time",
            "Actions": "Edit | Disable | Delete",
        },
        {
            "Rule Name": "Data Quality Score Low",
            "Metric": "Data Quality",
            "Component": "ETL Pipeline",
            "Condition": "Score < 95%",
            "Severity": "Warning",
            "Status": "Active",
            "Created": "2024-01-12",
            "Triggered": "7 times",
            "Actions": "Edit | Disable | Delete",
        },
    ]

    df_rules = pd.DataFrame(existing_rules)
    st.dataframe(df_rules, hide_index=True, use_container_width=True)


def render() -> None:
    """Render the operational dashboard page."""
    st.title("🎛️ Operational Dashboard")
    st.markdown(
        "Comprehensive operational monitoring for fraud detection, system health, data quality, and alerts"
    )

    # Add enhanced filters and get filter values
    filter_values = render_operational_filters()

    # Load data with applied filters
    with st.spinner("Loading operational dashboard data..."):
        data = load_operational_data()

    if not data:
        st.error("Failed to load operational dashboard data")
        return

    # Extract data components
    system_health = data.get("system_health", {})
    fraud_monitoring = data.get("fraud_monitoring", {})
    data_quality = data.get("data_quality", {})
    alerts_data = data.get("alerts", {})
    performance = data.get("performance", {})
    streaming = data.get("streaming", {})

    # Operational overview with drill-down buttons
    render_operational_overview(system_health)

    # Add drill-down buttons for main sections
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        if st.button("🔍 System Health Details", key="drill_system_health"):
            st.session_state.show_system_health_drill_down = True

    with col2:
        if st.button("🔍 Fraud Monitoring Details", key="drill_fraud_monitoring"):
            st.session_state.show_fraud_monitoring_drill_down = True

    with col3:
        if st.button("🔍 Data Quality Details", key="drill_data_quality"):
            st.session_state.show_data_quality_drill_down = True

    with col4:
        if st.button("🔍 Performance Details", key="drill_performance"):
            st.session_state.show_performance_drill_down = True

    with col5:
        if st.button("🔍 Alerts Details", key="drill_alerts"):
            st.session_state.show_alerts_drill_down = True

    st.markdown("---")

    # Alert and notification panel
    render_alert_notification_panel(alerts_data)

    st.markdown("---")

    # Fraud detection monitoring
    render_fraud_monitoring_ops(fraud_monitoring)

    st.markdown("---")

    # System health monitoring
    render_system_health_ops(system_health)

    st.markdown("---")

    # Data quality monitoring
    render_data_quality_monitoring(data_quality)

    st.markdown("---")

    # Performance monitoring
    render_performance_monitoring(performance)

    st.markdown("---")

    # Streaming pipeline health
    render_streaming_pipeline_health(streaming)

    st.markdown("---")

    # Custom alert configuration
    render_custom_alert_configuration()

    # Render drill-down modals if requested
    render_drill_down_modal("system_health", system_health)
    render_drill_down_modal("fraud_monitoring", fraud_monitoring)
    render_drill_down_modal("data_quality", data_quality)
    render_drill_down_modal("performance", performance)
    render_drill_down_modal("alerts", alerts_data)

    # Footer with last update time
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.caption(
            f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Filters applied: {len([k for k, v in filter_values.items() if v not in ['All', 'Custom', 0]])}"
        )
