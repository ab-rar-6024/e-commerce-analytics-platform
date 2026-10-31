"""
Executive Dashboard page.

This module provides the executive dashboard with high-level business metrics,
KPIs, and strategic insights for business leadership.
"""

from datetime import datetime
from typing import Any, Dict

import pandas as pd
import streamlit as st

from dashboard.components import (
    render_alert_summary,
    render_bar_chart,
    render_customer_metrics_executive,
    render_executive_kpi_overview,
    render_geographic_map,
    render_kpi_row,
    render_metric_card,
    render_pie_chart,
    render_revenue_performance_executive,
    render_time_series_chart,
)
from dashboard.utils.api_client import APIError, handle_api_error
from dashboard.utils.data_processing import format_currency


@handle_api_error
def load_executive_data() -> Dict[str, Any]:
    """Load data for executive dashboard."""
    api_client = st.session_state.get("api_client")
    if not api_client:
        st.error("API client not available")
        return {}

    # Get filter values from session state
    start_date = st.session_state.get("filter_start_date")
    end_date = st.session_state.get("filter_end_date")
    region = st.session_state.get("filter_region")

    # Format dates for API
    start_date_str = start_date.strftime("%Y-%m-%d") if start_date else None
    end_date_str = end_date.strftime("%Y-%m-%d") if end_date else None

    data = {}

    try:
        # Load revenue analytics
        revenue_params = {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "granularity": "daily",
            "region": region,
        }
        data["revenue"] = api_client.get_revenue_analytics(**revenue_params)

        # Load customer segments
        data["segments"] = api_client.get_customer_segments(
            segment_type="rfm", include_predictions=True
        )

        # Load real-time business metrics
        data["business_metrics"] = api_client.get_business_metrics(
            metric_category="all", time_window="24h"
        )

        # Load fraud dashboard summary
        data["fraud_summary"] = api_client.get_fraud_dashboard(
            time_range="24h", include_predictions=False
        )

        # Load system health
        data["system_health"] = api_client.get_system_health(include_details=False)

        # Executive KPIs and breakdowns, computed from stored orders
        data.update(api_client.get_executive_overview(days=90))

    except APIError as e:
        st.error(f"Failed to load data: {e.message}")
        return {}

    return data


def render_executive_summary(data: Dict[str, Any]) -> None:
    """Render executive summary section."""
    st.header("📊 Executive Summary")

    # Key metrics row
    revenue_data = data.get("revenue", {})
    business_data = data.get("business_metrics", {})
    fraud_data = data.get("fraud_summary", {})

    if revenue_data and "summary" in revenue_data:
        summary = revenue_data["summary"]

        # Calculate period-over-period metrics
        current_revenue = summary.get("total_revenue", 0)
        growth_rate = summary.get("growth_rate", 0)

        metrics = [
            {
                "title": "Total Revenue",
                "value": current_revenue,
                "delta": growth_rate,
                "format_type": "currency",
                "help_text": "Total revenue for the selected period",
            },
            {
                "title": "Total Orders",
                "value": summary.get("total_orders", 0),
                "format_type": "large_number",
                "help_text": "Number of orders placed",
            },
            {
                "title": "Average Order Value",
                "value": summary.get("average_order_value", 0),
                "format_type": "currency",
                "help_text": "Average value per order",
            },
            {
                "title": "Profit Margin",
                "value": summary.get("profit_margin", 0),
                "format_type": "percentage",
                "help_text": "Overall profit margin",
            },
        ]

        render_kpi_row(metrics)

    # Business health indicators
    st.subheader("🎯 Business Health")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        # Revenue health
        if revenue_data and "forecast" in revenue_data:
            forecast = revenue_data["forecast"]
            trend = forecast.get("trend", "stable")

            if trend == "increasing":
                st.success("📈 Revenue Growing")
            elif trend == "decreasing":
                st.error("📉 Revenue Declining")
            else:
                st.info("➡️ Revenue Stable")

    with col2:
        # Customer health
        if business_data and "customer" in business_data:
            customer = business_data["customer"]
            active_users = customer.get("active_users", {}).get("current", 0)

            if active_users > 1500:
                st.success("👥 High Engagement")
            elif active_users > 800:
                st.warning("👥 Moderate Engagement")
            else:
                st.error("👥 Low Engagement")

    with col3:
        # Fraud health
        if fraud_data and "overview" in fraud_data:
            overview = fraud_data["overview"]
            fraud_rate = overview.get("fraud_rate", 0)

            if fraud_rate < 0.01:
                st.success("🛡️ Low Fraud Risk")
            elif fraud_rate < 0.02:
                st.warning("🛡️ Moderate Fraud Risk")
            else:
                st.error("🛡️ High Fraud Risk")

    with col4:
        # System health
        system_data = data.get("system_health", {})
        overall_status = system_data.get("overall_status", "unknown")

        if overall_status == "healthy":
            st.success("⚡ Systems Healthy")
        elif overall_status == "degraded":
            st.warning("⚡ Systems Degraded")
        else:
            st.error("⚡ Systems Critical")


def render_revenue_overview(revenue_data: Dict[str, Any]) -> None:
    """Render revenue overview section."""
    st.header("💰 Revenue Overview")

    if not revenue_data:
        st.warning("No revenue data available")
        return

    col1, col2 = st.columns([2, 1])

    with col1:
        # Revenue trend chart
        if "time_series" in revenue_data:
            time_series = revenue_data["time_series"]

            render_time_series_chart(
                data=time_series,
                date_column="date",
                value_columns=["revenue"],
                title="Revenue Trend",
                height=400,
            )

    with col2:
        # Revenue breakdown by category
        if "breakdown" in revenue_data:
            breakdown = revenue_data["breakdown"]
            category_data = breakdown.get("by_category", [])

            if category_data:
                render_pie_chart(
                    data=category_data,
                    values_column="revenue",
                    names_column="category",
                    title="Revenue by Category",
                    height=400,
                )

    # Revenue breakdown tables
    st.subheader("Revenue Breakdown")

    col1, col2, col3 = st.columns(3)

    with col1:
        if "breakdown" in revenue_data:
            category_data = revenue_data["breakdown"].get("by_category", [])
            if category_data:
                df = pd.DataFrame(category_data)
                st.dataframe(
                    df,
                    column_config={
                        "revenue": st.column_config.NumberColumn(
                            "Revenue", format="$%.2f"
                        ),
                        "percentage": st.column_config.NumberColumn(
                            "Percentage", format="%.1f%%"
                        ),
                    },
                    hide_index=True,
                    use_container_width=True,
                )

    with col2:
        if "breakdown" in revenue_data:
            region_data = revenue_data["breakdown"].get("by_region", [])
            if region_data:
                df = pd.DataFrame(region_data)
                st.dataframe(
                    df,
                    column_config={
                        "revenue": st.column_config.NumberColumn(
                            "Revenue", format="$%.2f"
                        ),
                        "percentage": st.column_config.NumberColumn(
                            "Percentage", format="%.1f%%"
                        ),
                    },
                    hide_index=True,
                    use_container_width=True,
                )

    with col3:
        if "breakdown" in revenue_data:
            channel_data = revenue_data["breakdown"].get("by_channel", [])
            if channel_data:
                df = pd.DataFrame(channel_data)
                st.dataframe(
                    df,
                    column_config={
                        "revenue": st.column_config.NumberColumn(
                            "Revenue", format="$%.2f"
                        ),
                        "percentage": st.column_config.NumberColumn(
                            "Percentage", format="%.1f%%"
                        ),
                    },
                    hide_index=True,
                    use_container_width=True,
                )


def render_customer_overview(segments_data: Dict[str, Any]) -> None:
    """Render customer overview section."""
    st.header("👥 Customer Overview")

    if not segments_data:
        st.warning("No customer data available")
        return

    col1, col2 = st.columns([1, 1])

    with col1:
        # Customer segments pie chart
        if "segments" in segments_data:
            segments = segments_data["segments"]

            # Prepare data for chart
            chart_data = [
                {
                    "segment": seg["name"],
                    "count": seg["count"],
                    "percentage": seg["percentage"],
                }
                for seg in segments
            ]

            render_pie_chart(
                data=chart_data,
                values_column="count",
                names_column="segment",
                title="Customer Segments",
                height=400,
            )

    with col2:
        # Top segments by CLV
        if "segments" in segments_data:
            segments = segments_data["segments"]

            # Sort by CLV
            clv_data = [
                {
                    "segment": seg["name"],
                    "avg_clv": seg["characteristics"]["avg_customer_lifetime_value"],
                    "count": seg["count"],
                }
                for seg in segments
            ]
            clv_data.sort(key=lambda x: x["avg_clv"], reverse=True)

            render_bar_chart(
                data=clv_data[:5],  # Top 5
                x_column="segment",
                y_column="avg_clv",
                title="Top Segments by CLV",
                height=400,
            )

    # Customer insights
    if "insights" in segments_data:
        insights = segments_data["insights"]

        st.subheader("Customer Insights")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "Top Segment",
                insights.get("top_segment", "N/A"),
                help="Largest customer segment by count",
            )

        with col2:
            st.metric(
                "Growth Opportunity",
                insights.get("growth_opportunity", "N/A"),
                help="Segment with highest growth potential",
            )

        with col3:
            at_risk_revenue = insights.get("at_risk_revenue", 0)
            st.metric(
                "At-Risk Revenue",
                format_currency(at_risk_revenue),
                help="Revenue from at-risk customers",
            )


def render_operational_metrics(
    business_data: Dict[str, Any], fraud_data: Dict[str, Any]
) -> None:
    """Render operational metrics section."""
    st.header("⚡ Operational Metrics")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Business Performance")

        if business_data:
            # Sales metrics
            if "sales" in business_data:
                sales = business_data["sales"]

                metrics = [
                    {
                        "title": "Current Revenue",
                        "value": sales.get("revenue", {}).get("current", 0),
                        "format_type": "currency",
                    },
                    {
                        "title": "Conversion Rate",
                        "value": sales.get("orders", {}).get("conversion_rate", 0),
                        "format_type": "percentage",
                    },
                ]

                for metric in metrics:
                    render_metric_card(**metric)

    with col2:
        st.subheader("Security & Fraud")

        if fraud_data and "overview" in fraud_data:
            overview = fraud_data["overview"]

            metrics = [
                {
                    "title": "Fraud Rate",
                    "value": overview.get("fraud_rate", 0),
                    "format_type": "percentage",
                    "delta_color": "inverse",
                },
                {
                    "title": "Amount Blocked",
                    "value": overview.get("total_blocked_amount", 0),
                    "format_type": "currency",
                },
            ]

            for metric in metrics:
                render_metric_card(**metric)


def render_forecasting_insights(revenue_data: Dict[str, Any]) -> None:
    """Render forecasting and insights section."""
    st.header("🔮 Forecasting & Insights")

    if not revenue_data or "forecast" not in revenue_data:
        st.warning("No forecasting data available")
        return

    forecast = revenue_data["forecast"]

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue Forecast")

        # Forecast metrics
        predicted_revenue = forecast.get("next_period_revenue", 0)
        confidence = forecast.get("confidence_interval", {})
        trend = forecast.get("trend", "stable")

        st.metric(
            "Next Period Revenue",
            format_currency(predicted_revenue),
            help="Predicted revenue for next period",
        )

        # Confidence interval
        lower = confidence.get("lower", 0)
        upper = confidence.get("upper", 0)

        st.write(
            f"**Confidence Interval:** {format_currency(lower)} - {format_currency(upper)}"
        )
        st.write(f"**Trend:** {trend.title()}")

        # Trend indicator
        if trend == "increasing":
            st.success("📈 Positive trend expected")
        elif trend == "decreasing":
            st.error("📉 Declining trend expected")
        else:
            st.info("➡️ Stable trend expected")

    with col2:
        st.subheader("Key Insights")

        # Business insights
        insights = [
            "Revenue growth is driven by increased customer acquisition",
            "Mobile channel showing strongest performance",
            "Electronics category leading revenue growth",
            "Customer retention rates improving month-over-month",
        ]

        for insight in insights:
            st.write(f"• {insight}")


def render() -> None:
    """Render the executive dashboard page."""
    st.title("📈 Executive Dashboard")
    st.markdown("High-level business metrics and strategic insights for executives")

    # Load data
    with st.spinner("Loading executive dashboard data..."):
        data = load_executive_data()

    if not data:
        st.error("Failed to load dashboard data")
        return

    # Show alerts
    render_alert_summary(
        system_data=data.get("system_health"),
        fraud_data=data.get("fraud_summary"),
        business_data=data.get("business_metrics"),
    )

    # Executive KPI Overview (NEW)
    if "executive_kpis" in data:
        render_executive_kpi_overview(data["executive_kpis"])
        st.markdown("---")

    # Executive summary (Enhanced)
    render_executive_summary(data)

    st.markdown("---")

    # Revenue & Sales Performance (NEW Executive Level)
    if "revenue_performance" in data:
        render_revenue_performance_executive(data["revenue_performance"])
        st.markdown("---")

    # Customer Analytics (NEW Executive Level)
    if "customer_analytics" in data:
        render_customer_metrics_executive(data["customer_analytics"])
        st.markdown("---")

    # Geographic Sales Maps (NEW)
    if "geographic_data" in data:
        st.header("🌍 Geographic Sales Performance")

        col1, col2 = st.columns(2)

        with col1:
            render_geographic_map(
                data=data["geographic_data"],
                location_column="location",
                value_column="revenue",
                title="Revenue by Country",
                map_type="choropleth",
                color_scale="Blues",
            )

        with col2:
            # Top countries table
            st.subheader("Top Countries by Revenue")
            geo_df = pd.DataFrame(data["geographic_data"])
            geo_df = geo_df.sort_values("revenue", ascending=False)

            st.dataframe(
                geo_df,
                column_config={
                    "country": st.column_config.TextColumn("Country"),
                    "revenue": st.column_config.NumberColumn("Revenue", format="$%.0f"),
                    "location": st.column_config.TextColumn("Code"),
                },
                hide_index=True,
                use_container_width=True,
            )

        st.markdown("---")

    # Revenue overview (Original)
    render_revenue_overview(data.get("revenue", {}))

    st.markdown("---")

    # Customer overview (Original)
    render_customer_overview(data.get("segments", {}))

    st.markdown("---")

    # Operational metrics (Original)
    render_operational_metrics(
        data.get("business_metrics", {}), data.get("fraud_summary", {})
    )

    st.markdown("---")

    # Forecasting insights (Original)
    render_forecasting_insights(data.get("revenue", {}))

    # Footer with last update time
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
