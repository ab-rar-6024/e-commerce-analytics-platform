"""
Revenue Analytics Dashboard page.

This module provides comprehensive revenue analysis including forecasting,
multi-dimensional breakdowns, profit analysis, and trend insights.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from dashboard.components import (
    render_bar_chart,
    render_donut_chart,
    render_dual_axis_chart,
    render_gauge_chart,
    render_kpi_row,
    render_line_chart,
    render_metric_card,
    render_pie_chart,
    render_revenue_breakdown_table,
    render_revenue_metrics,
    render_time_series_chart,
    render_waterfall_chart,
)
from dashboard.utils.api_client import APIError, handle_api_error
from dashboard.utils.data_processing import (
    calculate_kpi_metrics,
    format_currency,
    format_percentage,
    prepare_revenue_breakdown,
    process_time_series_data,
)


@handle_api_error
def load_revenue_data() -> Dict[str, Any]:
    """Load data for revenue analytics dashboard."""
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
        # Load revenue analytics with different granularities
        revenue_params = {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "region": region,
        }

        # Daily revenue data
        data["revenue_daily"] = api_client.get_revenue_analytics(
            granularity="daily", **revenue_params
        )

        # Weekly revenue data for trends
        data["revenue_weekly"] = api_client.get_revenue_analytics(
            granularity="weekly", **revenue_params
        )

        # Monthly revenue data for long-term analysis
        data["revenue_monthly"] = api_client.get_revenue_analytics(
            granularity="monthly", **revenue_params
        )

        # Product performance data
        data["products"] = api_client.get_product_performance(
            sort_by="revenue", time_range="30d", include_recommendations=True
        )

    except APIError as e:
        st.error(f"Failed to load revenue data: {e.message}")
        return {}

    return data


def render_revenue_overview(revenue_data: Dict[str, Any]) -> None:
    """Render revenue overview section."""
    st.header("💰 Revenue Overview")

    if not revenue_data:
        st.warning("No revenue data available")
        return

    # Revenue metrics
    render_revenue_metrics(revenue_data)

    st.markdown("---")

    # Revenue trend analysis
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Revenue Trend")

        if "time_series" in revenue_data:
            time_series = revenue_data["time_series"]

            # Create dual-axis chart with revenue and orders
            df = pd.DataFrame(time_series)
            if not df.empty:
                render_dual_axis_chart(
                    data=df,
                    x_column="date",
                    y1_column="revenue",
                    y2_column="orders",
                    title="Revenue and Orders Trend",
                    y1_title="Revenue ($)",
                    y2_title="Orders",
                    height=400,
                )

    with col2:
        st.subheader("Revenue Health")

        if "summary" in revenue_data:
            summary = revenue_data["summary"]
            growth_rate = summary.get("growth_rate", 0)

            # Revenue growth gauge
            growth_percentage = max(
                0, min(100, (growth_rate + 0.2) * 250)
            )  # Scale to 0-100

            render_gauge_chart(
                value=growth_percentage,
                title="Revenue Growth",
                min_value=0,
                max_value=100,
                threshold_ranges=[
                    {"range": [0, 30], "color": "red"},
                    {"range": [30, 70], "color": "yellow"},
                    {"range": [70, 100], "color": "green"},
                ],
                height=300,
            )

            # Growth indicator
            if growth_rate > 0.1:
                st.success(f"📈 Strong Growth: {format_percentage(growth_rate)}")
            elif growth_rate > 0:
                st.info(f"↗️ Positive Growth: {format_percentage(growth_rate)}")
            elif growth_rate > -0.05:
                st.warning(f"➡️ Stable: {format_percentage(growth_rate)}")
            else:
                st.error(f"📉 Declining: {format_percentage(growth_rate)}")


def render_revenue_breakdown(revenue_data: Dict[str, Any]) -> None:
    """Render revenue breakdown analysis."""
    st.header("📊 Revenue Breakdown")

    if not revenue_data or "breakdown" not in revenue_data:
        st.warning("No revenue breakdown data available")
        return

    breakdown = revenue_data["breakdown"]

    # Multi-dimensional breakdown charts
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("By Category")
        category_data = breakdown.get("by_category", [])
        if category_data:
            render_pie_chart(
                data=category_data,
                values_column="revenue",
                names_column="category",
                title="Revenue by Category",
                height=350,
            )

    with col2:
        st.subheader("By Region")
        region_data = breakdown.get("by_region", [])
        if region_data:
            render_donut_chart(
                data=region_data,
                values_column="revenue",
                names_column="region",
                title="Revenue by Region",
                height=350,
            )

    with col3:
        st.subheader("By Channel")
        channel_data = breakdown.get("by_channel", [])
        if channel_data:
            render_bar_chart(
                data=channel_data,
                x_column="channel",
                y_column="revenue",
                title="Revenue by Channel",
                height=350,
            )

    # Detailed breakdown tables
    st.subheader("Detailed Breakdown")

    tab1, tab2, tab3 = st.tabs(["Category", "Region", "Channel"])

    with tab1:
        if category_data:
            df = pd.DataFrame(category_data)
            st.dataframe(
                df,
                column_config={
                    "revenue": st.column_config.NumberColumn("Revenue", format="$%.2f"),
                    "percentage": st.column_config.NumberColumn(
                        "Percentage", format="%.1f%%"
                    ),
                },
                hide_index=True,
                use_container_width=True,
            )

    with tab2:
        if region_data:
            df = pd.DataFrame(region_data)
            st.dataframe(
                df,
                column_config={
                    "revenue": st.column_config.NumberColumn("Revenue", format="$%.2f"),
                    "percentage": st.column_config.NumberColumn(
                        "Percentage", format="%.1f%%"
                    ),
                },
                hide_index=True,
                use_container_width=True,
            )

    with tab3:
        if channel_data:
            df = pd.DataFrame(channel_data)
            st.dataframe(
                df,
                column_config={
                    "revenue": st.column_config.NumberColumn("Revenue", format="$%.2f"),
                    "percentage": st.column_config.NumberColumn(
                        "Percentage", format="%.1f%%"
                    ),
                },
                hide_index=True,
                use_container_width=True,
            )


def render_forecasting_analysis(revenue_data: Dict[str, Any]) -> None:
    """Render revenue forecasting analysis."""
    st.header("🔮 Revenue Forecasting")

    if not revenue_data or "forecast" not in revenue_data:
        st.warning("No forecasting data available")
        return

    forecast = revenue_data["forecast"]

    # Forecast metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        predicted_revenue = forecast.get("next_period_revenue", 0)
        st.metric(
            "Next Period Revenue",
            format_currency(predicted_revenue),
            help="Predicted revenue for next period",
        )

    with col2:
        confidence = forecast.get("confidence_interval", {})
        lower = confidence.get("lower", 0)
        upper = confidence.get("upper", 0)
        range_pct = (
            ((upper - lower) / predicted_revenue * 100) if predicted_revenue > 0 else 0
        )
        st.metric(
            "Confidence Range",
            f"±{range_pct:.1f}%",
            help="Confidence interval range as percentage",
        )

    with col3:
        trend = forecast.get("trend", "stable")
        trend_icon = (
            "📈" if trend == "increasing" else "📉" if trend == "decreasing" else "➡️"
        )
        st.metric(
            "Trend Direction",
            f"{trend_icon} {trend.title()}",
            help="Predicted trend direction",
        )

    with col4:
        # Backtest: the forecast method re-run on the most recent days and compared to actuals
        accuracy = forecast.get("backtest_accuracy")
        st.metric(
            "Forecast Accuracy",
            "n/a" if accuracy is None else format_percentage(accuracy),
            help="Backtest of the trend forecast on the latest days (needs 9+ days of data)",
        )

    # Forecast visualization
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Revenue Forecast Chart")

        # Create forecast chart with confidence intervals
        if "time_series" in revenue_data:
            time_series = revenue_data["time_series"]
            df = pd.DataFrame(time_series)

            if not df.empty:
                # Add forecast points
                last_date = pd.to_datetime(df["date"].iloc[-1])
                forecast_dates = [last_date + timedelta(days=i) for i in range(1, 8)]

                forecast_values = [predicted_revenue / 7] * 7  # Daily forecast

                # Create forecast dataframe
                forecast_df = pd.DataFrame(
                    {
                        "date": forecast_dates,
                        "revenue": forecast_values,
                        "type": "forecast",
                    }
                )

                # Add type column to historical data
                df["type"] = "actual"

                # Combine data
                combined_df = pd.concat(
                    [df[["date", "revenue", "type"]], forecast_df], ignore_index=True
                )

                render_line_chart(
                    data=combined_df,
                    x_column="date",
                    y_column="revenue",
                    color_column="type",
                    title="Revenue Forecast",
                    height=400,
                )

    with col2:
        st.subheader("Forecast Confidence")

        # Confidence interval visualization
        confidence_data = [
            {"range": "Lower Bound", "value": lower},
            {"range": "Predicted", "value": predicted_revenue},
            {"range": "Upper Bound", "value": upper},
        ]

        render_bar_chart(
            data=confidence_data,
            x_column="range",
            y_column="value",
            title="Forecast Range",
            height=300,
        )


def render_profit_analysis(revenue_data: Dict[str, Any]) -> None:
    """Render profit margin analysis."""
    st.header("📈 Profit Analysis")

    if not revenue_data:
        st.warning("No profit data available")
        return

    # Profit metrics
    if "summary" in revenue_data:
        summary = revenue_data["summary"]

        total_revenue = summary.get("total_revenue", 0)
        profit_margin = summary.get("profit_margin", 0)
        total_profit = total_revenue * profit_margin

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total Profit",
                format_currency(total_profit),
                help="Total profit for the period",
            )

        with col2:
            st.metric(
                "Profit Margin",
                format_percentage(profit_margin),
                help="Overall profit margin percentage",
            )

        with col3:
            # Calculate profit per order
            total_orders = summary.get("total_orders", 0)
            profit_per_order = total_profit / total_orders if total_orders > 0 else 0
            st.metric(
                "Profit per Order",
                format_currency(profit_per_order),
                help="Average profit per order",
            )

        with col4:
            # Profit versus the previous period of equal length
            profit_growth = summary.get("profit_growth") or 0
            st.metric(
                "Profit Growth",
                format_percentage(profit_growth),
                help="Profit growth versus the previous period of equal length",
            )

    # Profit trend analysis
    if "time_series" in revenue_data:
        time_series = revenue_data["time_series"]
        df = pd.DataFrame(time_series)

        if not df.empty and "profit_margin" in df.columns:
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Profit Margin Trend")

                render_line_chart(
                    data=df,
                    x_column="date",
                    y_column="profit_margin",
                    title="Profit Margin Over Time",
                    height=350,
                )

            with col2:
                st.subheader("Revenue vs Profit")

                # Calculate profit values
                df["profit"] = df["revenue"] * df["profit_margin"]

                render_dual_axis_chart(
                    data=df,
                    x_column="date",
                    y1_column="revenue",
                    y2_column="profit",
                    title="Revenue vs Profit",
                    y1_title="Revenue ($)",
                    y2_title="Profit ($)",
                    height=350,
                )


def render_product_revenue_analysis(products_data: Dict[str, Any]) -> None:
    """Render product revenue analysis."""
    st.header("🛍️ Product Revenue Analysis")

    if not products_data or "products" not in products_data:
        st.warning("No product data available")
        return

    products = products_data["products"]

    # Top products by revenue
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Top Products by Revenue")

        # Sort products by revenue
        top_products = sorted(
            products, key=lambda x: x.get("revenue", 0), reverse=True
        )[:10]

        render_bar_chart(
            data=top_products,
            x_column="name",
            y_column="revenue",
            title="Top 10 Products by Revenue",
            height=400,
        )

    with col2:
        st.subheader("Product Performance Metrics")

        if "summary" in products_data:
            summary = products_data["summary"]

            metrics = [
                {
                    "title": "Total Products",
                    "value": len(products),
                    "format_type": "number",
                },
                {
                    "title": "Avg Product Revenue",
                    "value": summary.get("total_revenue", 0) / len(products)
                    if products
                    else 0,
                    "format_type": "currency",
                },
                {
                    "title": "Top Category",
                    "value": summary.get("top_performing_category", "N/A"),
                    "format_type": "text",
                },
            ]

            for metric in metrics:
                if metric["format_type"] == "text":
                    st.metric(metric["title"], metric["value"])
                else:
                    render_metric_card(**metric)

    # Product performance table
    st.subheader("Product Performance Details")

    # Prepare product data for table
    product_table_data = []
    for product in products[:20]:  # Show top 20
        row = {
            "Product ID": product.get("product_id", ""),
            "Name": product.get("name", ""),
            "Category": product.get("category", ""),
            "Revenue": product.get("revenue", 0),
            "Units Sold": product.get("units_sold", 0),
            "Margin": product.get("margin", 0),
            "Growth Rate": product.get("growth_rate", 0),
            "Rating": product.get("average_rating", 0),
        }
        product_table_data.append(row)

    df = pd.DataFrame(product_table_data)

    st.dataframe(
        df,
        column_config={
            "Revenue": st.column_config.NumberColumn("Revenue", format="$%.2f"),
            "Units Sold": st.column_config.NumberColumn("Units Sold", format="%d"),
            "Margin": st.column_config.NumberColumn("Margin", format="%.1f%%"),
            "Growth Rate": st.column_config.NumberColumn(
                "Growth Rate", format="%.1f%%"
            ),
            "Rating": st.column_config.NumberColumn("Rating", format="%.1f"),
        },
        hide_index=True,
        use_container_width=True,
    )


def render_cohort_revenue_analysis(revenue_data: Dict[str, Any]) -> None:
    """Render cohort revenue analysis."""
    st.header("👥 Cohort Revenue Analysis")

    if not revenue_data or "cohort_analysis" not in revenue_data:
        st.warning("No cohort revenue data available")
        return

    cohort_analysis = revenue_data["cohort_analysis"]

    # Cohort metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        retention_rates = cohort_analysis.get("retention_rates", {})
        month_1_retention = retention_rates.get("month_1", 0)
        st.metric(
            "Month 1 Retention",
            format_percentage(month_1_retention),
            help="Customer retention rate after 1 month",
        )

    with col2:
        month_6_retention = retention_rates.get("month_6", 0)
        st.metric(
            "Month 6 Retention",
            format_percentage(month_6_retention),
            help="Customer retention rate after 6 months",
        )

    with col3:
        ltv_by_cohort = cohort_analysis.get("ltv_by_cohort", {})
        avg_ltv = (
            sum(ltv_by_cohort.values()) / len(ltv_by_cohort) if ltv_by_cohort else 0
        )
        st.metric(
            "Average Cohort LTV",
            format_currency(avg_ltv),
            help="Average lifetime value across cohorts",
        )

    # Cohort LTV comparison
    if ltv_by_cohort:
        st.subheader("Cohort LTV Comparison")

        cohort_data = [{"cohort": k, "ltv": v} for k, v in ltv_by_cohort.items()]

        render_bar_chart(
            data=cohort_data,
            x_column="cohort",
            y_column="ltv",
            title="Lifetime Value by Cohort",
            height=400,
        )


def render() -> None:
    """Render the revenue analytics dashboard page."""
    st.title("💰 Revenue Analytics")
    st.markdown("Comprehensive revenue analysis, forecasting, and profit insights")

    # Load data
    with st.spinner("Loading revenue analytics data..."):
        data = load_revenue_data()

    if not data:
        st.error("Failed to load revenue analytics data")
        return

    # Use daily data as primary source
    primary_revenue_data = data.get("revenue_daily", {})

    # Revenue overview
    render_revenue_overview(primary_revenue_data)

    st.markdown("---")

    # Revenue breakdown
    render_revenue_breakdown(primary_revenue_data)

    st.markdown("---")

    # Forecasting analysis
    render_forecasting_analysis(primary_revenue_data)

    st.markdown("---")

    # Profit analysis
    render_profit_analysis(primary_revenue_data)

    st.markdown("---")

    # Product revenue analysis
    render_product_revenue_analysis(data.get("products", {}))

    st.markdown("---")

    # Cohort revenue analysis
    render_cohort_revenue_analysis(primary_revenue_data)

    # Footer with last update time
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
