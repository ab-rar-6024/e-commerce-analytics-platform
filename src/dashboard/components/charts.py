"""
Chart components for data visualization.

This module provides reusable chart components using Plotly and Altair
for various types of data visualization in the dashboard.
"""

from typing import Any, Dict, List, Optional, Union

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from dashboard.config.settings import get_dashboard_settings
from dashboard.utils.data_processing import process_time_series_data


def get_chart_theme() -> Dict[str, Any]:
    """Get chart theme configuration."""
    settings = get_dashboard_settings()
    return settings.get_chart_config()


def render_line_chart(
    data: Union[pd.DataFrame, List[Dict]],
    x_column: str,
    y_column: str,
    title: str = "",
    color_column: Optional[str] = None,
    height: int = 400,
) -> None:
    """
    Render a line chart.

    Args:
        data: Data for the chart
        x_column: X-axis column name
        y_column: Y-axis column name
        title: Chart title
        color_column: Column for color grouping
        height: Chart height
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available for chart")
        return

    theme = get_chart_theme()

    fig = px.line(
        df,
        x=x_column,
        y=y_column,
        color=color_column,
        title=title,
        height=height,
        template=theme.get("template", "plotly_white"),
    )

    fig.update_layout(showlegend=color_column is not None, hovermode="x unified")

    st.plotly_chart(fig, use_container_width=True)


def render_bar_chart(
    data: Union[pd.DataFrame, List[Dict]],
    x_column: str,
    y_column: str,
    title: str = "",
    orientation: str = "v",
    color_column: Optional[str] = None,
    height: int = 400,
) -> None:
    """
    Render a bar chart.

    Args:
        data: Data for the chart
        x_column: X-axis column name
        y_column: Y-axis column name
        title: Chart title
        orientation: Chart orientation ('v' for vertical, 'h' for horizontal)
        color_column: Column for color grouping
        height: Chart height
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available for chart")
        return

    theme = get_chart_theme()

    fig = px.bar(
        df,
        x=x_column,
        y=y_column,
        color=color_column,
        title=title,
        orientation=orientation,
        height=height,
        template=theme.get("template", "plotly_white"),
    )

    fig.update_layout(showlegend=color_column is not None)

    st.plotly_chart(fig, use_container_width=True)


def render_pie_chart(
    data: Union[pd.DataFrame, List[Dict]],
    values_column: str,
    names_column: str,
    title: str = "",
    height: int = 400,
) -> None:
    """
    Render a pie chart.

    Args:
        data: Data for the chart
        values_column: Column with values
        names_column: Column with category names
        title: Chart title
        height: Chart height
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available for chart")
        return

    theme = get_chart_theme()

    fig = px.pie(
        df,
        values=values_column,
        names=names_column,
        title=title,
        height=height,
        template=theme.get("template", "plotly_white"),
    )

    fig.update_traces(textposition="inside", textinfo="percent+label")

    st.plotly_chart(fig, use_container_width=True)


def render_donut_chart(
    data: Union[pd.DataFrame, List[Dict]],
    values_column: str,
    names_column: str,
    title: str = "",
    height: int = 400,
) -> None:
    """
    Render a donut chart.

    Args:
        data: Data for the chart
        values_column: Column with values
        names_column: Column with category names
        title: Chart title
        height: Chart height
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available for chart")
        return

    theme = get_chart_theme()

    fig = px.pie(
        df,
        values=values_column,
        names=names_column,
        title=title,
        height=height,
        template=theme.get("template", "plotly_white"),
        hole=0.4,  # Creates the donut hole
    )

    fig.update_traces(textposition="inside", textinfo="percent+label")

    st.plotly_chart(fig, use_container_width=True)


def render_scatter_plot(
    data: Union[pd.DataFrame, List[Dict]],
    x_column: str,
    y_column: str,
    title: str = "",
    size_column: Optional[str] = None,
    color_column: Optional[str] = None,
    height: int = 400,
) -> None:
    """
    Render a scatter plot.

    Args:
        data: Data for the chart
        x_column: X-axis column name
        y_column: Y-axis column name
        title: Chart title
        size_column: Column for bubble size
        color_column: Column for color grouping
        height: Chart height
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available for chart")
        return

    theme = get_chart_theme()

    fig = px.scatter(
        df,
        x=x_column,
        y=y_column,
        size=size_column,
        color=color_column,
        title=title,
        height=height,
        template=theme.get("template", "plotly_white"),
    )

    st.plotly_chart(fig, use_container_width=True)


def render_heatmap(
    data: Union[pd.DataFrame, List[Dict]],
    x_column: str,
    y_column: str,
    z_column: str,
    title: str = "",
    height: int = 400,
) -> None:
    """
    Render a heatmap.

    Args:
        data: Data for the chart
        x_column: X-axis column name
        y_column: Y-axis column name
        z_column: Values column for color intensity
        title: Chart title
        height: Chart height
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available for chart")
        return

    # Pivot data for heatmap
    pivot_df = df.pivot(index=y_column, columns=x_column, values=z_column)

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot_df.values,
            x=pivot_df.columns,
            y=pivot_df.index,
            colorscale="RdYlBu_r",
        )
    )

    fig.update_layout(title=title, height=height)

    st.plotly_chart(fig, use_container_width=True)


def render_gauge_chart(
    value: float,
    title: str = "",
    min_value: float = 0,
    max_value: float = 100,
    threshold_ranges: Optional[List[Dict]] = None,
    height: int = 300,
) -> None:
    """
    Render a gauge chart.

    Args:
        value: Current value
        title: Chart title
        min_value: Minimum value
        max_value: Maximum value
        threshold_ranges: List of threshold ranges with colors
        height: Chart height
    """
    if threshold_ranges is None:
        threshold_ranges = [
            {"range": [0, 50], "color": "red"},
            {"range": [50, 80], "color": "yellow"},
            {"range": [80, 100], "color": "green"},
        ]

    fig = go.Figure(
        go.Indicator(
            mode="gauge+number+delta",
            value=value,
            domain={"x": [0, 1], "y": [0, 1]},
            title={"text": title},
            gauge={
                "axis": {"range": [None, max_value]},
                "bar": {"color": "darkblue"},
                "steps": [
                    {"range": [r["range"][0], r["range"][1]], "color": r["color"]}
                    for r in threshold_ranges
                ],
                "threshold": {
                    "line": {"color": "red", "width": 4},
                    "thickness": 0.75,
                    "value": max_value * 0.9,
                },
            },
        )
    )

    fig.update_layout(height=height)

    st.plotly_chart(fig, use_container_width=True)


def render_funnel_chart(
    data: Union[pd.DataFrame, List[Dict]],
    stage_column: str,
    value_column: str,
    title: str = "",
    height: int = 400,
) -> None:
    """
    Render a funnel chart.

    Args:
        data: Data for the chart
        stage_column: Column with funnel stages
        value_column: Column with values
        title: Chart title
        height: Chart height
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available for chart")
        return

    fig = go.Figure(
        go.Funnel(
            y=df[stage_column], x=df[value_column], textinfo="value+percent initial"
        )
    )

    fig.update_layout(title=title, height=height)

    st.plotly_chart(fig, use_container_width=True)


def render_waterfall_chart(
    data: Union[pd.DataFrame, List[Dict]],
    x_column: str,
    y_column: str,
    title: str = "",
    height: int = 400,
) -> None:
    """
    Render a waterfall chart.

    Args:
        data: Data for the chart
        x_column: X-axis column name
        y_column: Y-axis column name
        title: Chart title
        height: Chart height
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available for chart")
        return

    fig = go.Figure(
        go.Waterfall(
            name="",
            orientation="v",
            measure=["relative"] * (len(df) - 1) + ["total"],
            x=df[x_column],
            textposition="outside",
            text=df[y_column],
            y=df[y_column],
            connector={"line": {"color": "rgb(63, 63, 63)"}},
        )
    )

    fig.update_layout(title=title, height=height, showlegend=False)

    st.plotly_chart(fig, use_container_width=True)


def render_time_series_chart(
    data: Union[pd.DataFrame, List[Dict]],
    date_column: str = "date",
    value_columns: List[str] = None,
    title: str = "",
    height: int = 400,
) -> None:
    """
    Render a time series chart with multiple lines.

    Args:
        data: Time series data
        date_column: Date column name
        value_columns: List of value columns to plot
        title: Chart title
        height: Chart height
    """
    if isinstance(data, list):
        df = process_time_series_data(data, date_column)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available for chart")
        return

    if value_columns is None:
        # Use all numeric columns except date
        value_columns = df.select_dtypes(include=["number"]).columns.tolist()

    fig = go.Figure()

    for column in value_columns:
        if column in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df[date_column],
                    y=df[column],
                    mode="lines+markers",
                    name=column.replace("_", " ").title(),
                    line=dict(width=2),
                )
            )

    fig.update_layout(
        title=title,
        height=height,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    st.plotly_chart(fig, use_container_width=True)


def render_cohort_heatmap(
    cohort_data: Dict[str, Any], title: str = "Cohort Analysis"
) -> None:
    """
    Render a cohort analysis heatmap.

    Args:
        cohort_data: Cohort analysis data
        title: Chart title
    """
    if not cohort_data or "cohorts" not in cohort_data:
        st.warning("No cohort data available")
        return

    # Prepare data for heatmap
    cohorts = cohort_data["cohorts"]
    cohort_table = []

    for cohort in cohorts:
        row = [cohort["cohort"]]
        for period in cohort["periods"]:
            row.append(period["value"])
        cohort_table.append(row)

    if not cohort_table:
        st.warning("No cohort data to display")
        return

    # Convert to DataFrame
    max_periods = max(len(cohort["periods"]) for cohort in cohorts)
    columns = ["Cohort"] + [f"Period {i}" for i in range(max_periods)]

    df = pd.DataFrame(cohort_table, columns=columns[: len(cohort_table[0])])
    df = df.set_index("Cohort")

    # Create heatmap
    fig = go.Figure(
        data=go.Heatmap(
            z=df.values,
            x=df.columns,
            y=df.index,
            colorscale="RdYlGn",
            text=df.values,
            texttemplate="%{text:.1%}"
            if cohort_data.get("metric") == "retention"
            else "%{text:.0f}",
            textfont={"size": 10},
        )
    )

    fig.update_layout(
        title=title, height=400, xaxis_title="Period", yaxis_title="Cohort"
    )

    st.plotly_chart(fig, use_container_width=True)


def render_segment_treemap(
    segment_data: Dict[str, Any], title: str = "Customer Segments"
) -> None:
    """
    Render a treemap for customer segments.

    Args:
        segment_data: Customer segment data
        title: Chart title
    """
    if not segment_data or "segments" not in segment_data:
        st.warning("No segment data available")
        return

    segments = segment_data["segments"]

    # Prepare data
    labels = [seg["name"] for seg in segments]
    values = [seg["count"] for seg in segments]
    parents = [""] * len(segments)  # All segments are top-level

    fig = go.Figure(
        go.Treemap(
            labels=labels,
            values=values,
            parents=parents,
            textinfo="label+value+percent parent",
            textfont_size=12,
            marker_colorscale="RdYlGn",
        )
    )

    fig.update_layout(title=title, height=400)

    st.plotly_chart(fig, use_container_width=True)


def render_dual_axis_chart(
    data: Union[pd.DataFrame, List[Dict]],
    x_column: str,
    y1_column: str,
    y2_column: str,
    title: str = "",
    y1_title: str = "Y1",
    y2_title: str = "Y2",
    height: int = 400,
) -> None:
    """
    Render a chart with dual y-axes.

    Args:
        data: Data for the chart
        x_column: X-axis column name
        y1_column: Left y-axis column name
        y2_column: Right y-axis column name
        title: Chart title
        y1_title: Left y-axis title
        y2_title: Right y-axis title
        height: Chart height
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available for chart")
        return

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add first trace
    fig.add_trace(
        go.Scatter(x=df[x_column], y=df[y1_column], name=y1_title),
        secondary_y=False,
    )

    # Add second trace
    fig.add_trace(
        go.Scatter(x=df[x_column], y=df[y2_column], name=y2_title),
        secondary_y=True,
    )

    # Set y-axes titles
    fig.update_yaxes(title_text=y1_title, secondary_y=False)
    fig.update_yaxes(title_text=y2_title, secondary_y=True)

    fig.update_layout(title=title, height=height)

    st.plotly_chart(fig, use_container_width=True)


def render_geographic_map(
    data: Union[pd.DataFrame, List[Dict]],
    location_column: str,
    value_column: str,
    title: str = "Geographic Sales",
    map_type: str = "choropleth",
    height: int = 500,
    color_scale: str = "Blues",
) -> None:
    """
    Render a geographic sales map.

    Args:
        data: Data for the map
        location_column: Column with location codes (ISO country codes, state codes, etc.)
        value_column: Column with values to visualize
        title: Map title
        map_type: Type of map ('choropleth' or 'scatter_geo')
        height: Map height
        color_scale: Color scale for the map
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No geographic data available for map")
        return

    theme = get_chart_theme()

    if map_type == "choropleth":
        # Create choropleth map
        fig = px.choropleth(
            df,
            locations=location_column,
            color=value_column,
            title=title,
            color_continuous_scale=color_scale,
            height=height,
            template=theme.get("template", "plotly_white"),
        )

        # Update layout for better appearance
        fig.update_layout(
            geo=dict(
                showframe=False, showcoastlines=True, projection_type="natural earth"
            ),
            title_x=0.5,
        )

    elif map_type == "scatter_geo":
        # Create scatter geo map
        fig = px.scatter_geo(
            df,
            locations=location_column,
            size=value_column,
            title=title,
            height=height,
            template=theme.get("template", "plotly_white"),
        )

        fig.update_layout(
            geo=dict(
                projection_type="natural earth",
                showland=True,
                landcolor="lightgray",
            ),
            title_x=0.5,
        )

    else:
        st.error(f"Unsupported map type: {map_type}")
        return

    st.plotly_chart(fig, use_container_width=True)


def render_executive_kpi_overview(
    kpi_data: Dict[str, Any],
    title: str = "Executive KPI Overview",
    height: int = 150,
) -> None:
    """
    Render executive-level KPI overview with strategic metrics.

    Args:
        kpi_data: Dictionary containing KPI data
        title: Section title
        height: Height of each metric card
    """
    if not kpi_data:
        st.warning("No KPI data available")
        return

    st.subheader(title)

    # Executive strategic metrics (only metrics that are computed from data)
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "Total Revenue",
            f"${kpi_data.get('total_revenue', 0):,.0f}",
            f"{kpi_data.get('revenue_growth', 0):+.1f}%",
            help="Revenue for the period with growth versus the previous period of equal length",
        )

    with col2:
        st.metric(
            "Active Customers",
            f"{kpi_data.get('active_customers', 0):,}",
            help="Customers with at least one paid order in the period",
        )

    with col3:
        st.metric(
            "Average Order Value",
            f"${kpi_data.get('average_order_value', 0):,.2f}",
            help="Revenue per paid order in the period",
        )

    with col4:
        st.metric(
            "Profit Margin",
            f"{kpi_data.get('profit_margin', 0):.1f}%",
            f"{kpi_data.get('margin_change', 0):+.2f}pp",
            help="(Revenue - product cost) / revenue, with change from the previous period",
        )

    with col5:
        st.metric(
            "Product Rating",
            f"{kpi_data.get('customer_satisfaction', 0):.2f}/5.0",
            f"{kpi_data.get('satisfaction_change', 0):+.2f}",
            help="Average rating of the products sold, weighted by units",
        )


def render_revenue_performance_executive(
    revenue_data: Dict[str, Any],
    title: str = "Revenue & Sales Performance",
    height: int = 400,
) -> None:
    """
    Render executive-level revenue and sales performance charts.

    Args:
        revenue_data: Revenue data dictionary
        title: Section title
        height: Chart height
    """
    if not revenue_data:
        st.warning("No revenue data available")
        return

    st.subheader(title)

    col1, col2 = st.columns(2)

    with col1:
        # Period-over-period revenue comparison waterfall
        if "period_comparison" in revenue_data:
            cmp = revenue_data["period_comparison"]

            waterfall_data = [
                {"category": cmp.get("previous_label", "Previous period"), "value": cmp.get("previous_period", 0)},
                {"category": "Growth", "value": cmp.get("growth_amount", 0)},
                {"category": cmp.get("current_label", "Current period"), "value": cmp.get("current_period", 0)},
            ]

            render_waterfall_chart(
                data=waterfall_data,
                x_column="category",
                y_column="value",
                title="Period-over-Period Revenue Growth",
                height=height,
            )

    with col2:
        # Profit margin trend analysis
        if "profit_trends" in revenue_data:
            profit_data = revenue_data["profit_trends"]

            render_dual_axis_chart(
                data=profit_data,
                x_column="period",
                y1_column="revenue",
                y2_column="profit_margin",
                title="Revenue vs Profit Margin Trend",
                y1_title="Revenue ($)",
                y2_title="Profit Margin (%)",
                height=height,
            )

    # Channel performance overview
    if "channel_performance" in revenue_data:
        st.subheader("Channel Performance")
        channel_data = revenue_data["channel_performance"]

        col1, col2 = st.columns(2)

        with col1:
            render_bar_chart(
                data=channel_data,
                x_column="channel",
                y_column="revenue",
                title="Revenue by Channel",
                height=300,
            )

        with col2:
            render_bar_chart(
                data=channel_data,
                x_column="channel",
                y_column="growth_rate",
                title="Channel Growth Rate",
                height=300,
            )


def render_customer_metrics_executive(
    customer_data: Dict[str, Any],
    title: str = "Customer Analytics",
    height: int = 400,
) -> None:
    """
    Render executive-level customer metrics visualization.

    Args:
        customer_data: Customer data dictionary
        title: Section title
        height: Chart height
    """
    if not customer_data:
        st.warning("No customer data available")
        return

    st.subheader(title)

    col1, col2 = st.columns(2)

    with col1:
        # New customers per month (no marketing spend is tracked, so no CAC)
        if customer_data.get("acquisition_trends"):
            render_line_chart(
                data=customer_data["acquisition_trends"],
                x_column="period",
                y_column="new_customers",
                title="New Customers per Month",
                height=height,
            )

    with col2:
        # Customer lifetime value distribution
        if "clv_distribution" in customer_data:
            clv_data = customer_data["clv_distribution"]

            render_bar_chart(
                data=clv_data,
                x_column="clv_range",
                y_column="customer_count",
                title="Customer Lifetime Value Distribution",
                height=height,
            )

    # Customer retention and churn analysis
    col1, col2 = st.columns(2)

    with col1:
        if "retention_data" in customer_data:
            retention_data = customer_data["retention_data"]

            render_line_chart(
                data=retention_data,
                x_column="cohort_period",
                y_column="retention_rate",
                title="Customer Retention Rate",
                height=300,
            )

    with col2:
        if "churn_analysis" in customer_data:
            churn_data = customer_data["churn_analysis"]

            render_gauge_chart(
                value=churn_data.get("current_churn_rate", 0) * 100,
                title="Monthly Churn Rate",
                min_value=0,
                max_value=20,
                threshold_ranges=[
                    {"range": [0, 5], "color": "green"},
                    {"range": [5, 10], "color": "yellow"},
                    {"range": [10, 20], "color": "red"},
                ],
                height=300,
            )
