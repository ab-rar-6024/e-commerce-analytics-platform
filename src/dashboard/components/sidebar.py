"""
Sidebar navigation component for the dashboard.

This module provides the main navigation sidebar with page selection,
user information, and system status indicators.
"""

from datetime import datetime, timedelta
from typing import Optional

import streamlit as st

from dashboard.config.settings import get_dashboard_settings
from dashboard.utils.api_client import APIError
from dashboard.utils.auth import get_current_user, logout


def render_user_info():
    """Render user information section in sidebar."""
    current_user = get_current_user()
    if current_user:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 👤 User Info")
        st.sidebar.write(f"**Logged in as:** {current_user}")

        # Login time
        if "login_time" in st.session_state:
            login_time = datetime.fromtimestamp(st.session_state.login_time)
            st.sidebar.write(f"**Session started:** {login_time.strftime('%H:%M:%S')}")

        # Logout button
        if st.sidebar.button("🚪 Logout", use_container_width=True):
            logout()


def render_system_status():
    """Render system status indicators in sidebar."""
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔧 System Status")

    # API Connection Status
    api_client = st.session_state.get("api_client")
    if api_client:
        try:
            # Quick health check
            health_data = api_client.health_check()
            if health_data.get("status") == "healthy":
                st.sidebar.success("🟢 API Connected")
            else:
                st.sidebar.warning("🟡 API Degraded")
        except APIError:
            st.sidebar.error("🔴 API Disconnected")
        except Exception:
            st.sidebar.error("🔴 Connection Error")
    else:
        st.sidebar.error("🔴 No API Client")

    # Last refresh time
    if st.session_state.get("last_refresh"):
        refresh_time = datetime.fromtimestamp(st.session_state.last_refresh)
        st.sidebar.write(f"**Last refresh:** {refresh_time.strftime('%H:%M:%S')}")

    # Auto-refresh status
    if st.session_state.get("auto_refresh"):
        interval = st.session_state.get("refresh_interval", 30)
        st.sidebar.write(f"**Auto-refresh:** {interval}s")
    else:
        st.sidebar.write("**Auto-refresh:** Off")


def render_page_navigation() -> str:
    """
    Render page navigation menu.

    Returns:
        str: Selected page name
    """
    settings = get_dashboard_settings()
    pages = settings.pages

    st.sidebar.markdown("### 📊 Navigation")

    # Create navigation options
    page_options = []
    page_icons = {}

    for page in pages:
        if page["enabled"]:
            page_name = page["name"]
            page_options.append(page_name)
            page_icons[page_name] = page["icon"]

    # Current page selection
    current_page = st.session_state.get("current_page", page_options[0])

    # Ensure current page is in available options
    if current_page not in page_options:
        current_page = page_options[0]

    # Page selection with radio buttons
    selected_page = st.sidebar.radio(
        "Select Page",
        options=page_options,
        index=page_options.index(current_page),
        format_func=lambda x: f"{page_icons.get(x, '📄')} {x}",
        label_visibility="collapsed",
    )

    # Show page description
    for page in pages:
        if page["name"] == selected_page:
            st.sidebar.caption(page["description"])
            break

    return selected_page


def _quick_range_dates(time_range: str):
    """Return (start, end) dates for a quick-select range, or None for "Custom"."""
    today = datetime.now().date()
    if time_range == "Last 7 days":
        return today - timedelta(days=7), today
    if time_range == "Last 30 days":
        return today - timedelta(days=30), today
    if time_range == "Last 90 days":
        return today - timedelta(days=90), today
    if time_range == "Year to date":
        return today.replace(month=1, day=1), today
    return None


def _apply_quick_range():
    """Callback: copy the selected quick range into the date inputs.

    Streamlit only allows changing a widget's session-state value before the
    widget is created, which is the case for callbacks (they run first on rerun).
    """
    dates = _quick_range_dates(st.session_state.global_time_range)
    if dates:
        st.session_state.global_start_date, st.session_state.global_end_date = dates


def _mark_custom_range():
    """Callback: a manually edited date means the range is no longer a preset."""
    st.session_state.global_time_range = "Custom"


def render_filters_section():
    """Render common filters section."""
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔍 Filters")

    # Seed the date inputs from the default quick range on first load
    if "global_start_date" not in st.session_state:
        (
            st.session_state.global_start_date,
            st.session_state.global_end_date,
        ) = _quick_range_dates("Last 30 days")

    # Date range filter
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date", key="global_start_date", on_change=_mark_custom_range
        )

    with col2:
        end_date = st.date_input(
            "End Date", key="global_end_date", on_change=_mark_custom_range
        )

    # Time range quick select
    time_range = st.sidebar.selectbox(
        "Quick Select",
        options=[
            "Custom",
            "Last 7 days",
            "Last 30 days",
            "Last 90 days",
            "Year to date",
        ],
        index=2,  # Default to "Last 30 days"
        key="global_time_range",
        on_change=_apply_quick_range,
    )

    # Store filter values in session state
    st.session_state.filter_start_date = start_date
    st.session_state.filter_end_date = end_date
    st.session_state.filter_time_range = time_range

    # Additional filters
    st.sidebar.markdown("#### Additional Filters")

    # Region filter
    region = st.sidebar.selectbox(
        "Region",
        options=["All", "North America", "Europe", "Asia", "Other"],
        key="global_region_filter",
    )

    # Customer segment filter
    segment = st.sidebar.selectbox(
        "Customer Segment",
        options=[
            "All",
            "Champions",
            "Loyal Customers",
            "Potential Loyalists",
            "New Customers",
            "At Risk",
            "Need Attention",
        ],
        key="global_segment_filter",
    )

    # Store additional filters
    st.session_state.filter_region = region if region != "All" else None
    st.session_state.filter_segment = segment if segment != "All" else None


def render_export_section():
    """Render data export section."""
    settings = get_dashboard_settings()

    if settings.enable_export:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📥 Export Data")

        export_format = st.sidebar.selectbox(
            "Format", options=["CSV", "Excel", "JSON"], key="export_format"
        )

        if st.sidebar.button("📊 Export Current View", use_container_width=True):
            st.sidebar.success("Export functionality would be implemented here")
            # In a real implementation, this would trigger data export
            # based on the current page and filters


def render_help_section():
    """Render help and information section."""
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ❓ Help & Info")

    with st.sidebar.expander("📖 Quick Help"):
        st.markdown(
            """
        **Navigation:**
        - Use the radio buttons above to switch between pages
        - Apply filters to refine your data view
        - Enable auto-refresh for real-time updates

        **Features:**
        - 📈 Executive Dashboard: High-level KPIs
        - 👥 Customer Analytics: Segmentation & CLV
        - 💰 Revenue Analytics: Financial insights
        - 🛡️ Fraud Detection: Security monitoring
        - ⚡ Real-time Monitoring: Live metrics
        """
        )

    with st.sidebar.expander("🔧 Troubleshooting"):
        st.markdown(
            """
        **Common Issues:**
        - If data doesn't load, check API connection status
        - Refresh the page if auto-refresh stops working
        - Clear browser cache if experiencing display issues
        - Contact support if authentication fails
        """
        )

    # Version info
    st.sidebar.markdown("---")
    st.sidebar.caption("E-Commerce Analytics Platform v1.0.0")


def render_sidebar() -> str:
    """
    Render the complete sidebar with all components.

    Returns:
        str: Selected page name
    """
    # Sidebar header
    st.sidebar.title("📊 ECAP Dashboard")
    st.sidebar.markdown("E-Commerce Analytics Platform")

    # Main navigation
    selected_page = render_page_navigation()

    # Common filters
    render_filters_section()

    # Export section
    render_export_section()

    # System status
    render_system_status()

    # User information
    render_user_info()

    # Help section
    render_help_section()

    return selected_page
