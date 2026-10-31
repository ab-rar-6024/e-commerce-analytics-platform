"""
Authentication utilities for the dashboard.

This module provides authentication functions for the Streamlit dashboard,
including login forms and session management.
"""

import hashlib
import time
from typing import Optional

import streamlit as st

from dashboard.config.settings import get_dashboard_settings


def hash_password(password: str) -> str:
    """
    Hash a password using SHA-256.

    Args:
        password: Plain text password

    Returns:
        str: Hashed password
    """
    return hashlib.sha256(password.encode()).hexdigest()


def verify_credentials(username: str, password: str) -> bool:
    """
    Verify user credentials.

    Args:
        username: Username
        password: Password

    Returns:
        bool: True if credentials are valid
    """
    settings = get_dashboard_settings()

    # For demo purposes, use simple credential check
    # In production, this would check against a database or external auth service
    return (
        username == settings.default_username and password == settings.default_password
    )


def login_form() -> bool:
    """
    Display login form and handle authentication.

    Returns:
        bool: True if login successful
    """
    st.markdown(
        """
        <div style='text-align: center; padding: 2rem;'>
            <h1>🔐 E-Commerce Analytics Platform</h1>
            <p>Please log in to access the dashboard</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        with st.form("login_form"):
            st.markdown("### Login")

            username = st.text_input(
                "Username", placeholder="Enter your username", help="Default: admin"
            )

            password = st.text_input(
                "Password",
                type="password",
                placeholder="Enter your password",
                help="Default: admin123",
            )

            submitted = st.form_submit_button("Login", use_container_width=True)

            if submitted:
                if not username or not password:
                    st.error("Please enter both username and password")
                    return False

                if verify_credentials(username, password):
                    # Set session state
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.session_state.login_time = time.time()

                    # Initialize API client with authentication
                    from dashboard.utils.api_client import APIClient

                    settings = get_dashboard_settings()

                    api_client = APIClient(
                        base_url=settings.api_base_url, timeout=settings.api_timeout
                    )

                    # Authenticate with API
                    if api_client.authenticate(
                        settings.api_username, settings.api_password
                    ):
                        st.session_state.api_client = api_client
                        st.success("Login successful! Redirecting...")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Failed to authenticate with API")
                        return False
                else:
                    st.error("Invalid username or password")
                    return False

    # Add demo credentials info
    with st.expander("Demo Credentials", expanded=False):
        st.info(
            """
            **Demo Credentials:**
            - Username: `admin`
            - Password: `admin123`

            This is a demo environment. In production, proper authentication
            would be implemented with secure credential storage.
            """
        )

    return False


def check_authentication() -> bool:
    """
    Check if user is authenticated and session is valid.

    Returns:
        bool: True if authenticated and session valid
    """
    settings = get_dashboard_settings()

    # Check if authentication is disabled
    if not settings.auth_enabled:
        return True

    # Check if user is authenticated
    if not st.session_state.get("authenticated", False):
        login_form()
        return False

    # Check session timeout
    login_time = st.session_state.get("login_time", 0)
    if time.time() - login_time > settings.session_timeout:
        st.session_state.authenticated = False
        st.session_state.api_client = None
        st.warning("Session expired. Please log in again.")
        st.rerun()
        return False

    return True


def logout():
    """Logout the current user."""
    # Clear session state
    for key in ["authenticated", "username", "login_time", "api_client"]:
        if key in st.session_state:
            del st.session_state[key]

    st.success("Logged out successfully")
    st.rerun()


def get_current_user() -> Optional[str]:
    """
    Get the current authenticated user.

    Returns:
        Optional[str]: Username if authenticated, None otherwise
    """
    if st.session_state.get("authenticated", False):
        return st.session_state.get("username")
    return None


def require_auth(func):
    """
    Decorator to require authentication for a function.

    Args:
        func: Function to protect

    Returns:
        Wrapped function
    """

    def wrapper(*args, **kwargs):
        if check_authentication():
            return func(*args, **kwargs)
        else:
            st.stop()

    return wrapper
