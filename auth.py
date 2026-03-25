"""
Password protection with multi-user support.
Users are defined in st.secrets["users"] as email = password pairs.
"""

import hmac
import streamlit as st


def check_password():
    """Block access unless user provides valid credentials."""

    if "users" not in st.secrets:
        return True

    users: dict = dict(st.secrets["users"])

    if st.session_state.get("authenticated"):
        return True

    def _on_submit():
        email = st.session_state.get("_email_input", "").strip().lower()
        pw = st.session_state.get("_password_input", "")
        expected_pw = users.get(email)

        if expected_pw and hmac.compare_digest(pw, expected_pw):
            st.session_state["authenticated"] = True
            st.session_state["username"] = email
            st.session_state.pop("_login_error", None)
        else:
            st.session_state["_login_error"] = True

    # Already authenticated after callback — skip login page
    if st.session_state.get("authenticated"):
        return True

    st.set_page_config(page_title="Login", page_icon="🔒")

    st.title("🔒 Data Dictionary")
    st.text_input("Email", key="_email_input", placeholder="you@example.com")
    st.text_input("Password", type="password", key="_password_input")
    st.button("Login", on_click=_on_submit)

    if st.session_state.get("_login_error"):
        st.error("Invalid email or password.")

    st.stop()
