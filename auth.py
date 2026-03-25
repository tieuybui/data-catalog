"""
Simple password protection for Streamlit app.
Uses st.secrets["password"] set via .streamlit/secrets.toml or Streamlit Cloud secrets.
"""

import hmac
import streamlit as st


def check_password():
    """Return True if the user has entered the correct password."""

    if "password" not in st.secrets:
        # No password configured — allow access
        return True

    if st.session_state.get("authenticated"):
        return True

    def _on_submit():
        if hmac.compare_digest(
            st.session_state.get("_password_input", ""),
            st.secrets["password"],
        ):
            st.session_state["authenticated"] = True
        else:
            st.session_state["_password_wrong"] = True

    st.set_page_config(page_title="Login", page_icon="🔒")

    st.title("🔒 Data Dictionary")
    st.text_input("Password", type="password", key="_password_input", on_change=_on_submit)

    if st.session_state.get("_password_wrong"):
        st.error("Incorrect password. Please try again.")

    st.stop()
