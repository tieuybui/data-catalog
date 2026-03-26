"""
Simple password authentication for Streamlit app.
"""

import hmac

import streamlit as st

_LS_KEY = "dd_auth_ok"


def restore_auth(ls):
    """Restore authentication state from browser LocalStorage."""
    if "password_correct" not in st.session_state:
        val = ls.getItem(_LS_KEY)
        if val == "1":
            st.session_state["password_correct"] = True
    if st.session_state.get("password_correct") and ls.getItem(_LS_KEY) != "1":
        ls.setItem(_LS_KEY, "1")


def check_password():
    """Gate the app behind a password prompt. Halts execution if not authenticated."""
    if st.session_state.get("password_correct"):
        return

    pwd = st.text_input("Password", type="password", key="_pw_input")
    if pwd:
        if hmac.compare_digest(pwd, st.secrets.get("APP_PASSWORD", "")):
            st.session_state["password_correct"] = True
            st.rerun()
        else:
            st.error("Incorrect password")
    st.stop()
