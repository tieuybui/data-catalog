"""
Simple password protection.
Uses st.secrets["username"] and st.secrets["password"].
"""

import hmac
import streamlit as st
from streamlit_local_storage import LocalStorage

_LS_KEY = "dd_logged_in"


def check_password():
    if "password" not in st.secrets:
        return True

    if st.session_state.get("authenticated"):
        return True

    st.set_page_config(page_title="Login", page_icon="🔒")
    st.title("🔒 Data Dictionary")

    _ls = LocalStorage()
    saved = _ls.getItem(_LS_KEY)
    if saved == "true":
        st.session_state["authenticated"] = True
        return True

    user = st.text_input("Username")
    pw = st.text_input("Password", type="password")

    if st.button("Login"):
        if (
            hmac.compare_digest(user, st.secrets["username"])
            and hmac.compare_digest(pw, st.secrets["password"])
        ):
            st.session_state["authenticated"] = True
            _ls.setItem(_LS_KEY, "true")
            st.rerun()
        else:
            st.error("Invalid username or password.")

    st.stop()
