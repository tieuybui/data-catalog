"""
Data Dictionary App — Entry Point
Connect to Local SQL Server or Fabric Lakehouse, scan all tables, display & edit metadata.

Run: streamlit run data_dictionary_app.py
"""

import streamlit as st
from streamlit_local_storage import LocalStorage

from core.auth import check_password, restore_auth
from core.database import check_odbc_driver
from ui.css import inject_css
from ui.sidebar import render_sidebar
from ui.overview import render_overview
from ui.table_detail import render_table_detail

# ── Auth ──
ls = LocalStorage()
restore_auth(ls)
check_password()

# Preload LocalStorage values into session_state on first run
if "_ls_synced" not in st.session_state:
    _groq_val = ls.getItem("dd_groq_api_key") if "_groq_key" not in st.session_state else None
    if _groq_val:
        st.session_state["_groq_key"] = _groq_val.strip()
    st.session_state["_ls_synced"] = True
    st.rerun()

# ── Init ──
check_odbc_driver()

st.set_page_config(
    page_title="Data Dictionary",
    page_icon="📖",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_css()

if "env" not in st.session_state:
    st.session_state.env = "fabric_dev"
if "selected_table" not in st.session_state:
    st.session_state.selected_table = None

# ── Sidebar (may call st.stop()) ──
dd_tables, total_tables, tbl_coverage = render_sidebar(ls)

# ── Main area ──
if st.session_state.selected_table is None:
    render_overview(dd_tables, total_tables, tbl_coverage)
else:
    render_table_detail(dd_tables)
