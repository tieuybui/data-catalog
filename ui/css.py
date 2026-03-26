"""
Custom CSS styles for the Streamlit app.
"""

import streamlit as st


def inject_css():
    st.markdown("""
    <style>
        .layer-badge {
            display: inline-block;
            padding: 2px 10px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 700;
            font-family: monospace;
        }
        .metric-card {
            background: #1e293b;
            border-radius: 8px;
            padding: 16px;
            text-align: center;
        }
        .metric-value {
            font-size: 28px;
            font-weight: 700;
            color: #e2e8f0;
        }
        .metric-label {
            font-size: 12px;
            color: #94a3b8;
            margin-top: 4px;
        }
        div[data-testid="stSidebar"] .stRadio label {
            font-size: 13px !important;
        }
    </style>
    """, unsafe_allow_html=True)
