"""
Database overview page: summary metrics, layer breakdown, table list.
"""

import pandas as pd
import streamlit as st

from services.metadata import load_dd_columns


def render_overview(dd_tables: pd.DataFrame, total_tables: int, tbl_coverage: int):
    """Render the database overview dashboard."""
    st.header("Database Overview")

    dd_all_cols = load_dd_columns()
    total_cols = len(dd_all_cols)
    described_cols = dd_all_cols["description"].notna().sum() if not dd_all_cols.empty else 0
    col_coverage = round(described_cols / total_cols * 100) if total_cols else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Tables", total_tables)
    m2.metric("Total Columns", total_cols)
    m3.metric("Table Coverage", f"{tbl_coverage}%")
    m4.metric("Column Coverage", f"{col_coverage}%")

    st.subheader("Tables by Layer")
    agg_dict = {"table_name": ("table_name", "count")}
    if "row_count" in dd_tables.columns:
        agg_dict["total_rows"] = ("row_count", "sum")
    if "column_count" in dd_tables.columns:
        agg_dict["avg_columns"] = ("column_count", "mean")
    layer_summary = dd_tables.groupby("layer").agg(**agg_dict).reset_index()
    if "avg_columns" in layer_summary.columns:
        layer_summary["avg_columns"] = layer_summary["avg_columns"].round(1)
    st.dataframe(layer_summary, use_container_width=True, hide_index=True)

    st.subheader("All Tables")
    display_cols = ["table_name", "layer", "row_count", "column_count",
                    "description", "business_owner", "scanned_at"]
    available = [c for c in display_cols if c in dd_tables.columns]
    st.dataframe(dd_tables[available], use_container_width=True, hide_index=True)
