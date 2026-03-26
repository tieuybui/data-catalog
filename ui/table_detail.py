"""
Table detail view: metadata editor, column grid with AI suggestions.
"""

import pandas as pd
import streamlit as st

from config.settings import LAYER_COLORS, safe_get
from core.database import is_fabric
from services.metadata import (
    load_dd_columns, load_column_stats, update_table_fields, update_column_fields,
)
from services.overrides import apply_column_overrides
from services.ai_suggest import suggest_table, suggest_columns


def render_table_detail(dd_tables: pd.DataFrame):
    """Render the detail view for the selected table."""
    selected = st.session_state.selected_table
    tbl_row = dd_tables[dd_tables["table_name"] == selected].iloc[0]
    layer = tbl_row["layer"]
    color = LAYER_COLORS.get(layer, "#94a3b8")

    # ── Header ──
    st.markdown(
        f'<span class="layer-badge" style="background:{color}20;color:{color}">'
        f'{layer.upper()}</span> <b style="font-size:24px">{selected}</b>',
        unsafe_allow_html=True,
    )

    s1, s2, s3 = st.columns(3)
    rc = safe_get(tbl_row, "row_count")
    cc = safe_get(tbl_row, "column_count")
    s1.metric("Rows", f"{int(rc):,}" if rc is not None else "?")
    s2.metric("Columns", int(cc) if cc is not None else "?")
    sa = safe_get(tbl_row, "scanned_at")
    s3.metric("Last Scanned", str(sa)[:19] if sa is not None else "Never")

    st.divider()

    # ── Table metadata form ──
    _render_table_form(selected, tbl_row, layer)

    st.divider()

    # ── Column grid ──
    _render_column_grid(selected, layer)


def _render_table_form(selected: str, tbl_row, layer: str):
    """Editable table metadata form with AI suggest."""
    st.subheader("Table Metadata")

    if st.button("🤖 AI Suggest Table Info", key="ai_tbl"):
        try:
            with st.spinner("Asking Groq AI..."):
                dd_cols_for_ai = load_dd_columns(selected)
                suggestion = suggest_table(selected, layer, dd_cols_for_ai)
                st.session_state[f"ai_tbl_{selected}"] = suggestion
        except Exception as e:
            st.error(f"AI error: {e}")

    ai_tbl = st.session_state.get(f"ai_tbl_{selected}", {})

    with st.form(f"table_meta_{selected}"):
        fc1, fc2 = st.columns(2)
        desc = fc1.text_area(
            "Description",
            value=ai_tbl.get("description") or safe_get(tbl_row, "description", ""),
            height=80,
        )
        owner = fc2.text_input(
            "Business Owner",
            value=ai_tbl.get("business_owner") or safe_get(tbl_row, "business_owner", ""),
        )
        fc3, fc4, fc5 = st.columns(3)
        source = fc3.text_input(
            "Source System",
            value=ai_tbl.get("source_system") or safe_get(tbl_row, "source_system", ""),
        )
        freq_options = ["", "hourly", "daily", "weekly", "monthly", "ad-hoc", "real-time"]
        ai_freq = ai_tbl.get("refresh_frequency", "")
        cur_freq = ai_freq if ai_freq in freq_options else safe_get(tbl_row, "refresh_frequency", "")
        freq = fc4.selectbox(
            "Refresh Frequency",
            freq_options,
            index=freq_options.index(cur_freq) if cur_freq in freq_options else 0,
        )
        tags = fc5.text_input(
            "Tags (comma-separated)",
            value=ai_tbl.get("tags") or safe_get(tbl_row, "tags", ""),
        )

        if st.form_submit_button("💾 Save Table Info", type="primary"):
            update_table_fields(selected, {
                "description": desc or None,
                "business_owner": owner or None,
                "source_system": source or None,
                "refresh_frequency": freq or None,
                "tags": tags or None,
            })
            if is_fabric():
                st.success("Table metadata queued! Check sidebar for generated code.")
            else:
                st.success("Table metadata saved!")
            st.rerun()


def _render_column_grid(selected: str, layer: str):
    """Editable column data grid with AI suggestions."""
    st.subheader("Columns")

    dd_cols = load_column_stats(st.session_state.env, selected)
    if is_fabric() and not dd_cols.empty:
        dd_cols = apply_column_overrides(dd_cols)

    if dd_cols.empty:
        dd_cols = load_dd_columns(selected)

    if dd_cols.empty:
        st.info("No column data. Run a scan first.")
        return

    # AI suggest
    if st.button("🤖 AI Suggest All Columns", key="ai_cols"):
        try:
            with st.spinner("Asking Groq AI..."):
                suggestions = suggest_columns(selected, layer, dd_cols)
                st.session_state[f"ai_cols_{selected}"] = {
                    s["column_name"]: s for s in suggestions
                }
        except Exception as e:
            st.error(f"AI error: {e}")

    ai_cols = st.session_state.get(f"ai_cols_{selected}", {})

    # Prepare dataframes
    edit_cols = [
        "column_name", "data_type", "is_primary_key", "is_nullable",
        "null_percentage", "distinct_count", "sample_values",
        "description", "business_name",
    ]
    original_df = dd_cols[[c for c in edit_cols if c in dd_cols.columns]].copy()
    display_df = original_df.copy()

    # Apply AI suggestions to empty fields
    if ai_cols:
        for idx, row in display_df.iterrows():
            cn = row.get("column_name", "")
            if cn in ai_cols:
                s = ai_cols[cn]
                if "description" in display_df.columns and pd.isna(row.get("description")):
                    display_df.at[idx, "description"] = s.get("description", "")
                if "business_name" in display_df.columns and pd.isna(row.get("business_name")):
                    display_df.at[idx, "business_name"] = s.get("business_name", "")

    if "is_primary_key" in display_df.columns:
        display_df["is_primary_key"] = display_df["is_primary_key"].astype(bool)

    column_config = {
        "column_name": st.column_config.TextColumn("Column", disabled=True),
        "data_type": st.column_config.TextColumn("Type", disabled=True),
        "is_primary_key": st.column_config.CheckboxColumn("PK", width="small"),
        "is_nullable": st.column_config.TextColumn("Nullable", disabled=True, width="small"),
        "null_percentage": st.column_config.NumberColumn("Null %", format="%.1f", disabled=True),
        "distinct_count": st.column_config.NumberColumn("Distinct", disabled=True),
        "sample_values": st.column_config.TextColumn("Samples", disabled=True, width="large"),
        "description": st.column_config.TextColumn("Description", width="large"),
        "business_name": st.column_config.TextColumn("Business Name"),
    }

    edited = st.data_editor(
        display_df,
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key=f"col_editor_{selected}",
    )

    if st.button("💾 Save Column Edits", type="primary"):
        changes = 0
        for idx in range(len(edited)):
            orig_row = original_df.iloc[idx]
            edit_row = edited.iloc[idx]
            col_name = orig_row["column_name"]
            updates = {}
            for field in ["description", "business_name", "is_primary_key"]:
                if field in orig_row and field in edit_row:
                    ov = orig_row[field]
                    ev = edit_row[field]
                    if pd.isna(ov):
                        ov = None
                    if pd.isna(ev):
                        ev = None
                    if field == "is_primary_key":
                        ov = bool(ov) if ov is not None else False
                        ev = bool(ev) if ev is not None else False
                        if ov != ev:
                            updates[field] = 1 if ev else 0
                    elif str(ov or "") != str(ev or ""):
                        updates[field] = ev
            if updates:
                update_column_fields(selected, col_name, updates)
                changes += 1

        if changes:
            if is_fabric():
                st.success(f"Queued {changes} column(s)! Check sidebar for generated code.")
            else:
                st.success(f"Saved {changes} column(s)!")
            st.rerun()
        else:
            st.info("No changes detected.")
