"""
Sidebar UI: environment selector, scan, cleanup, export, AI settings.
"""

import pandas as pd
import streamlit as st
from streamlit_local_storage import LocalStorage

from config.settings import ENV_CONFIGS, safe_get
from core.database import is_fabric
from services.metadata import (
    ensure_dd_tables, load_dd_tables, clear_query_caches, load_column_stats,
)
from services.scanner import (
    list_tables, batch_load_metadata, scan_one_table, scan_columns,
    save_scan_results,
)
from services.cleanup import find_stale_records, delete_stale_local
from services.export import export_json
from services.fabric_codegen import generate_scan_code, generate_cleanup_code, generate_edit_code
from services.overrides import load_overrides, clear_overrides
from services.ai_suggest import load_groq_key, save_groq_key


def render_sidebar(ls: LocalStorage) -> tuple[pd.DataFrame, int, int]:
    """Render the full sidebar. Returns (dd_tables, total_tables, tbl_coverage).

    Calls st.stop() if connection fails or no data — halting app execution.
    """
    with st.sidebar:
        st.title("📖 Data Dictionary")

        # ── Environment selector ──
        env_options = list(ENV_CONFIGS.keys())
        current_idx = env_options.index(st.session_state.env)
        selected_env = st.selectbox(
            "Environment",
            env_options,
            index=current_idx,
            format_func=lambda k: ENV_CONFIGS[k]["label"],
        )
        if selected_env != st.session_state.env:
            st.session_state.env = selected_env
            st.session_state.selected_table = None
            st.rerun()

        st.caption(ENV_CONFIGS[st.session_state.env]["label"])

        # ── Ensure DD tables exist ──
        try:
            ensure_dd_tables()
        except Exception as e:
            st.error(f"Connection failed: `{type(e).__name__}: {e}`")
            st.stop()

        # ── Refresh ──
        if st.button("🔄 Refresh Data", use_container_width=True,
                     help="Reload data from dd_tables/dd_columns (clear cache)"):
            clear_query_caches()
            for key in list(st.session_state.keys()):
                if key.startswith("row_count_"):
                    del st.session_state[key]
            if "_fabric_conn" in st.session_state:
                try:
                    st.session_state["_fabric_conn"].close()
                except Exception:
                    pass
                del st.session_state["_fabric_conn"]
            st.rerun()

        # ── Scan ──
        _render_scan_section()

        # ── Cleanup ──
        _render_cleanup_section()

        st.divider()

        # ── Load data ──
        dd_tables = load_dd_tables()
        if dd_tables.empty:
            st.info("No data yet. Click **Scan Database** to start.")
            st.stop()

        total_tables = len(dd_tables)
        described = dd_tables["description"].notna().sum() if "description" in dd_tables.columns else 0
        tbl_coverage = round(described / total_tables * 100) if total_tables else 0

        col1, col2 = st.columns(2)
        col1.metric("Tables", total_tables)
        col2.metric("Coverage", f"{tbl_coverage}%")

        st.divider()

        # ── Table list ──
        _render_table_list(dd_tables, total_tables)

        st.divider()

        # ── Export ──
        st.download_button(
            "📥 Export JSON",
            data=export_json(),
            file_name="data_dictionary.json",
            mime="application/json",
            use_container_width=True,
        )

        # ── Fabric overrides ──
        if is_fabric():
            _render_fabric_overrides()

        # ── AI settings ──
        st.divider()
        _render_ai_settings(ls)

    return dd_tables, total_tables, tbl_coverage


def _render_scan_section():
    """Scan Database button with progress and stop support."""
    if "scanning" not in st.session_state:
        st.session_state.scanning = False

    scan_col1, scan_col2 = st.columns([3, 1])
    with scan_col1:
        start_scan = st.button(
            "🔄 Scan Database", use_container_width=True, type="primary",
            disabled=st.session_state.scanning,
            help="Scan all tables and columns from the Lakehouse",
        )
    with scan_col2:
        stop_scan = st.button(
            "⏹", use_container_width=True,
            disabled=not st.session_state.scanning,
            help="Stop scanning",
        )

    if stop_scan:
        st.session_state.scanning = False
        st.rerun()
    if start_scan:
        st.session_state.scanning = True
        st.rerun()

    if st.session_state.scanning:
        tables_to_scan = list_tables()
        all_tables, all_columns = [], []
        progress = st.progress(0, text="Loading metadata...")
        stopped = False

        all_cols_df, all_pks = batch_load_metadata()

        for i, name in enumerate(tables_to_scan):
            if not st.session_state.scanning:
                stopped = True
                break
            progress.progress(
                (i + 1) / len(tables_to_scan),
                text=f"Scanning {name}... ({i+1}/{len(tables_to_scan)})",
            )
            try:
                tbl_meta, cols_df = scan_one_table(name, all_cols_df)
                all_tables.append(tbl_meta)
                col_results = scan_columns(name, cols_df, pks=all_pks.get(name, set()))
                all_columns.extend(col_results)
            except Exception as e:
                st.warning(f"Skip {name}: {e}")

        progress.empty()
        st.session_state.scanning = False

        if all_tables:
            if is_fabric():
                code = generate_scan_code(all_tables, all_columns)
                st.session_state["_scan_code"] = code
                msg = f"Scanned {len(all_tables)} tables, {len(all_columns)} columns."
                if stopped:
                    msg += " (stopped early)"
                msg += " Download code below."
                st.success(msg)
            else:
                save_scan_results(all_tables, all_columns)
                msg = f"Scanned {len(all_tables)} tables, {len(all_columns)} columns"
                if stopped:
                    msg += " (stopped early — partial results saved)"
                st.success(msg)
            clear_query_caches()
        elif stopped:
            st.info("Scan stopped before any tables were scanned.")
        st.rerun()

    # Download scan code (Fabric)
    if is_fabric() and "_scan_code" in st.session_state:
        st.download_button(
            "📥 Download Scan Code",
            data=st.session_state["_scan_code"],
            file_name="dd_scan_results.py",
            mime="text/x-python",
            use_container_width=True,
        )
        with st.expander("Preview scan code"):
            st.code(st.session_state["_scan_code"], language="python")
        if st.button("🗑 Clear scan code", use_container_width=True):
            del st.session_state["_scan_code"]
            st.rerun()


def _render_cleanup_section():
    """Cleanup stale records button."""
    if st.button("🧹 Cleanup Stale Records", use_container_width=True,
                 help="Remove tables/columns no longer in the Lakehouse"):
        try:
            stale_tables, stale_columns = find_stale_records()
            if not stale_tables and not stale_columns:
                st.info("No stale records found. Everything is up to date.")
            elif is_fabric():
                code = generate_cleanup_code(stale_tables, stale_columns)
                st.session_state["_cleanup_code"] = code
                st.warning(
                    f"Found {len(stale_tables)} stale tables, "
                    f"{len(stale_columns)} stale columns. Download code below."
                )
                st.rerun()
            else:
                delete_stale_local(stale_tables, stale_columns)
                clear_query_caches()
                st.success(f"Removed {len(stale_tables)} tables, {len(stale_columns)} columns")
                st.rerun()
        except Exception as e:
            st.error(f"Cleanup failed: {e}")

    # Download cleanup code (Fabric)
    if is_fabric() and "_cleanup_code" in st.session_state:
        st.download_button(
            "📥 Download Cleanup Code",
            data=st.session_state["_cleanup_code"],
            file_name="dd_cleanup.py",
            mime="text/x-python",
            use_container_width=True,
        )
        with st.expander("Preview cleanup code"):
            st.code(st.session_state["_cleanup_code"], language="python")
        if st.button("🗑 Clear cleanup code", use_container_width=True):
            del st.session_state["_cleanup_code"]
            st.rerun()


def _render_table_list(dd_tables: pd.DataFrame, total_tables: int):
    """Searchable, filterable table list."""
    with st.expander(f"📋 Tables ({total_tables})", expanded=True):
        search = st.text_input("🔍 Search tables", placeholder="Type to filter...")
        layers = sorted(dd_tables["layer"].unique().tolist())
        layer_filter = st.multiselect("Filter by layer", layers, default=layers)

        filtered = dd_tables[dd_tables["layer"].isin(layer_filter)]
        if search:
            mask = (
                filtered["table_name"].str.contains(search, case=False, na=False)
                | filtered["description"].str.contains(search, case=False, na=False)
            )
            filtered = filtered[mask]

        table_names = filtered["table_name"].tolist()
        if table_names:
            labels = []
            for _, row in filtered.iterrows():
                layer_tag = row["layer"].upper()
                name = row["table_name"]
                rc = safe_get(row, "row_count")
                rows = f"{int(rc):,}" if rc is not None else "?"
                labels.append(f"[{layer_tag}] {name} ({rows} rows)")

            selected_idx = st.radio(
                "Tables",
                range(len(table_names)),
                format_func=lambda i: labels[i],
                label_visibility="collapsed",
            )
            st.session_state.selected_table = table_names[selected_idx]
        else:
            st.warning("No tables match your filter.")
            st.session_state.selected_table = None


def _render_fabric_overrides():
    """Show pending overrides count and download button."""
    ov = load_overrides()
    n_tbl = len(ov["tables"])
    n_col = len(ov["columns"])
    if n_tbl or n_col:
        st.divider()
        st.warning(f"Pending edits: {n_tbl} tables, {n_col} columns")
        code = generate_edit_code()
        if code:
            st.download_button(
                "📋 Download Code for Fabric",
                data=code,
                file_name="dd_updates.py",
                mime="text/x-python",
                use_container_width=True,
            )
            with st.expander("Preview generated code"):
                st.code(code, language="python")
            if st.button("🗑 Clear pending edits", use_container_width=True):
                clear_overrides()
                st.rerun()


def _render_ai_settings(ls: LocalStorage):
    """Groq API key management."""
    saved_key = load_groq_key()
    with st.expander("🤖 AI Settings", expanded=not saved_key):
        if saved_key:
            st.success("API key configured ✓")
            if st.button("Change API Key"):
                st.session_state.pop("_groq_key", None)
                save_groq_key(ls, "")
                st.rerun()
        else:
            new_key = st.text_input(
                "Groq API Key",
                placeholder="gsk_...",
                help="Free key: https://console.groq.com/keys",
            )
            if new_key:
                save_groq_key(ls, new_key)
                st.success("API key saved! Reload page to apply.")
