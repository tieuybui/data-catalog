"""
Local JSON overrides for Fabric read-only mode.
Stores edits locally and applies them on top of database data.
"""

import json

import pandas as pd
import streamlit as st
from datetime import datetime
from pathlib import Path


OVERRIDES_FILE = Path(__file__).resolve().parent.parent / "dd_overrides.json"


def load_overrides() -> dict:
    """Load local overrides from JSON file (cached in session state)."""
    if "overrides" not in st.session_state:
        if OVERRIDES_FILE.exists():
            st.session_state.overrides = json.loads(OVERRIDES_FILE.read_text(encoding="utf-8"))
        else:
            st.session_state.overrides = {"tables": {}, "columns": {}}
    return st.session_state.overrides


def _save_overrides(data: dict):
    """Persist overrides to JSON file and update session cache."""
    OVERRIDES_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    st.session_state.overrides = data


def save_table_override(table_name: str, fields: dict):
    """Queue a table-level edit to local JSON."""
    ov = load_overrides()
    ov["tables"].setdefault(table_name, {})
    ov["tables"][table_name].update({k: v for k, v in fields.items() if v})
    ov["tables"][table_name]["updated_at"] = datetime.now().isoformat()
    _save_overrides(ov)


def save_column_override(table_name: str, column_name: str, fields: dict):
    """Queue a column-level edit to local JSON."""
    ov = load_overrides()
    key = f"{table_name}::{column_name}"
    ov["columns"].setdefault(key, {"table_name": table_name, "column_name": column_name})
    ov["columns"][key].update({k: v for k, v in fields.items() if v is not None})
    ov["columns"][key]["updated_at"] = datetime.now().isoformat()
    _save_overrides(ov)


def apply_table_overrides(df: pd.DataFrame) -> pd.DataFrame:
    """Merge local overrides into a dd_tables DataFrame."""
    ov = load_overrides()
    if not ov["tables"] or df.empty:
        return df
    df = df.copy()
    for tbl_name, fields in ov["tables"].items():
        mask = df["table_name"] == tbl_name
        if mask.any():
            for k, v in fields.items():
                if k in df.columns:
                    df.loc[mask, k] = v
    return df


def apply_column_overrides(df: pd.DataFrame) -> pd.DataFrame:
    """Merge local overrides into a dd_columns DataFrame."""
    ov = load_overrides()
    if not ov["columns"] or df.empty:
        return df
    df = df.copy()
    for _, fields in ov["columns"].items():
        tbl = fields.get("table_name")
        col = fields.get("column_name")
        mask = (df["table_name"] == tbl) & (df["column_name"] == col)
        if mask.any():
            for k, v in fields.items():
                if k in df.columns and k not in ("table_name", "column_name"):
                    df.loc[mask, k] = v
    return df


def clear_overrides():
    """Clear all pending edits after syncing to Fabric."""
    _save_overrides({"tables": {}, "columns": {}})
