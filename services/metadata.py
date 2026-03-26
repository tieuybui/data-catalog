"""
CRUD operations for dd_tables and dd_columns.
Handles loading, saving, updating metadata and cached queries.
"""

import pandas as pd
import streamlit as st
from datetime import datetime

from config.settings import FABRIC_DATABASE
from core.database import (
    run_query, run_non_query, run_non_query_params, is_fabric,
)
from services.overrides import (
    apply_table_overrides, apply_column_overrides,
    save_table_override, save_column_override,
)


# ════════════════════════════════════════
# DDL: Ensure dd_tables & dd_columns exist
# ════════════════════════════════════════
def ensure_dd_tables():
    """Create dd_tables/dd_columns if they don't exist (local SQL Server only)."""
    if is_fabric():
        last_err = None
        for prefix in ("dbo.", f"{FABRIC_DATABASE}.dbo.", ""):
            try:
                run_query(f"SELECT TOP 1 table_name FROM {prefix}dd_tables")
                return
            except Exception as e:
                last_err = e
                continue
        st.warning(
            f"Cannot read `dd_tables` from Fabric. Error: `{last_err}`\n\n"
            "Possible causes:\n"
            "- Tables `dd_tables`/`dd_columns` not created yet → run `nb_dd_run` notebook in Fabric\n"
            "- Connection issue → check ODBC Driver 18, Azure AD login\n"
            "- SQL endpoint not enabled for this Lakehouse"
        )
        return

    run_non_query("""
        IF OBJECT_ID('dbo.dd_tables', 'U') IS NULL
        CREATE TABLE dbo.dd_tables (
            table_name        NVARCHAR(255) NOT NULL PRIMARY KEY,
            layer             NVARCHAR(50)  NOT NULL,
            row_count         BIGINT,
            column_count      INT,
            description       NVARCHAR(MAX),
            business_owner    NVARCHAR(255),
            source_system     NVARCHAR(255),
            refresh_frequency NVARCHAR(100),
            tags              NVARCHAR(MAX),
            scanned_at        NVARCHAR(50)  NOT NULL,
            updated_at        NVARCHAR(50),
            updated_by        NVARCHAR(255)
        )
    """)
    run_non_query("""
        IF OBJECT_ID('dbo.dd_columns', 'U') IS NULL
        CREATE TABLE dbo.dd_columns (
            table_name       NVARCHAR(255) NOT NULL,
            column_name      NVARCHAR(255) NOT NULL,
            data_type        NVARCHAR(100) NOT NULL,
            ordinal_position INT           NOT NULL,
            is_nullable      NVARCHAR(10),
            is_primary_key   BIT DEFAULT 0,
            description      NVARCHAR(MAX),
            business_name    NVARCHAR(255),
            sample_values    NVARCHAR(MAX),
            null_percentage  FLOAT,
            distinct_count   BIGINT,
            scanned_at       NVARCHAR(50)  NOT NULL,
            updated_at       NVARCHAR(50),
            updated_by       NVARCHAR(255),
            PRIMARY KEY (table_name, column_name)
        )
    """)


# ════════════════════════════════════════
# Existing metadata (for merge on re-scan)
# ════════════════════════════════════════
def load_existing_table_descs() -> dict:
    try:
        df = run_query("""
            SELECT table_name, description, business_owner, source_system,
                   refresh_frequency, tags, updated_at, updated_by
            FROM dbo.dd_tables
        """)
        return {row["table_name"]: row.to_dict() for _, row in df.iterrows()}
    except Exception:
        return {}


def load_existing_col_descs() -> dict:
    try:
        df = run_query("""
            SELECT table_name, column_name, description, business_name,
                   is_primary_key, updated_at, updated_by
            FROM dbo.dd_columns
        """)
        return {
            (row["table_name"], row["column_name"]): row.to_dict()
            for _, row in df.iterrows()
        }
    except Exception:
        return {}


def merge_existing_metadata(tables: list[dict], columns: list[dict]):
    """Merge existing descriptions/metadata into freshly scanned data.
    Preserves user edits (description, owner, tags, etc.) across re-scans.
    """
    old_tbl = load_existing_table_descs()
    old_col = load_existing_col_descs()

    for t in tables:
        if t["table_name"] in old_tbl:
            old = old_tbl[t["table_name"]]
            for f in ("description", "business_owner", "source_system",
                       "refresh_frequency", "tags", "updated_at", "updated_by"):
                t[f] = old.get(f) or t.get(f)

    for c in columns:
        key = (c["table_name"], c["column_name"])
        if key in old_col:
            old = old_col[key]
            for f in ("description", "business_name", "is_primary_key",
                       "updated_at", "updated_by"):
                c[f] = old.get(f) or c.get(f)


# ════════════════════════════════════════
# Cached queries (1 hour TTL)
# ════════════════════════════════════════
@st.cache_data(ttl=3600, show_spinner=False)
def _query_dd_tables(env: str) -> pd.DataFrame:
    """Cached query for dd_tables (1 hour TTL)."""
    return run_query("SELECT * FROM dbo.dd_tables ORDER BY layer, table_name")


@st.cache_data(ttl=3600, show_spinner=False)
def _query_dd_columns(env: str, table_name: str | None = None) -> pd.DataFrame:
    """Cached query for dd_columns (1 hour TTL)."""
    if table_name:
        return run_query(
            f"SELECT * FROM dbo.dd_columns WHERE table_name = '{table_name}' "
            f"ORDER BY ordinal_position"
        )
    return run_query("SELECT * FROM dbo.dd_columns ORDER BY table_name, ordinal_position")


@st.cache_data(ttl=3600, show_spinner="Loading column stats...")
def load_column_stats(env: str, table_name: str) -> pd.DataFrame:
    """On-demand: load null%, distinct count, sample values for a single table."""
    dd_cols = _query_dd_columns(env, table_name)
    if dd_cols.empty:
        return dd_cols

    col_names = dd_cols["column_name"].tolist()
    row_count_df = run_query(f"SELECT COUNT(*) AS cnt FROM [dbo].[{table_name}]")
    row_count = int(row_count_df["cnt"].iloc[0])

    stats_row = None
    if row_count > 0 and col_names:
        agg_parts = []
        for cn in col_names:
            agg_parts.append(
                f"SUM(CASE WHEN [{cn}] IS NULL THEN 1 ELSE 0 END) AS [{cn}__nulls], "
                f"COUNT(DISTINCT [{cn}]) AS [{cn}__dist]"
            )
        try:
            stats = run_query(f"SELECT {', '.join(agg_parts)} FROM [dbo].[{table_name}]")
            stats_row = stats.iloc[0]
        except Exception:
            pass

    samples = {}
    if col_names:
        try:
            sample_parts = []
            for cn in col_names:
                safe = cn.replace(' ', '_').replace('.', '_')
                sample_parts.append(
                    f"(SELECT STRING_AGG(val, ' | ') FROM "
                    f"(SELECT DISTINCT TOP 5 CAST([{cn}] AS NVARCHAR(200)) AS val "
                    f"FROM [dbo].[{table_name}] WHERE [{cn}] IS NOT NULL) AS s_{safe}) "
                    f"AS [{cn}__sample]"
                )
            sample_df = run_query(f"SELECT {', '.join(sample_parts)}")
            if not sample_df.empty:
                sample_row = sample_df.iloc[0]
                for cn in col_names:
                    val = sample_row.get(f"{cn}__sample")
                    if pd.notna(val) and str(val).strip():
                        samples[cn] = str(val)
        except Exception:
            pass

    df = dd_cols.copy()
    for idx, row in df.iterrows():
        cn = row["column_name"]
        if stats_row is not None and row_count > 0:
            try:
                nulls = int(stats_row[f"{cn}__nulls"])
                df.at[idx, "null_percentage"] = round(nulls / row_count * 100, 2)
                df.at[idx, "distinct_count"] = int(stats_row[f"{cn}__dist"])
            except Exception:
                pass
        if cn in samples:
            df.at[idx, "sample_values"] = samples[cn]

    return df


# ════════════════════════════════════════
# Public load/update API
# ════════════════════════════════════════
def load_dd_tables() -> pd.DataFrame:
    try:
        df = _query_dd_tables(st.session_state.env)
        if is_fabric():
            df = apply_table_overrides(df)
        return df
    except Exception:
        return pd.DataFrame()


def load_dd_columns(table_name: str | None = None) -> pd.DataFrame:
    try:
        df = _query_dd_columns(st.session_state.env, table_name)
        if is_fabric():
            df = apply_column_overrides(df)
        return df
    except Exception:
        return pd.DataFrame()


def update_table_fields(table_name: str, fields: dict):
    """Update table metadata — queues locally for Fabric, writes to DB for local."""
    if is_fabric():
        save_table_override(table_name, fields)
        return
    now = datetime.now().isoformat()
    fields["updated_at"] = now
    set_parts = ", ".join(f"{k} = :{k}" for k in fields)
    fields["_table_name"] = table_name
    run_non_query_params(
        f"UPDATE dbo.dd_tables SET {set_parts} WHERE table_name = :_table_name",
        fields,
    )


def update_column_fields(table_name: str, column_name: str, fields: dict):
    """Update column metadata — queues locally for Fabric, writes to DB for local."""
    if is_fabric():
        save_column_override(table_name, column_name, fields)
        return
    now = datetime.now().isoformat()
    fields["updated_at"] = now
    set_parts = ", ".join(f"{k} = :{k}" for k in fields)
    fields["_table_name"] = table_name
    fields["_column_name"] = column_name
    run_non_query_params(
        f"UPDATE dbo.dd_columns SET {set_parts} "
        f"WHERE table_name = :_table_name AND column_name = :_column_name",
        fields,
    )


def clear_query_caches():
    """Clear all cached queries (call after data changes)."""
    _query_dd_tables.clear()
    _query_dd_columns.clear()
    load_column_stats.clear()
