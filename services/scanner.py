"""
Database scanner: reads table/column structure from the Lakehouse.
"""

import pandas as pd
from datetime import datetime

from config.settings import EXCLUDE_PREFIXES, detect_layer
from core.database import run_query, run_non_query_params, is_fabric
from services.metadata import merge_existing_metadata


def list_tables() -> list[str]:
    """Get all non-excluded base tables from the database."""
    df = run_query("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """)
    names = df["TABLE_NAME"].tolist()
    return [n for n in names if not any(n.startswith(p) for p in EXCLUDE_PREFIXES)]


def batch_load_metadata() -> tuple[pd.DataFrame, dict[str, set[str]]]:
    """Load ALL column metadata and primary keys in 2 queries (instead of per-table)."""
    all_cols = run_query("""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo'
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """)

    pks: dict[str, set[str]] = {}
    try:
        pk_df = run_query("""
            SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
              ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_SCHEMA = 'dbo'
        """)
        for _, row in pk_df.iterrows():
            pks.setdefault(row["TABLE_NAME"], set()).add(row["COLUMN_NAME"])
    except Exception:
        pass

    return all_cols, pks


def scan_one_table(table_name: str, all_cols_df: pd.DataFrame = None) -> tuple[dict, pd.DataFrame]:
    """Scan a single table: get row count and column metadata."""
    now = datetime.now().isoformat()

    if all_cols_df is not None:
        cols = all_cols_df[all_cols_df["TABLE_NAME"] == table_name].copy()
    else:
        cols = run_query(f"""
            SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)

    rc = run_query(f"SELECT COUNT(*) AS cnt FROM [dbo].[{table_name}]")
    row_count = int(rc["cnt"].iloc[0])

    tbl_meta = {
        "table_name": table_name,
        "layer": detect_layer(table_name),
        "row_count": row_count,
        "column_count": len(cols),
        "scanned_at": now,
    }
    return tbl_meta, cols


def scan_columns(table_name: str, cols_df: pd.DataFrame,
                 pks: set[str] = None) -> list[dict]:
    """Extract column metadata from a columns DataFrame."""
    now = datetime.now().isoformat()
    if pks is None:
        try:
            pk_df = run_query(f"""
                SELECT kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                WHERE tc.TABLE_NAME = '{table_name}'
                  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            """)
            pks = set(pk_df["COLUMN_NAME"].tolist()) if not pk_df.empty else set()
        except Exception:
            pks = set()

    if cols_df.empty:
        return []

    cn_col = "COLUMN_NAME" if "COLUMN_NAME" in cols_df.columns else "column_name"
    dt_col = "DATA_TYPE" if "DATA_TYPE" in cols_df.columns else "data_type"
    op_col = "ORDINAL_POSITION" if "ORDINAL_POSITION" in cols_df.columns else "ordinal_position"
    in_col = "IS_NULLABLE" if "IS_NULLABLE" in cols_df.columns else "is_nullable"

    results = []
    for _, row in cols_df.iterrows():
        cn = row[cn_col]
        results.append({
            "table_name": table_name,
            "column_name": cn,
            "data_type": row[dt_col],
            "ordinal_position": int(row[op_col]),
            "is_nullable": row[in_col],
            "is_primary_key": 1 if cn in pks else 0,
            "null_percentage": None,
            "distinct_count": None,
            "sample_values": None,
            "scanned_at": now,
        })
    return results


def save_scan_results(tables: list[dict], columns: list[dict]):
    """Save scan results to local SQL Server (upsert via MERGE)."""
    merge_existing_metadata(tables, columns)

    for t in tables:
        for k in ["table_name", "layer", "row_count", "column_count", "description",
                   "business_owner", "source_system", "refresh_frequency", "tags",
                   "scanned_at", "updated_at", "updated_by"]:
            t.setdefault(k, None)

        run_non_query_params("""
            MERGE dbo.dd_tables AS target
            USING (SELECT :table_name AS table_name) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET
                layer = :layer, row_count = :row_count, column_count = :column_count,
                description = :description, business_owner = :business_owner,
                source_system = :source_system, refresh_frequency = :refresh_frequency,
                tags = :tags, scanned_at = :scanned_at,
                updated_at = :updated_at, updated_by = :updated_by
            WHEN NOT MATCHED THEN INSERT
                (table_name, layer, row_count, column_count, description, business_owner,
                 source_system, refresh_frequency, tags, scanned_at, updated_at, updated_by)
            VALUES (:table_name, :layer, :row_count, :column_count, :description,
                    :business_owner, :source_system, :refresh_frequency, :tags,
                    :scanned_at, :updated_at, :updated_by);
        """, t)

    for c in columns:
        for k in ["table_name", "column_name", "data_type", "ordinal_position",
                   "is_nullable", "is_primary_key", "description", "business_name",
                   "sample_values", "null_percentage", "distinct_count", "scanned_at",
                   "updated_at", "updated_by"]:
            c.setdefault(k, None)

        run_non_query_params("""
            MERGE dbo.dd_columns AS target
            USING (SELECT :table_name AS table_name, :column_name AS column_name) AS source
            ON target.table_name = source.table_name AND target.column_name = source.column_name
            WHEN MATCHED THEN UPDATE SET
                data_type = :data_type, ordinal_position = :ordinal_position,
                is_nullable = :is_nullable, is_primary_key = :is_primary_key,
                description = :description, business_name = :business_name,
                sample_values = :sample_values, null_percentage = :null_percentage,
                distinct_count = :distinct_count, scanned_at = :scanned_at,
                updated_at = :updated_at, updated_by = :updated_by
            WHEN NOT MATCHED THEN INSERT
                (table_name, column_name, data_type, ordinal_position, is_nullable,
                 is_primary_key, description, business_name, sample_values,
                 null_percentage, distinct_count, scanned_at, updated_at, updated_by)
            VALUES (:table_name, :column_name, :data_type, :ordinal_position,
                    :is_nullable, :is_primary_key, :description, :business_name,
                    :sample_values, :null_percentage, :distinct_count,
                    :scanned_at, :updated_at, :updated_by);
        """, c)
