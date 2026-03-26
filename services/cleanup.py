"""
Stale record detection and cleanup.
Finds tables/columns in dd_tables/dd_columns that no longer exist in the database.
"""

from core.database import run_non_query, sql_escape
from services.scanner import list_tables
from services.metadata import load_dd_tables, load_dd_columns


def find_stale_records() -> tuple[list[str], list[tuple[str, str]]]:
    """Return (stale_tables, stale_columns) that no longer exist in the database."""
    current_tables = set(list_tables())

    dd_tables = load_dd_tables()
    stale_tables = []
    if not dd_tables.empty:
        for tn in dd_tables["table_name"].tolist():
            if tn not in current_tables:
                stale_tables.append(tn)

    dd_cols = load_dd_columns()
    stale_columns = []
    if not dd_cols.empty:
        for tn in dd_cols["table_name"].unique():
            if tn not in current_tables:
                for cn in dd_cols[dd_cols["table_name"] == tn]["column_name"].tolist():
                    stale_columns.append((tn, cn))

    return stale_tables, stale_columns


def delete_stale_local(stale_tables: list[str], stale_columns: list[tuple[str, str]]):
    """Delete stale records directly from local SQL Server."""
    for tn in stale_tables:
        run_non_query(f"DELETE FROM dbo.dd_columns WHERE table_name = '{sql_escape(tn)}'")
        run_non_query(f"DELETE FROM dbo.dd_tables WHERE table_name = '{sql_escape(tn)}'")

    for tn, cn in stale_columns:
        if tn not in stale_tables:
            run_non_query(
                f"DELETE FROM dbo.dd_columns "
                f"WHERE table_name = '{sql_escape(tn)}' AND column_name = '{sql_escape(cn)}'"
            )
