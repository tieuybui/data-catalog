"""
Data export functionality.
"""

import json

from services.metadata import load_dd_tables, load_dd_columns


def export_json() -> str:
    """Export the full data dictionary as JSON."""
    tables = load_dd_tables()
    columns = load_dd_columns()
    result = []
    for _, row in tables.iterrows():
        t = row.to_dict()
        tbl_cols = columns[columns["table_name"] == row["table_name"]]
        t["columns"] = tbl_cols.to_dict("records")
        result.append(t)
    return json.dumps(result, indent=2, default=str)
