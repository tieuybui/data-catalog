"""
Fabric notebook code generation.
Generates Python/Spark SQL code for syncing data to Fabric Lakehouse.
"""

import json
from datetime import datetime

from config.settings import FABRIC_DATABASE
from core.database import run_query, sql_escape
from services.metadata import merge_existing_metadata
from services.overrides import load_overrides


def _get_fabric_table_columns(table_name: str) -> set[str]:
    """Get actual column names of a table on Fabric."""
    try:
        df = run_query(f"""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
        """)
        return set(df["COLUMN_NAME"].str.lower().tolist())
    except Exception:
        return set()


def _build_schema(keys: list[str], spark_types: dict) -> str:
    """Build a PySpark StructType schema string."""
    fields = []
    for k in keys:
        spark_type = spark_types.get(k, "StringType()")
        fields.append(f"    StructField('{k}', {spark_type}, True)")
    return "StructType([\n" + ",\n".join(fields) + "\n])"


def generate_scan_code(tables: list[dict], columns: list[dict]) -> str:
    """Generate Fabric notebook code for upserting scanned results."""
    merge_existing_metadata(tables, columns)

    db = f"{FABRIC_DATABASE}.dbo"

    tbl_actual_cols = _get_fabric_table_columns("dd_tables")
    col_actual_cols = _get_fabric_table_columns("dd_columns")

    all_tbl_keys = ["table_name", "layer", "row_count", "column_count", "description",
                    "business_owner", "source_system", "refresh_frequency", "tags",
                    "scanned_at", "updated_at", "updated_by"]
    if tbl_actual_cols:
        all_tbl_keys = [k for k in all_tbl_keys if k.lower() in tbl_actual_cols]

    all_col_keys = ["table_name", "column_name", "data_type", "ordinal_position",
                    "is_nullable", "is_primary_key", "description", "business_name",
                    "sample_values", "null_percentage", "distinct_count", "scanned_at",
                    "updated_at", "updated_by"]
    if col_actual_cols:
        all_col_keys = [k for k in all_col_keys if k.lower() in col_actual_cols]

    for t in tables:
        for k in all_tbl_keys:
            t.setdefault(k, None)
    for c in columns:
        for k in all_col_keys:
            c.setdefault(k, None)

    tbl_update_keys = [k for k in all_tbl_keys if k != "table_name"]
    tbl_sets = ", ".join(f"target.{k} = source.{k}" for k in tbl_update_keys)
    tbl_cols = ", ".join(all_tbl_keys)
    tbl_src_cols = ", ".join(f"source.{k}" for k in all_tbl_keys)

    col_update_keys = [k for k in all_col_keys if k not in ("table_name", "column_name")]
    col_sets = ", ".join(f"target.{k} = source.{k}" for k in col_update_keys)
    col_cols = ", ".join(all_col_keys)
    col_src_cols = ", ".join(f"source.{k}" for k in all_col_keys)

    for c in columns:
        if "is_primary_key" in c:
            c["is_primary_key"] = bool(c["is_primary_key"])

    tbl_data_str = json.dumps([{k: t[k] for k in all_tbl_keys} for t in tables], default=str)
    col_data_str = json.dumps([{k: c[k] for k in all_col_keys} for c in columns], default=str)

    spark_types = {
        "row_count": "LongType()", "column_count": "IntegerType()",
        "ordinal_position": "IntegerType()", "is_primary_key": "BooleanType()",
        "null_percentage": "DoubleType()", "distinct_count": "LongType()",
    }
    tbl_schema_str = _build_schema(all_tbl_keys, spark_types)
    col_schema_str = _build_schema(all_col_keys, spark_types)

    lines = [
        "# ════════════════════════════════════════",
        "# Auto-generated SCAN results from Data Dictionary App",
        "# Paste into a Fabric notebook cell and run",
        "# ════════════════════════════════════════",
        "import json",
        "from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, BooleanType, DoubleType",
        "",
        "# ── Tables ──",
        f"tbl_data = json.loads('''{tbl_data_str}''')",
        f"tbl_schema = {tbl_schema_str}",
        "tbl_df = spark.createDataFrame(tbl_data, schema=tbl_schema)",
        "tbl_df.createOrReplaceTempView('_dd_scan_tables')",
        f'spark.sql("""',
        f"    MERGE INTO {db}.dd_tables AS target",
        f"    USING _dd_scan_tables AS source",
        f"    ON target.table_name = source.table_name",
        f"    WHEN MATCHED THEN UPDATE SET {tbl_sets}",
        f"    WHEN NOT MATCHED THEN INSERT ({tbl_cols}) VALUES ({tbl_src_cols})",
        f'""")',
        f"print(f'Updated {{len(tbl_data)}} tables')",
        "",
        "# ── Columns ──",
        f"col_data = json.loads('''{col_data_str}''')",
        f"col_schema = {col_schema_str}",
        "col_df = spark.createDataFrame(col_data, schema=col_schema)",
        "col_df.createOrReplaceTempView('_dd_scan_columns')",
        f'spark.sql("""',
        f"    MERGE INTO {db}.dd_columns AS target",
        f"    USING _dd_scan_columns AS source",
        f"    ON target.table_name = source.table_name AND target.column_name = source.column_name",
        f"    WHEN MATCHED THEN UPDATE SET {col_sets}",
        f"    WHEN NOT MATCHED THEN INSERT ({col_cols}) VALUES ({col_src_cols})",
        f'""")',
        f"print(f'Updated {{len(col_data)}} columns')",
        "",
        f"print('Done: {len(tables)} tables, {len(columns)} columns')",
    ]
    return "\n".join(lines)


def generate_cleanup_code(stale_tables: list[str], stale_columns: list[tuple[str, str]]) -> str:
    """Generate Fabric notebook code to delete stale records."""
    db = f"{FABRIC_DATABASE}.dbo"
    lines = [
        "# ════════════════════════════════════════",
        "# Auto-generated CLEANUP from Data Dictionary App",
        "# Removes tables/columns no longer in the Lakehouse",
        "# ════════════════════════════════════════",
        "",
    ]

    if stale_tables:
        in_list = ", ".join(f"'{sql_escape(tn)}'" for tn in stale_tables)
        lines.append(f"# Remove {len(stale_tables)} stale tables")
        lines.append(f"spark.sql(\"DELETE FROM {db}.dd_tables WHERE table_name IN ({in_list})\")")
        lines.append(f"spark.sql(\"DELETE FROM {db}.dd_columns WHERE table_name IN ({in_list})\")")
        lines.append(f"print('Removed {len(stale_tables)} stale tables')")
        lines.append("")

    seen_tables = set(stale_tables)
    orphan_cols: dict[str, list[str]] = {}
    for tn, cn in stale_columns:
        if tn not in seen_tables:
            orphan_cols.setdefault(tn, []).append(cn)

    if orphan_cols:
        total_cols = sum(len(v) for v in orphan_cols.values())
        lines.append(f"# Remove {total_cols} stale columns from {len(orphan_cols)} tables")
        for tn, cols in orphan_cols.items():
            safe_tn = sql_escape(tn)
            col_list = ", ".join(f"'{sql_escape(cn)}'" for cn in cols)
            lines.append(
                f"spark.sql(\"DELETE FROM {db}.dd_columns "
                f"WHERE table_name = '{safe_tn}' AND column_name IN ({col_list})\")"
            )
        lines.append(f"print('Removed {total_cols} stale columns')")
        lines.append("")

    lines.append(f"print('Cleanup done: {len(stale_tables)} tables, {len(stale_columns)} columns removed')")
    return "\n".join(lines)


def generate_edit_code() -> str | None:
    """Generate Fabric notebook code from queued local overrides.

    Uses batch MERGE via Spark DataFrames + temp views instead of
    one MERGE per row — runs in 2 SQL statements regardless of row count.
    """
    ov = load_overrides()
    if not ov["tables"] and not ov["columns"]:
        return None

    db = f"{FABRIC_DATABASE}.dbo"
    now = datetime.now().isoformat()
    lines = [
        "# ════════════════════════════════════════",
        "# Auto-generated from Data Dictionary App",
        "# Paste into a Fabric notebook cell and run",
        "# ════════════════════════════════════════",
        "import json",
        "from pyspark.sql.types import StructType, StructField, StringType",
        "",
    ]

    # ── Table updates ──
    _skip_keys = {"updated_at", "_table_name", "_column_name"}
    tbl_rows = []
    tbl_update_fields = set()
    for tbl, fields in ov.get("tables", {}).items():
        clean = {k: v for k, v in fields.items() if k not in _skip_keys and v}
        if not clean:
            continue
        clean["table_name"] = tbl
        clean["updated_at"] = now
        tbl_rows.append(clean)
        tbl_update_fields.update(k for k in clean if k != "table_name")

    if tbl_rows:
        all_tbl_keys = ["table_name"] + sorted(tbl_update_fields)
        for row in tbl_rows:
            for k in all_tbl_keys:
                row.setdefault(k, None)

        tbl_data_str = json.dumps([{k: r[k] for k in all_tbl_keys} for r in tbl_rows], default=str)
        tbl_schema = "StructType([\n" + ",\n".join(
            f"    StructField('{k}', StringType(), True)" for k in all_tbl_keys
        ) + "\n])"
        tbl_sets = ", ".join(f"target.{k} = source.{k}" for k in all_tbl_keys if k != "table_name")
        tbl_cols = ", ".join(all_tbl_keys)
        tbl_src_cols = ", ".join(f"source.{k}" for k in all_tbl_keys)

        lines.append("# ── Table updates ──")
        lines.append(f"tbl_data = json.loads('''{tbl_data_str}''')")
        lines.append(f"tbl_schema = {tbl_schema}")
        lines.append("tbl_df = spark.createDataFrame(tbl_data, schema=tbl_schema)")
        lines.append("tbl_df.createOrReplaceTempView('_dd_edit_tables')")
        lines.append(f'spark.sql("""')
        lines.append(f"    MERGE INTO {db}.dd_tables AS target")
        lines.append(f"    USING _dd_edit_tables AS source")
        lines.append(f"    ON target.table_name = source.table_name")
        lines.append(f"    WHEN MATCHED THEN UPDATE SET {tbl_sets}")
        lines.append(f"    WHEN NOT MATCHED THEN INSERT ({tbl_cols}) VALUES ({tbl_src_cols})")
        lines.append(f'""")')
        lines.append(f"print(f'Updated {{len(tbl_data)}} table(s)')")
        lines.append("spark.catalog.dropTempView('_dd_edit_tables')")
        lines.append("")

    # ── Column updates ──
    col_rows = []
    col_update_fields = set()
    for _, fields in ov.get("columns", {}).items():
        tbl = fields["table_name"]
        col = fields["column_name"]
        clean = {k: v for k, v in fields.items()
                 if k not in ("table_name", "column_name", "updated_at", "_table_name", "_column_name")
                 and v is not None}
        if not clean:
            continue
        clean["table_name"] = tbl
        clean["column_name"] = col
        clean["updated_at"] = now
        col_rows.append(clean)
        col_update_fields.update(k for k in clean if k not in ("table_name", "column_name"))

    if col_rows:
        all_col_keys = ["table_name", "column_name"] + sorted(col_update_fields)
        for row in col_rows:
            for k in all_col_keys:
                row.setdefault(k, None)

        col_data_str = json.dumps([{k: r[k] for k in all_col_keys} for r in col_rows], default=str)
        col_schema = "StructType([\n" + ",\n".join(
            f"    StructField('{k}', StringType(), True)" for k in all_col_keys
        ) + "\n])"
        col_sets = ", ".join(f"target.{k} = source.{k}" for k in all_col_keys
                             if k not in ("table_name", "column_name"))
        col_cols = ", ".join(all_col_keys)
        col_src_cols = ", ".join(f"source.{k}" for k in all_col_keys)

        lines.append("# ── Column updates ──")
        lines.append(f"col_data = json.loads('''{col_data_str}''')")
        lines.append(f"col_schema = {col_schema}")
        lines.append("col_df = spark.createDataFrame(col_data, schema=col_schema)")
        lines.append("col_df.createOrReplaceTempView('_dd_edit_columns')")
        lines.append(f'spark.sql("""')
        lines.append(f"    MERGE INTO {db}.dd_columns AS target")
        lines.append(f"    USING _dd_edit_columns AS source")
        lines.append(f"    ON target.table_name = source.table_name AND target.column_name = source.column_name")
        lines.append(f"    WHEN MATCHED THEN UPDATE SET {col_sets}")
        lines.append(f"    WHEN NOT MATCHED THEN INSERT ({col_cols}) VALUES ({col_src_cols})")
        lines.append(f'""")')
        lines.append(f"print(f'Updated {{len(col_data)}} column(s)')")
        lines.append("spark.catalog.dropTempView('_dd_edit_columns')")
        lines.append("")

    tbl_count = len(tbl_rows)
    col_count = len(col_rows)
    lines.append(f"print('Done: {tbl_count} table(s), {col_count} column(s)')")
    return "\n".join(lines)
