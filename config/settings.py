"""
Application settings: environment configs, layer definitions, constants.
"""

import os

import pandas as pd
import pyodbc

# ════════════════════════════════════════
# ODBC Driver Detection
# ════════════════════════════════════════
ODBC_DRIVER: str | None = None
for _drv in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]:
    if _drv in pyodbc.drivers():
        ODBC_DRIVER = _drv
        break

# ════════════════════════════════════════
# Environment Configs
# ════════════════════════════════════════
FABRIC_SERVER = os.environ.get("FABRIC_SERVER", "")
FABRIC_DATABASE = os.environ.get("FABRIC_DATABASE", "")
LOCAL_SERVER = os.environ.get("LOCAL_SERVER", ".")
LOCAL_DATABASE = os.environ.get("LOCAL_DATABASE", "")

ENV_CONFIGS = {
    "fabric_dev": {
        "label": "Fabric - Dev",
        "fabric": True,
        "database": FABRIC_DATABASE,
        "odbc": (
            f"DRIVER={{{ODBC_DRIVER}}};"
            f"Server={FABRIC_SERVER},1433;"
            f"Database={FABRIC_DATABASE};"
            "Encrypt=yes;"
            "TrustServerCertificate=no"
        ),
    },
    "fabric_prod": {
        "label": "Fabric - Prod",
        "fabric": True,
        "database": FABRIC_DATABASE,
        "odbc": (
            f"DRIVER={{{ODBC_DRIVER}}};"
            f"Server={FABRIC_SERVER},1433;"
            f"Database={FABRIC_DATABASE};"
            "Encrypt=yes;"
            "TrustServerCertificate=no"
        ),
    },
    "local": {
        "label": "Local SQL Server",
        "fabric": False,
        "odbc": (
            "DRIVER={SQL Server};"
            f"Server={LOCAL_SERVER};"
            f"Database={LOCAL_DATABASE};"
            "Trusted_Connection=yes;"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
            "Command Timeout=0"
        ),
    },
}

# ════════════════════════════════════════
# Layer Definitions
# ════════════════════════════════════════
LAYER_PREFIXES = {
    "brz": "brz", "slv": "slv", "gld": "gld", "ref": "ref",
    "dq": "dq", "dd": "dd", "utl": "utl",
}

EXCLUDE_PREFIXES = ["dd_", "brz2"]

LAYER_COLORS = {
    "brz": "#fbbf24", "slv": "#7dd3fc", "gld": "#fde047",
    "ref": "#86efac", "dq": "#c084fc", "utl": "#94a3b8", "other": "#94a3b8",
}


def detect_layer(table_name: str) -> str:
    """Infer medallion layer from table name prefix."""
    for prefix, layer in LAYER_PREFIXES.items():
        if table_name.startswith(prefix + "_"):
            return layer
    return "other"


def safe_get(row, key, default=None):
    """Safely get a value from a pandas row, returning default if key missing or NaN."""
    try:
        val = row[key]
        return default if pd.isna(val) else val
    except (KeyError, IndexError):
        return default
