"""
AI-powered metadata suggestions using Groq API.
"""

import json

import pandas as pd
import streamlit as st
from streamlit_local_storage import LocalStorage

from config.settings import safe_get


GROQ_MODEL = "llama-3.3-70b-versatile"
_GROQ_LS_KEY = "dd_groq_api_key"


def load_groq_key() -> str:
    """Load Groq API key from session state."""
    return st.session_state.get("_groq_key", "")


def save_groq_key(ls: LocalStorage, key: str):
    """Save Groq API key to browser LocalStorage and session state."""
    key = key.strip()
    ls.setItem(_GROQ_LS_KEY, key)
    st.session_state["_groq_key"] = key


def _call_ai(prompt: str, max_tokens: int = 2000) -> str:
    """Call Groq API."""
    import requests as req
    api_key = load_groq_key()
    if not api_key:
        raise ValueError("No API key. Enter your Groq API key in the sidebar.")
    r = req.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": GROQ_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": 0.2,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()


def _parse_json_response(text: str):
    """Extract JSON from AI response (handles markdown code blocks)."""
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    return json.loads(text)


def suggest_table(table_name: str, layer: str, columns_df: pd.DataFrame) -> dict:
    """Ask AI to suggest table metadata."""
    col_info = []
    for _, row in columns_df.head(30).iterrows():
        parts = [f"{row.get('column_name', '?')} ({row.get('data_type', '?')})"]
        sample = safe_get(row, "sample_values")
        if sample:
            parts.append(f"samples: {sample}")
        col_info.append(" - ".join(parts))

    prompt = f"""You are a data dictionary assistant for a supply chain analytics lakehouse.

Table: {table_name}
Layer: {layer} (brz=raw/bronze, slv=cleaned/silver, gld=aggregated/gold, ref=reference/master)
Columns:
{chr(10).join(col_info)}

Based on the table name, layer, column names, data types and sample values, suggest:
1. description: A concise English business description (1-2 sentences)
2. business_owner: Which team likely owns this (e.g. "Supply Chain", "Sales", "Finance", "Analytics")
3. source_system: The likely source system
4. tags: Comma-separated relevant tags
5. refresh_frequency: One of: hourly, daily, weekly, monthly, ad-hoc

Respond in JSON format only, no explanation:
{{"description": "...", "business_owner": "...", "source_system": "...", "tags": "...", "refresh_frequency": "..."}}"""

    return _parse_json_response(_call_ai(prompt, 500))


def suggest_columns(table_name: str, layer: str, columns_df: pd.DataFrame) -> list[dict]:
    """Ask AI to suggest column descriptions and business names."""
    col_info = []
    for _, row in columns_df.iterrows():
        parts = [row.get("column_name", "?"), row.get("data_type", "?")]
        sample = safe_get(row, "sample_values")
        if sample:
            parts.append(f"samples: {sample}")
        null_pct = safe_get(row, "null_percentage")
        if null_pct is not None:
            parts.append(f"null: {null_pct}%")
        col_info.append(" | ".join(parts))

    prompt = f"""You are a data dictionary assistant for a supply chain analytics lakehouse.

Table: {table_name} (layer: {layer})
Columns:
{chr(10).join(col_info)}

Column naming convention:
- id_* = identifiers/keys, code_* = category codes, name_* = descriptive text
- dt_* = date, ts_* = timestamp, amt_* = monetary, qty_* = quantity
- num_* = count, val_* = values, pct_* = percentage, is_* = boolean, sk_* = surrogate key

For each column, suggest:
- description: Concise English description
- business_name: Human-readable English business name

Respond as a JSON array only, no explanation:
[{{"column_name": "...", "description": "...", "business_name": "..."}}, ...]"""

    return _parse_json_response(_call_ai(prompt, 2000))
