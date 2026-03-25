# Data Dictionary App

Streamlit app to browse, edit and document table/column metadata from **Microsoft Fabric Lakehouse** or a local SQL Server.

## Features

- **Connect** to Fabric Lakehouse (via SQL endpoint + Azure AD) or local SQL Server
- **Browse** all tables with layer filters (BRZ / SLV / GLD / REF), search, row counts
- **Edit** table metadata: description, business owner, source system, refresh frequency, tags
- **Edit** column metadata: description, business name, primary key
- **AI Suggest** (Groq / Llama 3.3) — auto-fill descriptions based on table name, columns and sample data
- **Export** data dictionary as JSON
- **Generate Fabric code** — for Fabric (read-only SQL endpoint), edits are saved locally and exported as Spark SQL MERGE statements to paste into a Fabric notebook

## Quick Start

```bash
pip install -r requirements.txt
streamlit run data_dictionary_app.py
```

## Requirements

- Python 3.10+
- ODBC Driver 18 for SQL Server (for Fabric connection)
- Azure AD account with access to the Fabric workspace

## Fabric Connection

The app connects to the Fabric Lakehouse SQL analytics endpoint using Azure AD token authentication (`azure-identity`). On first run it will open a browser window for Microsoft login.

Prerequisite: tables `dd_tables` and `dd_columns` must exist in the Lakehouse (created by the `nb_dd_run` notebook in Fabric).

## AI Suggestions

Uses **Groq API** (free tier) with Llama 3.3 70B to suggest descriptions and business names.

1. Get a free API key at https://console.groq.com/keys
2. Enter the key in **AI Settings** in the sidebar
3. Click **AI Suggest Table Info** or **AI Suggest All Columns**

## Environments

| Environment | Connection | Write |
|-------------|-----------|-------|
| Fabric Dev | SQL endpoint + Azure AD token | Read-only (edits saved locally, export code for notebook) |
| Fabric Prod | SQL endpoint + Azure AD token | Read-only |
| Local SQL Server | ODBC + Windows Auth | Full read/write |
