"""Data loading for the dashboard.

Two sources, one interface:

  live  - the DuckDB warehouse a local pipeline run produced.
  demo  - Parquet snapshots of a real run, committed to the repository.

The demo snapshot is what makes the dashboard deployable: a hosting platform has
no warehouse, no Airflow and no API access, so the app falls back to frozen
results of an actual run rather than to fabricated data. Nothing here invents
numbers; `scripts/export_demo_data.py` is the only writer.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

DEMO_DIR = PROJECT_ROOT / "data" / "demo"
WAREHOUSE_PATH = PROJECT_ROOT / "warehouse" / "nyc311.duckdb"

LIVE_QUERIES = {
    "daily_volume": """
        select created_date, borough,
               sum(requests_opened) as requests_opened,
               sum(requests_closed) as requests_closed
        from analytics.agg_daily_borough_sla
        group by created_date, borough order by created_date, borough
    """,
    "dim_agency": "select * from analytics.dim_agency order by total_requests desc",
    "agg_daily_borough_sla": "select * from analytics.agg_daily_borough_sla",
    "top_complaints": """
        select complaint_type, count(*) as requests,
               round(avg(resolution_hours), 2) as avg_resolution_hours
        from analytics.fct_service_requests
        group by complaint_type order by requests desc limit 15
    """,
    "fct_sample": """
        select request_id, created_at, closed_at, agency_code, complaint_type,
               descriptor, borough, status, resolution_hours, has_invalid_timestamps,
               source_system, batch_date
        from analytics.fct_service_requests order by created_at desc limit 8000
    """,
    "timestamp_anomalies": """
        select request_id, agency_code, complaint_type, created_at, closed_at, borough
        from analytics.fct_service_requests where has_invalid_timestamps order by created_at
    """,
}


def warehouse_available() -> bool:
    """True when a local DuckDB warehouse exists and duckdb is importable."""
    if not WAREHOUSE_PATH.exists():
        return False
    try:
        import duckdb  # noqa: F401
    except ImportError:
        return False
    return True


@st.cache_data(show_spinner=False)
def load_manifest() -> dict:
    path = DEMO_DIR / "manifest.json"
    return json.loads(path.read_text()) if path.exists() else {}


@st.cache_data(show_spinner=False)
def load_table(name: str, source: str) -> pd.DataFrame:
    """Load one named table from the chosen source, falling back to the snapshot."""
    if source == "live" and warehouse_available() and name in LIVE_QUERIES:
        import duckdb
        with duckdb.connect(str(WAREHOUSE_PATH), read_only=True) as con:
            return con.execute(LIVE_QUERIES[name]).fetchdf()

    path = DEMO_DIR / f"{name}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


@st.cache_data(show_spinner=False)
def load_row_counts(source: str) -> pd.DataFrame:
    """Row counts at each pipeline stage - the funnel that proves nothing is lost."""
    if source == "live" and warehouse_available():
        import duckdb
        stages = {
            "1. landed (raw)": "raw.service_requests_raw",
            "2. staged (typed)": "staging.stg_service_requests",
            "3. deduped": "staging.int_service_requests_deduped",
            "4. fact": "analytics.fct_service_requests",
        }
        with duckdb.connect(str(WAREHOUSE_PATH), read_only=True) as con:
            return pd.DataFrame([
                {"stage": label, "relation": rel,
                 "rows": con.execute(f"select count(*) from {rel}").fetchone()[0]}
                for label, rel in stages.items()
            ])
    return load_table("row_counts", "demo")


@st.cache_data(show_spinner=False)
def headline_metrics(source: str) -> dict:
    """The numbers on the hero tiles, all derived from loaded data."""
    manifest = load_manifest()
    quality = manifest.get("quality_report", {})
    dbt = quality.get("dbt_tests", {})

    row_counts = load_row_counts(source)
    fact_rows = int(row_counts.loc[row_counts["stage"].str.startswith("4"), "rows"].iloc[0]) \
        if not row_counts.empty else 0

    decisions = load_table("mapping_decisions", source)
    tiers = decisions["method"].value_counts().to_dict() if not decisions.empty else {}
    auto_total = int(sum(v for k, v in tiers.items() if k != "override"))
    auto_mapped = int(len(decisions[(decisions["method"] != "override")
                                    & decisions["canonical_field"].notna()])) if not decisions.empty else 0

    return {
        "rows_loaded": fact_rows,
        "batches": len(manifest.get("batch_dates", [])),
        "dbt_tests": dbt.get("tests_total", 0),
        "dbt_pass_rate": dbt.get("test_pass_rate"),
        "dbt_failed": dbt.get("tests_failed", 0),
        "mapping_decisions": len(decisions),
        "auto_match_rate": auto_mapped / auto_total if auto_total else None,
        "tiers": tiers,
        "pipeline_runtime_s": quality.get("pipeline_runtime_s"),
        "exported_at": manifest.get("exported_at"),
    }
