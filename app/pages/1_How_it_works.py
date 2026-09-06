"""How it works - architecture, design rationale and setup.

Everything explanatory lives here so the landing page can stay a working demo.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from diagram import architecture_svg
from style import PAGE_STYLE, gap, note, page_link
from pipeline.ai_schema_mapper import load_overrides
from pipeline.canonical_schema import CANONICAL_FIELDS
from pipeline.source_dialects import DIALECTS

st.set_page_config(
    page_title="How it works · nyc311-ai-etl",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.markdown(PAGE_STYLE, unsafe_allow_html=True)

page_link("streamlit_app.py", "← Back to the demo", "🛠️")
st.markdown('<div class="hero-eyebrow">How it works</div>', unsafe_allow_html=True)
st.markdown('<h1 class="hero-title">An AI-augmented ELT pipeline that refuses to guess</h1>',
            unsafe_allow_html=True)
st.markdown(
    '<p class="hero-sub">Real NYC 311 service requests arrive daily from four upstream systems '
    'that each name their columns differently. The pipeline resolves every incoming header to a '
    'canonical schema, loads it into a warehouse, and builds tested dbt models on top — '
    'orchestrated by Airflow, incremental by batch date, safe to re-run.</p>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="chips">'
    '<span class="chip chip-accent">Apache Airflow</span><span class="chip chip-accent">dbt</span>'
    '<span class="chip chip-accent">BigQuery / DuckDB</span><span class="chip">Python</span>'
    '<span class="chip">Docker</span><span class="chip">Claude API</span>'
    '<span class="chip">Parquet</span></div>',
    unsafe_allow_html=True,
)
gap(1.0)

st.markdown("### Architecture")
st.markdown(f'<div class="svg-wrap">{architecture_svg()}</div>', unsafe_allow_html=True)
gap(1.2)

left, right = st.columns([1, 1], gap="large")
with left:
    st.markdown("### The problem")
    st.markdown(
        "The same 311 payload arrives from four systems with four naming conventions. "
        "`SR_NUMBER`, `Service Request ID`, `srId` and `request_id` are the same field. "
        "Each file also carries operational columns — checksums, batch refs, app versions — "
        "that map to **nothing**, and mapping one of those into a real field silently "
        "corrupts data."
    )
    st.dataframe(
        pd.DataFrame([
            {"canonical field": canonical,
             **{d.name: d.headers[canonical] for d in DIALECTS}}
            for canonical in ["request_id", "complaint_type", "intake_channel",
                              "resolution_updated_at"]
        ]),
        hide_index=True, width="stretch",
    )
    st.caption(
        "The rows are genuine NYC 311 Open Data. The four header dialects are synthesized in "
        "`pipeline/source_dialects.py` to simulate multi-source ingestion — that file also "
        "records the ground-truth mapping, which is what makes accuracy measurable rather "
        "than a matter of opinion."
    )

with right:
    st.markdown("### Why it is built this way")
    st.markdown(
        "- **The LLM is an optimization, not a dependency.** Outage, timeout, refusal, "
        "malformed JSON, hallucinated field, low confidence — every failure mode falls "
        "through to a deterministic tier, and each path has a unit test.\n"
        "- **The fallback declines rather than guesses.** A miss costs a null column; a "
        "mis-map writes wrong values into a real field, which is far more expensive.\n"
        "- **Required fields are a hard gate.** If a primary key does not resolve, the run "
        "fails loudly instead of loading a table with a missing key.\n"
        "- **Everything is re-runnable.** Loads replace a batch date; the fact table is "
        "incremental with `delete+insert`, so a retried task never double-counts.\n"
        "- **Orchestration holds no logic.** Every Airflow task is a thin wrapper over "
        "`python -m pipeline.cli <subcommand>`, so the pipeline runs and tests without Airflow."
    )
    note(
        "<b>Measured:</b> across 83 header decisions per run the pipeline resolved 100% of what "
        "it committed to correctly — <b>zero mis-mapped columns</b> — and correctly declined "
        "all 7 operational columns. Recall was 82.9%; the shortfall is columns it refused to "
        "guess at, which is the intended trade.",
        "note-good",
    )

gap(1.2)
st.markdown("### Where string similarity runs out")
note(
    "The fuzzy fallback rejects every operational column comfortably. What it cannot do is "
    "separate <i>semantic</i> near-misses. All three score <b>0.90</b> — one is right, two are "
    "wrong, and no threshold tells them apart, because the difference is meaning, not spelling. "
    "<b>That gap is the entire reason the LLM tier exists</b>, and why the one-field-per-column "
    "collision guard is not optional.",
    "note-warn",
)
st.dataframe(
    pd.DataFrame([
        {"column": "boroughName", "best fuzzy match": "borough", "score": 0.90,
         "correct?": "✅ right"},
        {"column": "assignedAgencyLabel", "best fuzzy match": "agency_code", "score": 0.90,
         "correct?": "❌ should be agency_name"},
        {"column": "agencyOutcomeNotes", "best fuzzy match": "agency_code", "score": 0.90,
         "correct?": "❌ should be resolution_description"},
    ]),
    hide_index=True, width="stretch",
)

gap(1.2)
st.markdown("### The dbt layer")
model_col, test_col = st.columns([1, 1], gap="large")
with model_col:
    st.dataframe(
        pd.DataFrame([
            {"layer": "staging", "model": "stg_service_requests", "materialization": "view",
             "purpose": "Type-cast and trim; a faithful typed mirror"},
            {"layer": "intermediate", "model": "int_service_requests_deduped",
             "materialization": "view", "purpose": "One row per request_id, newest ingest wins"},
            {"layer": "marts", "model": "fct_service_requests", "materialization": "incremental",
             "purpose": "Request grain, delete+insert on request_id"},
            {"layer": "marts", "model": "dim_agency", "materialization": "table",
             "purpose": "Agency dimension, most frequent spelling per acronym"},
            {"layer": "marts", "model": "agg_daily_borough_sla", "materialization": "table",
             "purpose": "Volume, closure rate and resolution stats by day/borough/agency"},
        ]),
        hide_index=True, width="stretch",
    )
with test_col:
    st.markdown(
        "**44 tests**, including two custom generic tests and three singular tests:\n\n"
        "- **`not_in_future`** *(custom)* — timestamps cannot be in the future\n"
        "- **`within_range`** *(custom)* — lat/lon inside NYC, rates in [0,1], no negative durations\n"
        "- **`relationships`** — every fact row joins to an agency in the dimension\n"
        "- **Singular** — closed-after-created invariant, aggregate row-count reconciliation, "
        "anomaly-rate observability"
    )
    note(
        "The first two do double duty: a swapped <code>created_at</code> / "
        "<code>closed_at</code> mapping would trip them, so they guard the AI mapper as well "
        "as the source data."
    )

gap(1.2)
schema_col, override_col = st.columns([1, 1], gap="large")
with schema_col:
    st.markdown("### The canonical schema")
    st.caption("The descriptions and synonyms below are the mapper's only knowledge of the "
               "target — they feed both the LLM prompt and the fuzzy corpus.")
    st.dataframe(
        pd.DataFrame([
            {"field": f.name, "type": f.dtype, "required": f.required,
             "description": f.description}
            for f in CANONICAL_FIELDS
        ]),
        hide_index=True, width="stretch", height=320,
    )
with override_col:
    st.markdown("### Human-in-the-loop overrides")
    st.caption("When no tier could resolve a required field, the run failed loudly and a person "
               "decided once. Recorded here, applied automatically since, and excluded from the "
               "auto-match rate — a column a human mapped is not a column the pipeline mapped.")
    overrides = load_overrides()
    if overrides:
        st.dataframe(
            pd.DataFrame([
                {"source system": system, "column": column, "canonical field": target}
                for system, columns in overrides.items()
                for column, target in columns.items()
            ]),
            hide_index=True, width="stretch",
        )

gap(1.2)
st.markdown("### Run it yourself")
run_left, run_right = st.columns([1, 1], gap="large")
with run_left:
    st.markdown("**Pipeline, no Docker (~2 minutes)**")
    st.code(
        "make setup                          # venv + dependencies\n"
        "make backfill                       # 14 daily batches end to end\n"
        "make report BATCH_DATE=2024-01-21   # the quality report\n"
        "make test                           # 32 tests",
        language="bash",
    )
    st.markdown("**Airflow via Docker Compose**")
    st.code(
        "make airflow-up        # http://localhost:8080 (airflow/airflow)\n"
        "make airflow-trigger   # unpause; catchup runs the 14-day window\n"
        "make airflow-down",
        language="bash",
    )
with run_right:
    st.markdown("**Enable the LLM tier**")
    st.code(
        "export ANTHROPIC_API_KEY=sk-ant-...\n"
        "make run BATCH_DATE=2024-01-15\n"
        "python scripts/mapping_accuracy.py --use-llm",
        language="bash",
    )
    st.caption("Without a key the pipeline still completes — the fallback answers, and the "
               "auth failure is recorded in the run's quality report.")
    st.markdown("**Point it at BigQuery instead of DuckDB**")
    st.code(
        "cp .env.example .env      # WAREHOUSE=bigquery, BQ_PROJECT, credentials\n"
        "pip install dbt-bigquery google-cloud-bigquery pandas-gbq\n"
        "make backfill",
        language="bash",
    )
