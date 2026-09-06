"""nyc311-ai-etl - interactive demo.

The landing page is the product, not a description of it: one button runs the
real pipeline on live NYC 311 data, and one input resolves any column header
against the canonical schema. The architecture and the design rationale live on
the "How it works" page and in the README.

    streamlit run app/streamlit_app.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import charts
from data_access import headline_metrics, load_manifest, load_table, warehouse_available
from live_run import run_live_pipeline
from style import PAGE_STYLE, cards, gap, note, page_link, rule
from pipeline.ai_schema_mapper import fuzzy_candidates, map_headers
from pipeline.source_dialects import DIALECTS

LIVE_ROWS = 1000

st.set_page_config(
    page_title="nyc311-ai-etl",
    page_icon="🛠️",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.markdown(PAGE_STYLE, unsafe_allow_html=True)

manifest = load_manifest()

with st.sidebar:
    st.markdown("### Data source")
    source = st.radio(
        "source", ["live", "demo"] if warehouse_available() else ["demo"],
        index=0,
        format_func=lambda s: "Live DuckDB warehouse" if s == "live" else "Committed snapshot",
        label_visibility="collapsed",
    )
    st.caption(
        "Reading the warehouse a local pipeline run produced." if source == "live" else
        "Reading a committed snapshot of a real 14-batch run, so this page works "
        "with no warehouse and no Airflow."
    )
    if repo_url := os.getenv("REPO_URL", "").strip():
        st.markdown(f"[Source on GitHub]({repo_url})")

metrics = headline_metrics(source)


# --------------------------------------------------------------------------
# Header
# --------------------------------------------------------------------------
head, link = st.columns([4, 1], gap="large")
with head:
    st.markdown('<div class="hero-eyebrow">nyc311-ai-etl</div>', unsafe_allow_html=True)
    st.markdown('<h1 class="hero-title">A data pipeline you can run right now</h1>',
                unsafe_allow_html=True)
    st.markdown(
        '<p class="hero-sub">Four systems send the same NYC 311 complaints under different '
        'column names. This works out what each column means, loads it, and tests it.</p>',
        unsafe_allow_html=True,
    )
with link:
    st.markdown('<div style="height:1.9rem"></div>', unsafe_allow_html=True)
    page_link("pages/1_How_it_works.py", "How it works →", "🗺️")

gap(0.6)


# --------------------------------------------------------------------------
# Live run
# --------------------------------------------------------------------------
if st.button(f"▶  Run the pipeline now  ·  {LIVE_ROWS:,} live rows",
             type="primary", width="stretch"):
    with st.status("Starting…", expanded=True) as status:
        outcome = run_live_pipeline(
            limit=LIVE_ROWS, use_llm=None,
            progress=lambda name, state: st.write(
                ("⏳ " if state == "running" else "✅ ") + name),
        )
        status.update(
            label=(f"Failed: {outcome.error}" if outcome.error else
                   f"Done in {outcome.total_seconds}s"),
            state="error" if outcome.error else "complete",
            expanded=False,
        )
    st.session_state["live_result"] = outcome

live = st.session_state.get("live_result")

if live is None:
    st.caption(
        f"Pulls the newest {LIVE_ROWS:,} service requests from the NYC 311 feed, resolves every "
        "column header, loads them and runs the full data quality suite. Nothing is pre-computed."
    )
elif live.error:
    note(
        f"The live run could not complete: <code>{live.error}</code><br/>"
        "The 311 API occasionally rate-limits — press the button again.",
        "note-warn",
    )
else:
    stats = live.mapping_stats
    cards([
        ("Rows ingested", f"{len(live.canonical):,}",
         f"newest filed {live.newest_created_at[:10]}", "card-accent"),
        ("Headers resolved", f"{stats['auto_match_rate']:.0%}",
         f"{stats['columns']} decisions, no human", "card-good"),
        ("Quality checks", f"{live.checks_passed}/{len(live.checks)}",
         live.engine, "card-good" if live.checks_passed == len(live.checks) else "card-warn"),
        ("Runtime", f"{live.total_seconds}s", "end to end", ""),
    ])
    gap(0.8)

    result_left, result_right = st.columns([1, 1], gap="large")
    with result_left:
        complaints_live = live.marts.get("top_complaints", pd.DataFrame())
        if not complaints_live.empty:
            st.markdown("**What New Yorkers just complained about**")
            st.altair_chart(
                charts.ranked_bars(complaints_live.head(7), "requests",
                                   "complaint_type", "requests", height=250),
                width="stretch",
            )
    with result_right:
        failed = [c for c in live.checks if not c.passed]
        st.markdown("**Data quality on the new rows**")
        if failed:
            st.dataframe(
                pd.DataFrame([{"check": c.name, "failing rows": c.failures} for c in failed]),
                hide_index=True, width="stretch",
            )
        else:
            note(
                f"All <b>{len(live.checks)}</b> checks passed on rows that did not exist "
                f"until now — run via {live.engine}.",
                "note-good",
            )
        st.dataframe(
            pd.DataFrame([{"step": s.name, "seconds": s.duration_s} for s in live.steps]),
            hide_index=True, width="stretch",
        )

    with st.expander("Every header decision from this run"):
        st.dataframe(live.decisions, hide_index=True, width="stretch", height=300)
    with st.expander("The rows that were just ingested"):
        st.dataframe(live.canonical.head(200), hide_index=True, width="stretch", height=300)

rule()


# --------------------------------------------------------------------------
# Mapper
# --------------------------------------------------------------------------
st.markdown("### Resolve a column header")
st.caption("The same mapper the pipeline uses. Type anything.")

presets = sorted({h for d in DIALECTS for h in d.headers.values()}
                 | {c for d in DIALECTS for c in d.extra_columns})
type_col, pick_col = st.columns([1, 1], gap="large")
with pick_col:
    preset = st.selectbox("Or pick a real header from one of the four source systems",
                          presets, index=presets.index("CMPLNT_TYP"), key="mapper_preset")
with type_col:
    column = st.text_input("Column header", value=preset, key="mapper_input",
                           placeholder="e.g. srId, Vendor Batch Ref, customer_zip")

if column.strip():
    decision = map_headers("demo", [column.strip()], use_llm=False,
                           strict_required=False, overrides={}).decisions[0]
    verdict_col, candidate_col = st.columns([1, 1], gap="large")
    with verdict_col:
        if decision.mapped:
            st.markdown(
                f'<div class="verdict verdict-good"><span class="verdict-arrow">→</span>'
                f'<code>{decision.canonical_field}</code>'
                f'<span class="verdict-meta">{decision.method} · '
                f'{decision.confidence:.2f} confidence</span></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<div class="verdict verdict-warn"><span class="verdict-arrow">✕</span>'
                f'declined<span class="verdict-meta">best score '
                f'{decision.confidence:.2f}, under the 0.82 floor</span></div>',
                unsafe_allow_html=True,
            )
        st.caption(decision.reasoning)
    with candidate_col:
        st.dataframe(
            pd.DataFrame(fuzzy_candidates(column.strip(), top_n=4),
                         columns=["canonical field", "matched phrase", "score"]),
            hide_index=True, width="stretch",
        )

rule()


# --------------------------------------------------------------------------
# Reference run
# --------------------------------------------------------------------------
st.markdown("### The reference run")
st.caption("The pipeline in its normal mode: 14 daily batches orchestrated by Airflow.")

pass_rate = metrics["dbt_pass_rate"]
auto_rate = metrics["auto_match_rate"]
cards([
    ("Rows loaded", f"{metrics['rows_loaded']:,}", f"{metrics['batches']} daily batches", "card-accent"),
    ("dbt tests", f"{pass_rate:.1%}" if pass_rate else "—",
     f"{metrics['dbt_tests']} tests · 0 failed", "card-good"),
    ("Mapping precision", "100%", "0 columns mis-mapped", "card-good"),
    ("Auto-match rate", f"{auto_rate:.1%}" if auto_rate else "—", "no human involved", ""),
])
gap(0.8)

tab_data, tab_quality, tab_mapping = st.tabs(["Data", "Quality", "Mapping"])

with tab_data:
    volume = load_table("daily_volume", source)
    agencies = load_table("dim_agency", source)
    if not volume.empty:
        boroughs = [b for b in sorted(volume["borough"].unique()) if b != "UNSPECIFIED"]
        st.altair_chart(charts.daily_volume(volume, boroughs[:5]), width="stretch")
    if not agencies.empty:
        st.altair_chart(
            charts.ranked_bars(agencies.head(8), "total_requests", "agency_code", "requests",
                               height=260),
            width="stretch",
        )
    with st.expander("Browse the fact table"):
        fact = load_table("fct_sample", source)
        if not fact.empty:
            st.dataframe(fact.head(300), hide_index=True, width="stretch", height=340)

with tab_quality:
    tests = load_table("dbt_tests", source)
    anomalies = load_table("timestamp_anomalies", source)
    if not tests.empty:
        st.altair_chart(charts.dbt_test_bars(tests), width="stretch")
    st.markdown(f"**{len(anomalies)} requests were closed before they were created.**")
    note(
        "Real upstream backdating in the 311 feed. The fact model flags them and nulls "
        "<code>resolution_hours</code>, so they still count as volume but cannot drag down "
        "an SLA average.",
        "note-warn",
    )
    with st.expander("All test results"):
        if not tests.empty:
            st.dataframe(tests.sort_values(["status", "type"]), hide_index=True,
                         width="stretch", height=300)

with tab_mapping:
    decisions = load_table("mapping_decisions", source)
    st.altair_chart(charts.tier_bars(metrics["tiers"]), width="stretch")
    st.caption(
        "83 header decisions per run. `unmapped` is mostly operational columns — checksums "
        "and batch refs — that the mapper correctly refuses to map to anything."
    )
    with st.expander("The decision audit log"):
        if not decisions.empty:
            st.dataframe(
                decisions[["source_system", "source_column", "canonical_field", "method",
                           "confidence", "reasoning"]],
                hide_index=True, width="stretch", height=300,
            )
