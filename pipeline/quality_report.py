"""Data quality report: one artifact per run, from real run output only.

Three sources, all produced by the run itself:
  - dbt's run_results.json  -> per-test pass/fail/warn and model timings
  - the mapping summary     -> AI/fallback decisions and confidence
  - the warehouse itself    -> row counts at every stage of the DAG

Nothing here is estimated; if a number is unavailable it is reported as such.
"""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pipeline import config
from pipeline.load import get_warehouse
from pipeline.logging_utils import get_logger, log_event

log = get_logger("pipeline.quality_report")

STAGE_RELATIONS = {
    "raw.service_requests_raw": "1_landed",
    "staging.stg_service_requests": "2_staged",
    "staging.int_service_requests_deduped": "3_deduped",
    "analytics.fct_service_requests": "4_fact",
    "analytics.agg_daily_borough_sla": "5_daily_mart",
    "analytics.dim_agency": "5_agency_dim",
}


def test_name(unique_id: str) -> str:
    """Readable name from a dbt test unique_id.

    Generic tests look like `test.nyc311.not_null_model_col.9ab3f2`; singular
    tests like `test.nyc311.assert_closed_after_created`. Index 2 is the name in
    both shapes - taking the last segment would return the hash.
    """
    parts = unique_id.split(".")
    return parts[2] if len(parts) > 2 else unique_id


def parse_dbt_results(run_results_path: Path) -> dict:
    """Summarize a dbt run_results.json (works for both `run` and `test`)."""
    if not run_results_path.exists():
        return {"available": False, "reason": f"{run_results_path} not found"}

    payload = json.loads(run_results_path.read_text())

    # run_results.json is rewritten by every dbt invocation, including `docs
    # generate`, which records every node as "success" and would otherwise be
    # read as "44 tests, 0 passed". Only a test/build run can be reported on.
    which = payload.get("args", {}).get("which", "unknown")
    if which not in ("test", "build"):
        return {
            "available": False,
            "reason": f"run_results.json is from `dbt {which}`, not `dbt test` - "
                      "re-run `dbt test` before building the report",
        }

    results = payload.get("results", [])
    statuses = Counter(r.get("status") for r in results)
    tests = [r for r in results if r.get("unique_id", "").startswith("test.")]
    test_statuses = Counter(r.get("status") for r in tests)
    passed = test_statuses.get("pass", 0)
    total_tests = sum(test_statuses.values())

    return {
        "available": True,
        "elapsed_s": round(payload.get("elapsed_time", 0), 3),
        "nodes_total": len(results),
        "status_counts": dict(statuses),
        "tests_total": total_tests,
        "tests_passed": passed,
        "tests_failed": test_statuses.get("fail", 0),
        "tests_warned": test_statuses.get("warn", 0),
        "tests_errored": test_statuses.get("error", 0),
        "test_pass_rate": round(passed / total_tests, 4) if total_tests else None,
        "failing_tests": [
            {"test": test_name(r["unique_id"]), "status": r["status"],
             "failures": r.get("failures"), "message": r.get("message")}
            for r in tests if r.get("status") in ("fail", "error", "warn")
        ],
    }


def confidence_distribution(decisions_path: Path, batch_date: str | None = None) -> dict:
    """Bucket AI/fallback mapping confidences from the decision audit log."""
    if not decisions_path.exists():
        return {"available": False}

    buckets = {"1.00 (exact/override)": 0, "0.90-0.99": 0, "0.82-0.89": 0, "below threshold (unmapped)": 0}
    by_method: Counter = Counter()
    rows = 0
    for line in decisions_path.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if batch_date and row.get("batch_date") != batch_date:
            continue
        rows += 1
        by_method[row["method"]] += 1
        conf = float(row.get("confidence") or 0)
        if not row.get("canonical_field"):
            buckets["below threshold (unmapped)"] += 1
        elif conf >= 1.0:
            buckets["1.00 (exact/override)"] += 1
        elif conf >= 0.90:
            buckets["0.90-0.99"] += 1
        else:
            buckets["0.82-0.89"] += 1

    return {"available": True, "decisions": rows,
            "by_method": dict(by_method), "confidence_buckets": buckets}


def build_report(batch_date: str, batch_id: str = "", pipeline_runtime_s: float | None = None) -> dict:
    """Assemble the run's quality report and write it as JSON + Markdown."""
    warehouse = get_warehouse()
    dbt_target = config.DBT_PROJECT_DIR / "target"

    mapping_path = config.REPORTS_DIR / f"mapping_summary_{batch_date}.json"
    mapping = json.loads(mapping_path.read_text()) if mapping_path.exists() else {}

    report = {
        "batch_date": batch_date,
        "batch_id": batch_id,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "warehouse": warehouse.name,
        "pipeline_runtime_s": pipeline_runtime_s,
        "row_counts": {
            label: warehouse.row_count(relation)
            for relation, label in sorted(STAGE_RELATIONS.items(), key=lambda kv: kv[1])
        },
        "dbt_tests": parse_dbt_results(dbt_target / "run_results.json"),
        "ai_mapping": {
            k: v for k, v in mapping.items() if k != "per_source"
        },
        "mapping_confidence": confidence_distribution(
            config.REPORTS_DIR / "mapping_decisions.jsonl", batch_date
        ),
    }

    json_path = config.REPORTS_DIR / f"quality_report_{batch_date}.json"
    json_path.write_text(json.dumps(report, indent=2))
    md_path = config.REPORTS_DIR / f"quality_report_{batch_date}.md"
    md_path.write_text(render_markdown(report))

    log_event(log, "quality_report.written", batch_date=batch_date,
              json=str(json_path), markdown=str(md_path))
    return report


def render_markdown(report: dict) -> str:
    dbt = report["dbt_tests"]
    ai = report["ai_mapping"]
    conf = report["mapping_confidence"]

    lines = [
        f"# Data quality report - batch {report['batch_date']}",
        "",
        f"- Generated: `{report['generated_at']}`",
        f"- Warehouse: `{report['warehouse']}`",
        f"- Batch id: `{report['batch_id'] or 'n/a'}`",
        f"- Pipeline runtime: "
        f"`{report['pipeline_runtime_s']}s`" if report.get("pipeline_runtime_s") else "- Pipeline runtime: `n/a`",
        "",
        "## Row counts by stage",
        "",
        "| Stage | Rows |",
        "| --- | ---: |",
    ]
    for stage, count in report["row_counts"].items():
        lines.append(f"| {stage} | {count:,} |" if count >= 0 else f"| {stage} | not available |")

    lines += ["", "## dbt tests", ""]
    if dbt.get("available"):
        lines += [
            f"- Tests run: **{dbt['tests_total']}**",
            f"- Passed: **{dbt['tests_passed']}** "
            f"({dbt['test_pass_rate']:.1%})" if dbt.get("test_pass_rate") is not None else "",
            f"- Failed: {dbt['tests_failed']} | Warned: {dbt['tests_warned']} | Errored: {dbt['tests_errored']}",
            f"- dbt elapsed: {dbt['elapsed_s']}s",
        ]
        if dbt["failing_tests"]:
            lines += ["", "| Test | Status | Failing rows |", "| --- | --- | ---: |"]
            lines += [f"| {t['test']} | {t['status']} | {t['failures']} |" for t in dbt["failing_tests"]]
    else:
        lines.append(f"- Not available: {dbt.get('reason')}")

    lines += ["", "## AI-assisted schema mapping", ""]
    if ai:
        by_method = ai.get("by_method", {})
        lines += [
            f"- Source systems mapped: **{ai.get('source_systems')}**",
            f"- Column decisions: **{ai.get('columns_total')}**",
            f"- Auto-match rate (excludes human overrides): **{ai.get('auto_match_rate', 0):.1%}**",
            f"- Fallback rate (share of mapped columns resolved by fuzzy, not the LLM): "
            f"**{ai.get('fallback_rate', 0):.1%}**",
            f"- LLM used: **{ai.get('llm_used')}**"
            + (f" (model `{config.AI_MAPPER_MODEL}`)" if ai.get("llm_used") else " - fallback path only"),
            f"- By tier: " + ", ".join(f"`{k}`={v}" for k, v in by_method.items()),
        ]
        if ai.get("llm_errors"):
            lines.append(f"- LLM errors (fell back): `{ai['llm_errors'][0][:160]}`")
        if ai.get("human_overrides"):
            lines.append(f"- Human overrides applied: {ai['human_overrides']}")
        if ai.get("llm_used"):
            lines += [
                f"- LLM latency: {ai.get('llm_latency_ms_total')} ms total",
                f"- LLM tokens: {ai.get('llm_input_tokens')} in / {ai.get('llm_output_tokens')} out",
                f"- Rejected proposals: {ai.get('llm_invalid_proposals')} invalid field, "
                f"{ai.get('llm_low_confidence')} below confidence floor",
            ]

    if conf.get("available"):
        lines += ["", "### Mapping confidence distribution", "",
                  "| Bucket | Columns |", "| --- | ---: |"]
        lines += [f"| {k} | {v} |" for k, v in conf["confidence_buckets"].items()]

    return "\n".join(l for l in lines if l != "") + "\n"
