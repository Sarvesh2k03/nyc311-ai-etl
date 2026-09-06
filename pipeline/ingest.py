"""The AI-mapping + load stage: raw files in, canonical rows in the warehouse.

For each source file in a batch partition:
  read headers and sample values -> resolve the column map (ai_schema_mapper)
  -> rename/drop/stamp lineage (transform) -> land in the warehouse (load).

Mapping happens per source system, not per row, so the LLM is called at most
once per source system per run - the cost is a function of how many upstream
systems exist, not of how much data flows through.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config
from pipeline.ai_schema_mapper import MappingResult, map_headers, write_decision_log
from pipeline.load import get_warehouse
from pipeline.logging_utils import get_logger, log_event, timed
from pipeline.transform import apply_mapping, read_source_file, sample_values

log = get_logger("pipeline.ingest")


def map_and_load_batch(
    batch_date: date | str,
    batch_id: str,
    use_llm: bool | None = None,
    warehouse_kind: str | None = None,
) -> dict:
    """Map, transform and load every source file landed for one batch date."""
    batch_date = str(batch_date)
    partition = config.RAW_DIR / f"dt={batch_date}"
    if not partition.exists():
        raise FileNotFoundError(f"No raw partition at {partition} - run extract first.")

    source_files = sorted(partition.glob("*.csv"))
    if not source_files:
        raise FileNotFoundError(f"No source files in {partition}.")

    warehouse = get_warehouse(warehouse_kind)
    results: list[MappingResult] = []
    frames: list[pd.DataFrame] = []
    per_source: list[dict] = []

    with timed(log, "map_and_load", batch_date=batch_date, batch_id=batch_id,
               warehouse=warehouse.name, sources=len(source_files)):
        for path in source_files:
            source_system = path.stem
            df = read_source_file(path)
            result = map_headers(
                source_system=source_system,
                columns=list(df.columns),
                samples=sample_values(df),
                batch_date=batch_date,
                use_llm=use_llm,
            )
            results.append(result)
            canonical = apply_mapping(df, result, batch_date, batch_id)
            frames.append(canonical)
            per_source.append({
                "source_system": source_system,
                "rows_read": len(df),
                "columns_seen": len(df.columns),
                **result.stats,
                "llm_used": result.llm_used,
                "llm_error": result.llm_error,
                "llm_latency_ms": result.llm_latency_ms,
                "llm_input_tokens": result.llm_input_tokens,
                "llm_output_tokens": result.llm_output_tokens,
                "llm_invalid_proposals": result.llm_invalid_proposals,
                "llm_low_confidence": result.llm_low_confidence,
                "human_overrides": result.human_overrides,
            })

        combined = pd.concat(frames, ignore_index=True)
        rows_loaded = warehouse.replace_batch(combined, batch_date)

        write_decision_log(results, config.REPORTS_DIR / "mapping_decisions.jsonl")

    # Roll the per-source numbers up into the batch-level mapping metrics.
    auto_denominator = sum(
        s["columns_total"] - s["by_method"]["override"] for s in per_source)
    auto_numerator = sum(
        s["columns_mapped"] - s["by_method"]["override"] for s in per_source)
    mapped_total = sum(s["columns_mapped"] for s in per_source)
    summary = {
        "batch_date": batch_date,
        "batch_id": batch_id,
        "warehouse": warehouse.name,
        "rows_read": int(sum(s["rows_read"] for s in per_source)),
        "rows_loaded": int(rows_loaded),
        "source_systems": len(per_source),
        "columns_total": sum(s["columns_total"] for s in per_source),
        "columns_auto_mapped": auto_numerator,
        "auto_match_rate": round(auto_numerator / auto_denominator, 4) if auto_denominator else 0.0,
        "by_method": {
            m: sum(s["by_method"][m] for s in per_source)
            for m in ("override", "exact", "llm", "fuzzy", "unmapped")
        },
        "fallback_rate": round(
            sum(s["by_method"]["fuzzy"] for s in per_source) / mapped_total, 4
        ) if mapped_total else 0.0,
        "llm_used": any(s["llm_used"] for s in per_source),
        "llm_errors": [s["llm_error"] for s in per_source if s["llm_error"]],
        "llm_latency_ms_total": sum(s["llm_latency_ms"] for s in per_source),
        "llm_input_tokens": sum(s["llm_input_tokens"] for s in per_source),
        "llm_output_tokens": sum(s["llm_output_tokens"] for s in per_source),
        "llm_invalid_proposals": sum(s["llm_invalid_proposals"] for s in per_source),
        "llm_low_confidence": sum(s["llm_low_confidence"] for s in per_source),
        "human_overrides": sum(s["human_overrides"] for s in per_source),
        "per_source": per_source,
    }

    out = config.REPORTS_DIR / f"mapping_summary_{batch_date}.json"
    out.write_text(json.dumps(summary, indent=2))
    log_event(log, "map_and_load.complete", **{
        k: v for k, v in summary.items() if k not in ("per_source", "by_method")
    })
    return summary
