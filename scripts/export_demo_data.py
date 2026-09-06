"""Export a self-contained demo snapshot for the dashboard.

The dashboard reads the live warehouse when one is present. On a hosting
platform there is no warehouse, no Airflow and no API access, so this script
freezes the results of a real pipeline run into small Parquet/JSON files that
ship with the repository. Every number in the demo snapshot came out of an
actual run - this exports results, it does not generate them.

    .venv/bin/python scripts/export_demo_data.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import pandas as pd

from pipeline import config

DEMO_DIR = config.DATA_DIR / "demo"
FACT_SAMPLE_ROWS = 8000


def export() -> dict:
    if not config.DUCKDB_PATH.exists():
        raise SystemExit(
            f"No warehouse at {config.DUCKDB_PATH}. Run `make backfill` before exporting."
        )

    DEMO_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(config.DUCKDB_PATH), read_only=True)
    written: dict[str, int] = {}

    def dump(name: str, frame: pd.DataFrame) -> None:
        frame.to_parquet(DEMO_DIR / f"{name}.parquet", index=False)
        written[name] = len(frame)

    # Analytics marts, small enough to ship whole.
    dump("agg_daily_borough_sla", con.execute(
        "select * from analytics.agg_daily_borough_sla order by created_date, borough").fetchdf())
    dump("dim_agency", con.execute(
        "select * from analytics.dim_agency order by total_requests desc").fetchdf())

    # Daily totals by borough, pre-aggregated for the volume chart.
    dump("daily_volume", con.execute("""
        select created_date, borough,
               sum(requests_opened) as requests_opened,
               sum(requests_closed) as requests_closed
        from analytics.agg_daily_borough_sla
        group by created_date, borough
        order by created_date, borough
    """).fetchdf())

    # Complaint mix, for the categorical breakdown.
    dump("top_complaints", con.execute("""
        select complaint_type,
               count(*) as requests,
               round(avg(resolution_hours), 2) as avg_resolution_hours
        from analytics.fct_service_requests
        group by complaint_type
        order by requests desc
        limit 15
    """).fetchdf())

    # Stage row counts, so the dashboard can show the funnel without a warehouse.
    stages = {
        "1. landed (raw)": "raw.service_requests_raw",
        "2. staged (typed)": "staging.stg_service_requests",
        "3. deduped": "staging.int_service_requests_deduped",
        "4. fact": "analytics.fct_service_requests",
    }
    dump("row_counts", pd.DataFrame([
        {"stage": label, "relation": rel,
         "rows": con.execute(f"select count(*) from {rel}").fetchone()[0]}
        for label, rel in stages.items()
    ]))

    # A browsable slice of the fact table, newest first.
    dump("fct_sample", con.execute(f"""
        select request_id, created_at, closed_at, agency_code, complaint_type,
               descriptor, borough, status, resolution_hours, has_invalid_timestamps,
               source_system, batch_date
        from analytics.fct_service_requests
        order by created_at desc
        limit {FACT_SAMPLE_ROWS}
    """).fetchdf())

    # Every quarantined timestamp anomaly - the data-quality find, in full.
    dump("timestamp_anomalies", con.execute("""
        select request_id, agency_code, complaint_type, created_at, closed_at, borough
        from analytics.fct_service_requests
        where has_invalid_timestamps
        order by created_at
    """).fetchdf())

    # The AI mapping decision log for a single representative batch.
    decisions_path = config.REPORTS_DIR / "mapping_decisions.jsonl"
    decisions = [json.loads(line) for line in
                 decisions_path.read_text().splitlines() if line.strip()]
    frame = pd.DataFrame(decisions)
    latest_batch = sorted(frame["batch_date"].unique())[-1]
    dump("mapping_decisions", frame[frame["batch_date"] == latest_batch].reset_index(drop=True))
    dump("mapping_decisions_all", frame)

    # dbt results and the pipeline's own quality report.
    run_results = json.loads((config.DBT_PROJECT_DIR / "target" / "run_results.json").read_text())
    if run_results.get("args", {}).get("which") not in ("test", "build"):
        raise SystemExit("run_results.json is not from `dbt test` - run `make dbt-test` first.")

    manifest = json.loads((config.DBT_PROJECT_DIR / "target" / "manifest.json").read_text())
    test_nodes = {k: v for k, v in manifest["nodes"].items() if v["resource_type"] == "test"}
    dump("dbt_tests", pd.DataFrame([
        {
            "test": r["unique_id"].split(".")[2],
            "type": ((test_nodes.get(r["unique_id"], {}).get("test_metadata") or {})
                     .get("name", "singular")),
            "model": (test_nodes.get(r["unique_id"], {}).get("attached_node") or "").split(".")[-1],
            "status": r["status"],
            "failures": r.get("failures", 0),
            "runtime_s": round(r.get("execution_time", 0), 4),
        }
        for r in run_results["results"] if r["unique_id"].startswith("test.")
    ]))

    reports = sorted(config.REPORTS_DIR.glob("quality_report_*.json"))
    quality = json.loads(reports[-1].read_text()) if reports else {}

    manifest_out = {
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "live DuckDB warehouse produced by a real 14-batch pipeline run",
        "batch_dates": sorted(
            con.execute("select distinct _batch_date from raw.service_requests_raw")
               .fetchdf()["_batch_date"].tolist()
        ),
        "tables": written,
        "quality_report": quality,
    }
    (DEMO_DIR / "manifest.json").write_text(json.dumps(manifest_out, indent=2, default=str))
    return manifest_out


if __name__ == "__main__":
    result = export()
    print(json.dumps({"tables": result["tables"], "batches": len(result["batch_dates"])}, indent=2))
