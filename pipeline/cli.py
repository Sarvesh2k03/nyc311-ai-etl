"""Command-line entry points.

Every Airflow task is a thin wrapper over one of these subcommands, which means
the whole pipeline can be run, debugged and unit-tested on a laptop without
Airflow, and the DAG contains orchestration only - no business logic.

    python -m pipeline.cli extract      --batch-date 2024-01-15
    python -m pipeline.cli map-and-load --batch-date 2024-01-15 --batch-id manual
    python -m pipeline.cli report       --batch-date 2024-01-15
    python -m pipeline.cli run          --batch-date 2024-01-15   # all of the above + dbt
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

from pipeline import config
from pipeline.extract import extract_batch
from pipeline.ingest import map_and_load_batch
from pipeline.logging_utils import get_logger, log_event
from pipeline.quality_report import build_report

log = get_logger("pipeline.cli")


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def dbt_env() -> dict:
    """Environment for dbt: keeps its target aligned with WAREHOUSE."""
    env = os.environ.copy()
    env["DBT_TARGET"] = "bigquery" if config.WAREHOUSE == "bigquery" else "duckdb"
    env["DUCKDB_PATH"] = str(config.DUCKDB_PATH)
    env.setdefault("DBT_PROFILES_DIR", str(config.DBT_PROJECT_DIR))
    return env


def dbt_executable() -> str:
    """Locate dbt next to the running interpreter before falling back to PATH.

    Without this, invoking the CLI as `.venv/bin/python -m pipeline.cli` finds no
    `dbt` on PATH even though the venv has one installed.
    """
    candidate = Path(sys.executable).parent / "dbt"
    return str(candidate) if candidate.exists() else "dbt"


def run_dbt(command: str, extra_args: list[str] | None = None) -> int:
    """Invoke dbt in-project and stream its output. Returns its exit code."""
    args = [dbt_executable(), command, "--profiles-dir", str(config.DBT_PROJECT_DIR)] + (extra_args or [])
    log_event(log, "dbt.start", command=command, args=args[2:])
    started = time.perf_counter()
    proc = subprocess.run(args, cwd=str(config.DBT_PROJECT_DIR), env=dbt_env())
    log_event(log, "dbt.done", command=command, returncode=proc.returncode,
              duration_s=round(time.perf_counter() - started, 3))
    return proc.returncode


def cmd_extract(args) -> int:
    manifest = extract_batch(args.batch_date, args.row_limit)
    print(json.dumps({"total_rows": manifest["total_rows"], "files": len(manifest["files"])}))
    return 0


def cmd_map_and_load(args) -> int:
    summary = map_and_load_batch(
        batch_date=args.batch_date,
        batch_id=args.batch_id,
        use_llm=None if args.use_llm is None else args.use_llm,
    )
    print(json.dumps({k: v for k, v in summary.items() if k != "per_source"}, indent=2))
    return 0


def cmd_dbt(args) -> int:
    return run_dbt(args.dbt_command, args.dbt_args)


def cmd_report(args) -> int:
    report = build_report(str(args.batch_date), args.batch_id or "", args.runtime)
    md = config.REPORTS_DIR / f"quality_report_{args.batch_date}.md"
    print(md.read_text())
    dbt = report["dbt_tests"]
    # A failed dbt test must fail the task; a warning must not.
    return 1 if dbt.get("available") and (dbt["tests_failed"] or dbt["tests_errored"]) else 0


def cmd_run(args) -> int:
    """Full local pipeline: extract -> map/load -> dbt run -> dbt test -> report."""
    started = time.perf_counter()
    batch_id = args.batch_id or f"local-{args.batch_date}-{int(started)}"

    extract_batch(args.batch_date, args.row_limit)
    map_and_load_batch(args.batch_date, batch_id,
                       use_llm=None if args.use_llm is None else args.use_llm)
    if (rc := run_dbt("run")) != 0:
        log_event(log, "pipeline.failed", stage="dbt_run", returncode=rc)
        return rc
    # dbt test's exit code is intentionally not fatal here: the report step
    # decides, so that a warn-severity test does not kill the run but a genuine
    # failure still does.
    run_dbt("test")
    return cmd_report(argparse.Namespace(
        batch_date=args.batch_date, batch_id=batch_id,
        runtime=round(time.perf_counter() - started, 2),
    ))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    def add_batch_date(p):
        p.add_argument("--batch-date", type=_parse_date, required=True,
                       help="Logical partition date, YYYY-MM-DD.")

    p = sub.add_parser("extract", help="Pull one day of 311 data into raw source files.")
    add_batch_date(p)
    p.add_argument("--row-limit", type=int, default=None)
    p.set_defaults(func=cmd_extract)

    p = sub.add_parser("map-and-load", help="AI-map source headers and load the batch.")
    add_batch_date(p)
    p.add_argument("--batch-id", default="")
    p.add_argument("--use-llm", dest="use_llm", action="store_true", default=None)
    p.add_argument("--no-llm", dest="use_llm", action="store_false")
    p.set_defaults(func=cmd_map_and_load)

    p = sub.add_parser("dbt", help="Run a dbt command against the configured target.")
    p.add_argument("dbt_command")
    p.add_argument("dbt_args", nargs="*", default=[])
    p.set_defaults(func=cmd_dbt)

    p = sub.add_parser("report", help="Build the data quality report for a batch.")
    add_batch_date(p)
    p.add_argument("--batch-id", default="")
    p.add_argument("--runtime", type=float, default=None)
    p.set_defaults(func=cmd_report)

    p = sub.add_parser("run", help="Run the whole pipeline for one batch date.")
    add_batch_date(p)
    p.add_argument("--batch-id", default="")
    p.add_argument("--row-limit", type=int, default=None)
    p.add_argument("--use-llm", dest="use_llm", action="store_true", default=None)
    p.add_argument("--no-llm", dest="use_llm", action="store_false")
    p.set_defaults(func=cmd_run)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
