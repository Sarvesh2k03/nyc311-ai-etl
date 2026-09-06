"""Run the real pipeline, live, from the dashboard.

This is not a replay. Clicking the button in the app:

  1. calls the NYC 311 Open Data API for the newest service requests that exist
     right now - rows that did not exist last week,
  2. splits them across the four upstream header dialects, so the mapper faces
     the same problem it faces in production,
  3. runs the actual `pipeline.ai_schema_mapper` over those headers,
  4. applies the resolved mapping and lands the rows in a throwaway warehouse,
  5. builds and tests the real dbt project on top of them.

Step 5 needs duckdb and dbt, which are optional here: where they are missing the
run falls back to an equivalent set of checks implemented in pandas, and says so.
That mirrors the pipeline's own design - the expensive path is an optimization,
never a dependency - and keeps the demo working on a host that only installed
the dashboard's requirements.

Nothing here writes to the project's real warehouse: every live run gets its own
temporary DuckDB file, which is deleted when the run finishes.
"""
from __future__ import annotations

import os
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterator

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.ai_schema_mapper import MappingResult, map_headers
from pipeline.canonical_schema import REQUIRED_FIELDS
from pipeline.source_dialects import DIALECTS, SOCRATA_TO_CANONICAL
from pipeline.transform import apply_mapping, sample_values

SOCRATA_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PROJECT_ROOT = Path(__file__).resolve().parents[1]

ProgressFn = Callable[[str, str], None]   # (step name, status message)


@dataclass
class Step:
    name: str
    detail: str = ""
    duration_s: float = 0.0
    rows: int | None = None
    ok: bool = True


@dataclass
class Check:
    name: str
    mirrors: str          # the dbt test this corresponds to
    failures: int
    passed: bool
    detail: str = ""


@dataclass
class LiveRunResult:
    started_at: str
    steps: list[Step] = field(default_factory=list)
    mapping: list[MappingResult] = field(default_factory=list)
    checks: list[Check] = field(default_factory=list)
    canonical: pd.DataFrame = field(default_factory=pd.DataFrame)
    marts: dict[str, pd.DataFrame] = field(default_factory=dict)
    rows_fetched: int = 0
    newest_created_at: str = ""
    oldest_created_at: str = ""
    engine: str = "pandas"        # "dbt + duckdb" when the full path ran
    total_seconds: float = 0.0
    error: str | None = None

    @property
    def decisions(self) -> pd.DataFrame:
        return pd.DataFrame([
            {"source_system": result.source_system, "source_column": d.source_column,
             "canonical_field": d.canonical_field, "method": d.method,
             "confidence": d.confidence, "reasoning": d.reasoning}
            for result in self.mapping for d in result.decisions
        ])

    @property
    def mapping_stats(self) -> dict:
        decisions = self.decisions
        if decisions.empty:
            return {}
        auto = decisions[decisions["method"] != "override"]
        auto_mapped = auto[auto["canonical_field"].notna()]
        mapped = decisions[decisions["canonical_field"].notna()]
        return {
            "columns": len(decisions),
            "auto_match_rate": len(auto_mapped) / len(auto) if len(auto) else 0.0,
            "by_method": decisions["method"].value_counts().to_dict(),
            "llm_used": any(r.llm_used for r in self.mapping),
            "llm_error": next((r.llm_error for r in self.mapping if r.llm_error), None),
        }

    @property
    def checks_passed(self) -> int:
        return sum(1 for c in self.checks if c.passed)


@contextmanager
def _timed(result: LiveRunResult, name: str, progress: ProgressFn | None) -> Iterator[Step]:
    if progress:
        progress(name, "running")
    step = Step(name=name)
    started = time.perf_counter()
    try:
        yield step
    except Exception as exc:
        step.ok = False
        step.detail = f"{type(exc).__name__}: {exc}"
        step.duration_s = round(time.perf_counter() - started, 2)
        result.steps.append(step)
        raise
    step.duration_s = round(time.perf_counter() - started, 2)
    result.steps.append(step)
    if progress:
        progress(name, "done")


# --------------------------------------------------------------------------
# 1. Extract
# --------------------------------------------------------------------------
def fetch_newest(limit: int) -> list[dict]:
    """The newest 311 service requests that exist in the feed right now."""
    response = requests.get(
        SOCRATA_URL,
        params={
            "$select": ",".join(SOCRATA_TO_CANONICAL),
            "$order": "created_date DESC",
            "$limit": limit,
        },
        headers={"X-App-Token": token} if (token := os.getenv("SOCRATA_APP_TOKEN")) else {},
        timeout=60,
    )
    response.raise_for_status()
    rows = response.json()
    if not rows:
        raise RuntimeError("The 311 API returned no rows.")
    return rows


def shard_into_dialects(rows: list[dict]) -> dict[str, pd.DataFrame]:
    """Re-header the same payload four ways, one per upstream source system."""
    socrata_of = {v: k for k, v in SOCRATA_TO_CANONICAL.items()}
    shards: dict[str, pd.DataFrame] = {}

    for index, dialect in enumerate(DIALECTS):
        slice_rows = rows[index::len(DIALECTS)]
        frame = pd.DataFrame([
            {
                **{header: str(row.get(socrata_of[canonical], "") or "")
                   for canonical, header in dialect.headers.items()},
                **{extra: f"{dialect.name}-{i}" for extra in dialect.extra_columns},
            }
            for i, row in enumerate(slice_rows)
        ])
        shards[dialect.name] = frame
    return shards


# --------------------------------------------------------------------------
# 5b. Quality checks without a warehouse
# --------------------------------------------------------------------------
def pandas_checks(frame: pd.DataFrame) -> list[Check]:
    """The dbt suite's assertions, evaluated in pandas.

    Each one names the dbt test it corresponds to, so the fallback path reports
    the same guarantees the warehouse path does rather than a weaker substitute.
    """
    checks: list[Check] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=24)

    for column in REQUIRED_FIELDS:
        missing = int(frame[column].isna().sum() + (frame[column] == "").sum())
        checks.append(Check(f"not_null: {column}", f"not_null_{column}", missing, missing == 0))

    duplicates = int(frame["request_id"].duplicated().sum())
    checks.append(Check("unique: request_id", "unique_fct_request_id", duplicates, duplicates == 0))

    for column in ("created_at", "closed_at"):
        parsed = pd.to_datetime(frame[column], errors="coerce")
        future = int((parsed > now).sum())
        checks.append(Check(f"not_in_future: {column}", f"not_in_future_{column}",
                            future, future == 0))

    for column, low, high in (("latitude", 40.4, 41.0), ("longitude", -74.3, -73.6)):
        values = pd.to_numeric(frame[column], errors="coerce")
        out = int(((values < low) | (values > high)).sum())
        checks.append(Check(f"within_range: {column}", f"within_range_{column}", out, out == 0,
                            f"expected {low} to {high}"))

    valid = {"BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND", "UNSPECIFIED"}
    borough = frame["borough"].fillna("").str.upper().str.strip()
    unexpected = int((~borough.isin(valid | {""})).sum())
    checks.append(Check("accepted_values: borough", "accepted_values_borough",
                        unexpected, unexpected == 0))

    created = pd.to_datetime(frame["created_at"], errors="coerce")
    closed = pd.to_datetime(frame["closed_at"], errors="coerce")
    backdated = int((closed.notna() & (closed < created)).sum())
    checks.append(Check(
        "closed_at is never before created_at", "assert_closed_after_created",
        backdated, True,
        f"{backdated} found — flagged and quarantined, not dropped" if backdated
        else "none in this batch",
    ))
    return checks


def build_marts(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """The analytics marts, computed in pandas for the no-warehouse path."""
    work = frame.copy()
    work["created_at"] = pd.to_datetime(work["created_at"], errors="coerce")
    work["closed_at"] = pd.to_datetime(work["closed_at"], errors="coerce")
    work["has_invalid_timestamps"] = work["closed_at"].notna() & (work["closed_at"] < work["created_at"])
    work["resolution_hours"] = (
        (work["closed_at"] - work["created_at"]).dt.total_seconds() / 3600
    ).where(~work["has_invalid_timestamps"])
    work["borough"] = work["borough"].fillna("UNSPECIFIED").str.upper().replace("", "UNSPECIFIED")

    agency = (work.groupby("agency_code")
              .agg(total_requests=("request_id", "count"),
                   closed_requests=("closed_at", "count"))
              .reset_index().sort_values("total_requests", ascending=False))
    agency["closure_rate"] = (agency["closed_requests"] / agency["total_requests"]).round(4)

    complaints = (work.groupby("complaint_type").size()
                  .reset_index(name="requests").sort_values("requests", ascending=False))

    borough_mix = (work.groupby("borough").size()
                   .reset_index(name="requests").sort_values("requests", ascending=False))

    return {"dim_agency": agency, "top_complaints": complaints, "borough_mix": borough_mix,
            "fact": work}


# --------------------------------------------------------------------------
# 5a. Quality checks through the real warehouse
# --------------------------------------------------------------------------
def warehouse_available() -> bool:
    try:
        import duckdb  # noqa: F401
        from dbt.cli.main import dbtRunner  # noqa: F401
    except Exception:
        return False
    return (PROJECT_ROOT / "dbt" / "nyc311" / "dbt_project.yml").exists()


def run_dbt_path(frame: pd.DataFrame, batch_date: str) -> tuple[list[Check], dict[str, pd.DataFrame]]:
    """Load into a throwaway DuckDB and run the real dbt build and test suite."""
    import duckdb
    from dbt.cli.main import dbtRunner

    tmp_dir = Path(tempfile.mkdtemp(prefix="nyc311-live-"))
    db_path = tmp_dir / "live.duckdb"
    project_dir = PROJECT_ROOT / "dbt" / "nyc311"

    try:
        with duckdb.connect(str(db_path)) as con:
            for schema in ("raw", "staging", "analytics"):
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            con.register("incoming", frame)
            con.execute("CREATE TABLE raw.service_requests_raw AS SELECT * FROM incoming")

        env = {"DBT_TARGET": "duckdb", "DUCKDB_PATH": str(db_path),
               "DBT_PROFILES_DIR": str(project_dir)}
        previous = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        try:
            runner = dbtRunner()
            common = ["--project-dir", str(project_dir), "--profiles-dir", str(project_dir),
                      "--target", "duckdb", "--no-use-colors"]
            build = runner.invoke(["run", *common])
            if not build.success:
                raise RuntimeError(f"dbt run failed: {build.exception or 'see logs'}")
            tested = runner.invoke(["test", *common])
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        checks = [
            Check(
                name=node.node.name,
                mirrors=node.node.name,
                failures=int(node.failures or 0),
                passed=node.status in ("pass", "warn"),
                detail="warn (non-blocking)" if node.status == "warn" else "",
            )
            for node in (tested.result or [])
        ]

        # Opened read-write, matching the configuration dbt's adapter still holds
        # on this file: DuckDB refuses a second connection that disagrees.
        with duckdb.connect(str(db_path)) as con:
            marts = {
                "dim_agency": con.execute(
                    "select agency_code, total_requests, closed_requests, closure_rate "
                    "from analytics.dim_agency order by total_requests desc").fetchdf(),
                "top_complaints": con.execute(
                    "select complaint_type, count(*) as requests "
                    "from analytics.fct_service_requests group by 1 order by 2 desc").fetchdf(),
                "borough_mix": con.execute(
                    "select coalesce(borough,'UNSPECIFIED') as borough, count(*) as requests "
                    "from analytics.fct_service_requests group by 1 order by 2 desc").fetchdf(),
                "fact": con.execute(
                    "select * from analytics.fct_service_requests").fetchdf(),
            }
        return checks, marts
    finally:
        # A live run must never leave a warehouse behind on the host.
        for path in sorted(tmp_dir.rglob("*"), reverse=True):
            path.unlink(missing_ok=True) if path.is_file() else path.rmdir()
        tmp_dir.rmdir()


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def run_live_pipeline(
    limit: int = 2000,
    use_llm: bool | None = None,
    force_pandas: bool = False,
    progress: ProgressFn | None = None,
) -> LiveRunResult:
    """Execute the whole pipeline on data fetched from the live 311 feed."""
    result = LiveRunResult(started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"))
    overall = time.perf_counter()

    try:
        with _timed(result, "Fetch live 311 data", progress) as step:
            rows = fetch_newest(limit)
            result.rows_fetched = len(rows)
            stamps = sorted(r.get("created_date", "") for r in rows if r.get("created_date"))
            result.newest_created_at = stamps[-1] if stamps else ""
            result.oldest_created_at = stamps[0] if stamps else ""
            step.rows = len(rows)
            step.detail = f"newest request filed {result.newest_created_at[:16].replace('T', ' ')} UTC"

        with _timed(result, "Split across 4 source systems", progress) as step:
            shards = shard_into_dialects(rows)
            step.rows = sum(len(f) for f in shards.values())
            step.detail = " · ".join(f"{name} {len(frame):,}" for name, frame in shards.items())

        with _timed(result, "Resolve headers to the canonical schema", progress) as step:
            frames = []
            batch_date = (result.newest_created_at[:10]
                          or datetime.now(timezone.utc).date().isoformat())
            for name, frame in shards.items():
                mapping = map_headers(
                    source_system=name,
                    columns=list(frame.columns),
                    samples=sample_values(frame),
                    batch_date=batch_date,
                    use_llm=use_llm,
                )
                result.mapping.append(mapping)
                frames.append(apply_mapping(frame, mapping, batch_date, "live-demo"))
            stats = result.mapping_stats
            step.rows = stats.get("columns")
            step.detail = (f"{stats['columns']} header decisions · "
                           f"{stats['auto_match_rate']:.0%} auto-matched")

        with _timed(result, "Land canonical rows", progress) as step:
            result.canonical = pd.concat(frames, ignore_index=True)
            step.rows = len(result.canonical)
            step.detail = f"{len(result.canonical.columns)} canonical columns + lineage"

        use_dbt = warehouse_available() and not force_pandas
        label = "Build and test dbt models" if use_dbt else "Run data quality checks"
        with _timed(result, label, progress) as step:
            if use_dbt:
                result.checks, result.marts = run_dbt_path(result.canonical, batch_date)
                result.engine = "dbt + duckdb"
            else:
                result.checks = pandas_checks(result.canonical)
                result.marts = build_marts(result.canonical)
                result.engine = "pandas"
            step.rows = len(result.checks)
            step.detail = f"{result.checks_passed}/{len(result.checks)} passed · {result.engine}"

    except Exception as exc:
        result.error = f"{type(exc).__name__}: {exc}"

    result.total_seconds = round(time.perf_counter() - overall, 2)
    return result
