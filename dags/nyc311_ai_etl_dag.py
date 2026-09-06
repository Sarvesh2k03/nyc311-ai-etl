"""Airflow DAG: AI-augmented ELT for NYC 311 service requests.

    extract -> ai_schema_map_and_load -> dbt_run -> dbt_test -> quality_report

Every task is a thin wrapper over `python -m pipeline.cli <subcommand>`, so the
DAG holds orchestration only - scheduling, retries, alerting and dependencies -
and the pipeline logic stays testable without Airflow.

Scheduling
----------
`schedule="@daily"` with catchup enabled over a bounded window, so turning the
DAG on produces 14 real daily runs against 14 real days of 311 data rather than
one synthetic run. Each run's batch date is its own data interval, which is what
makes the pipeline incremental: run N processes day N and nothing else.

To run it continuously against current data instead, drop `end_date` and set
LOOKBACK_DAYS to however far the open-data feed lags publication.
"""
from __future__ import annotations

import os
import pendulum
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/project")
PY_BIN = os.getenv("PIPELINE_PYTHON", "python")
DBT_DIR = f"{PROJECT_ROOT}/dbt/nyc311"

# The 311 feed publishes with a lag; a production daily run would target
# (data_interval_start - LOOKBACK_DAYS). Zero here because the demo window is
# historical and therefore already complete.
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))
BATCH_DATE = "{{ macros.ds_add(data_interval_start | ds, -%d) }}" % LOOKBACK_DAYS
BATCH_ID = "{{ run_id }}"

DBT_ENV = {
    "PROJECT_ROOT": PROJECT_ROOT,
    "DBT_PROFILES_DIR": DBT_DIR,
    "DBT_TARGET": os.getenv("DBT_TARGET", os.getenv("WAREHOUSE", "duckdb")),
    "DUCKDB_PATH": os.getenv("DUCKDB_PATH", f"{PROJECT_ROOT}/warehouse/nyc311.duckdb"),
    "WAREHOUSE": os.getenv("WAREHOUSE", "duckdb"),
    "BQ_PROJECT": os.getenv("BQ_PROJECT", ""),
    "BQ_RAW_DATASET": os.getenv("BQ_RAW_DATASET", "nyc311_raw"),
    "GOOGLE_APPLICATION_CREDENTIALS": os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""),
}


def alert_on_failure(context) -> None:
    """Failure alerting hook.

    Emits one structured JSON line per failed task and appends it to
    reports/alerts.jsonl. This is the integration point for a real notifier -
    swapping in Slack, PagerDuty or SMTP is a change to this function only, and
    `email` / `email_on_failure` in default_args below is the SMTP path once an
    Airflow SMTP connection is configured.
    """
    import json
    from datetime import datetime, timezone

    task_instance = context.get("task_instance")
    alert = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "event": "task_failed",
        "dag_id": context["dag"].dag_id,
        "task_id": task_instance.task_id,
        "run_id": context["run_id"],
        "batch_date": str(context["data_interval_start"].date()),
        "try_number": task_instance.try_number,
        "max_tries": task_instance.max_tries,
        "log_url": task_instance.log_url,
        "exception": str(context.get("exception"))[:1000],
    }
    print("ALERT " + json.dumps(alert))

    alerts_path = f"{PROJECT_ROOT}/reports/alerts.jsonl"
    try:
        os.makedirs(os.path.dirname(alerts_path), exist_ok=True)
        with open(alerts_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(alert) + "\n")
    except OSError as exc:  # never let alerting mask the original failure
        print(f"ALERT_WRITE_FAILED {exc}")


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": alert_on_failure,
    "execution_timeout": pendulum.duration(minutes=30),
    # Configure an Airflow SMTP connection and flip this on for email alerts:
    # "email": ["data-oncall@example.com"],
    # "email_on_failure": True,
}

with DAG(
    dag_id="nyc311_ai_etl",
    description="AI-augmented incremental ELT: NYC 311 -> canonical schema -> dbt -> warehouse",
    default_args=default_args,
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 8, tz="UTC"),
    end_date=pendulum.datetime(2024, 1, 22, tz="UTC"),
    catchup=True,
    max_active_runs=1,          # incremental models must not race each other
    doc_md=__doc__,
    tags=["elt", "dbt", "duckdb", "bigquery", "ai", "nyc311"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        # Pull one day of 311 requests and land one file per upstream source
        # system, each with its own header dialect.
        bash_command=(
            f"cd {PROJECT_ROOT} && {PY_BIN} -m pipeline.cli extract "
            f"--batch-date {BATCH_DATE}"
        ),
        env=DBT_ENV,
        append_env=True,
        retries=4,              # the public API is the flakiest dependency
        doc_md="Pull one batch date from the NYC Open Data (Socrata) API into `data/raw/dt=<date>/`.",
    )

    ai_schema_map_and_load = BashOperator(
        task_id="ai_schema_map_and_load",
        # The AI step. Resolves each source system's headers to the canonical
        # schema (override -> exact -> LLM -> fuzzy fallback), then loads the
        # renamed rows into the warehouse's raw schema.
        bash_command=(
            f"cd {PROJECT_ROOT} && {PY_BIN} -m pipeline.cli map-and-load "
            f"--batch-date {BATCH_DATE} --batch-id {BATCH_ID}"
        ),
        env=DBT_ENV,
        append_env=True,
        doc_md=(
            "AI-assisted schema mapping and load. Never fails on an LLM outage: "
            "the fuzzy fallback resolves what the LLM cannot, and the task fails "
            "only if a *required* canonical field is left unmapped."
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}",
        env=DBT_ENV,
        append_env=True,
        doc_md="Build staging views, the deduped intermediate view, and the incremental fact + marts.",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        # `|| true` hands the pass/fail decision to quality_report, which fails
        # the run on genuine test failures but tolerates warn-severity tests.
        bash_command=f"cd {DBT_DIR} && (dbt test --profiles-dir {DBT_DIR} || true)",
        env=DBT_ENV,
        append_env=True,
        doc_md="Run the dbt test suite. Results are parsed by the next task from run_results.json.",
    )

    def _build_report(**context):
        import sys
        sys.path.insert(0, PROJECT_ROOT)
        from pipeline.quality_report import build_report

        batch_date = str(context["data_interval_start"].date())
        report = build_report(batch_date, context["run_id"])
        dbt = report["dbt_tests"]
        if dbt.get("available") and (dbt["tests_failed"] or dbt["tests_errored"]):
            raise ValueError(
                f"dbt data quality gate failed: {dbt['tests_failed']} failed, "
                f"{dbt['tests_errored']} errored - see {report['batch_date']} report."
            )
        return {k: report[k] for k in ("row_counts", "dbt_tests")}

    quality_report = PythonOperator(
        task_id="quality_report",
        python_callable=_build_report,
        doc_md=(
            "Assemble the run's data quality report (row counts per stage, dbt test "
            "pass rate, AI mapping confidence distribution) and fail the run if any "
            "dbt test failed or errored."
        ),
    )

    extract >> ai_schema_map_and_load >> dbt_run >> dbt_test >> quality_report
