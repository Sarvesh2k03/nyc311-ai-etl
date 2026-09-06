"""Load: land canonical rows in the warehouse's raw schema.

Two backends behind one interface so the pipeline code, the DAG and the dbt
profile all switch together off the WAREHOUSE env var:

  duckdb   - local file, zero setup, what the reference run uses.
  bigquery - BigQuery sandbox (free, no credit card, 60-day table expiry).

Loads are idempotent per batch: a re-run deletes that batch date's rows before
inserting, so a retried Airflow task never double-counts.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from pipeline import config
from pipeline.logging_utils import get_logger, log_event

log = get_logger("pipeline.load")

RAW_COLUMNS_SQL = "batch_date DATE"


class Warehouse(ABC):
    name: str

    @abstractmethod
    def ensure_schema(self) -> None: ...

    @abstractmethod
    def replace_batch(self, df: pd.DataFrame, batch_date: str) -> int: ...

    @abstractmethod
    def row_count(self, relation: str) -> int: ...


class DuckDBWarehouse(Warehouse):
    name = "duckdb"

    def __init__(self, path=None, schema: str | None = None):
        self.path = str(path or config.DUCKDB_PATH)
        self.schema = schema or config.RAW_SCHEMA
        self.table = f"{self.schema}.{config.RAW_TABLE}"

    def _connect(self):
        import duckdb
        return duckdb.connect(self.path)

    def ensure_schema(self) -> None:
        with self._connect() as con:
            for schema in (self.schema, "staging", "analytics"):
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def replace_batch(self, df: pd.DataFrame, batch_date: str) -> int:
        self.ensure_schema()
        with self._connect() as con:
            con.register("incoming", df)
            existing = con.execute(
                "SELECT count(*) FROM duckdb_tables() WHERE schema_name = ? AND table_name = ?",
                [self.schema, config.RAW_TABLE],
            ).fetchone()[0]
            if existing:
                deleted = con.execute(
                    f"DELETE FROM {self.table} WHERE _batch_date = ?", [batch_date]
                ).fetchone()
                log_event(log, "load.batch_cleared", batch_date=batch_date,
                          rows_deleted=deleted[0] if deleted else 0)
                con.execute(f"INSERT INTO {self.table} SELECT * FROM incoming")
            else:
                con.execute(f"CREATE TABLE {self.table} AS SELECT * FROM incoming")
            return con.execute(
                f"SELECT count(*) FROM {self.table} WHERE _batch_date = ?", [batch_date]
            ).fetchone()[0]

    def row_count(self, relation: str) -> int:
        with self._connect() as con:
            try:
                return con.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]
            except Exception:
                return -1


class BigQueryWarehouse(Warehouse):
    name = "bigquery"

    def __init__(self, project: str | None = None, dataset: str | None = None):
        self.project = project or config.BQ_PROJECT
        self.dataset = dataset or config.BQ_RAW_DATASET
        if not self.project:
            raise ValueError("BQ_PROJECT must be set to load into BigQuery.")
        self.table = f"{self.project}.{self.dataset}.{config.RAW_TABLE}"

    def _client(self):
        from google.cloud import bigquery
        return bigquery.Client(project=self.project, location=config.BQ_LOCATION)

    def ensure_schema(self) -> None:
        from google.cloud import bigquery
        client = self._client()
        for dataset in (self.dataset, "nyc311_staging", "nyc311_analytics"):
            ref = bigquery.Dataset(f"{self.project}.{dataset}")
            ref.location = config.BQ_LOCATION
            client.create_dataset(ref, exists_ok=True)

    def replace_batch(self, df: pd.DataFrame, batch_date: str) -> int:
        from google.cloud import bigquery
        self.ensure_schema()
        client = self._client()
        # Delete-then-append keeps the task retry-safe. The table may not exist
        # on the very first run, which is not an error.
        try:
            client.query(
                f"DELETE FROM `{self.table}` WHERE _batch_date = @d",
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("d", "STRING", batch_date)]
                ),
            ).result()
        except Exception as exc:
            log_event(log, "load.delete_skipped", batch_date=batch_date, reason=str(exc)[:200])
        client.load_table_from_dataframe(
            df, self.table,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        ).result()
        return int(next(iter(client.query(
            f"SELECT count(*) AS n FROM `{self.table}` WHERE _batch_date = '{batch_date}'"
        ).result())).n)

    def row_count(self, relation: str) -> int:
        try:
            return int(next(iter(self._client().query(
                f"SELECT count(*) AS n FROM `{self.project}.{relation}`"
            ).result())).n)
        except Exception:
            return -1


def get_warehouse(kind: str | None = None) -> Warehouse:
    kind = (kind or config.WAREHOUSE).lower()
    if kind == "duckdb":
        return DuckDBWarehouse()
    if kind == "bigquery":
        return BigQueryWarehouse()
    raise ValueError(f"Unknown WAREHOUSE '{kind}'. Use 'duckdb' or 'bigquery'.")
