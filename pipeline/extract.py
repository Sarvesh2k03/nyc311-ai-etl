"""Extract: pull one logical day of NYC 311 requests and land it as raw files.

Incremental by design - the unit of work is a single batch date (the Airflow
data interval), so a backfill is N independent runs and a re-run of one day
overwrites only that day's partition. Rows are sharded across the source
dialects so every run lands one file per upstream system, each with its own
header convention.
"""
from __future__ import annotations

import csv
import json
from datetime import date, timedelta
from pathlib import Path

import requests

from pipeline import config
from pipeline.logging_utils import get_logger, log_event, timed
from pipeline.source_dialects import DIALECTS, SOCRATA_TO_CANONICAL, SourceDialect

log = get_logger("pipeline.extract")


def _fetch_day(batch_date: date, row_limit: int) -> list[dict]:
    """Page through the Socrata API for a single created_date day."""
    start = batch_date.isoformat() + "T00:00:00"
    end = (batch_date + timedelta(days=1)).isoformat() + "T00:00:00"
    where = f"created_date >= '{start}' AND created_date < '{end}'"
    headers = {"X-App-Token": config.SOCRATA_APP_TOKEN} if config.SOCRATA_APP_TOKEN else {}

    rows: list[dict] = []
    offset = 0
    while len(rows) < row_limit:
        page_size = min(config.EXTRACT_PAGE_SIZE, row_limit - len(rows))
        resp = requests.get(
            config.SOCRATA_DATASET_URL,
            params={
                "$where": where,
                "$limit": page_size,
                "$offset": offset,
                "$order": "unique_key",
                "$select": ",".join(SOCRATA_TO_CANONICAL),
            },
            headers=headers,
            timeout=120,
        )
        resp.raise_for_status()
        page = resp.json()
        rows.extend(page)
        log_event(log, "extract.page", batch_date=str(batch_date),
                  offset=offset, returned=len(page), total=len(rows))
        if len(page) < page_size:
            break
        offset += page_size
    return rows


def _write_dialect_file(dialect: SourceDialect, rows: list[dict], out_dir: Path) -> Path:
    """Write rows using this dialect's header names, plus its distractor columns."""
    out_path = out_dir / f"{dialect.name}.csv"
    header = [dialect.headers[c] for c in dialect.headers] + list(dialect.extra_columns)
    # canonical field -> socrata field, to read values back out of the API rows
    canonical_to_socrata = {v: k for k, v in SOCRATA_TO_CANONICAL.items()}

    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=header, extrasaction="ignore")
        writer.writeheader()
        for i, row in enumerate(rows):
            out = {
                dialect.headers[canonical]: row.get(canonical_to_socrata[canonical], "")
                for canonical in dialect.headers
            }
            for extra in dialect.extra_columns:
                out[extra] = f"{dialect.name}-{i}"
            writer.writerow(out)
    return out_path


def extract_batch(batch_date: date, row_limit: int | None = None) -> dict:
    """Land one batch date as one CSV per source dialect. Returns a manifest."""
    row_limit = row_limit or config.EXTRACT_ROW_LIMIT_PER_DAY
    out_dir = config.RAW_DIR / f"dt={batch_date.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)

    with timed(log, "extract", batch_date=str(batch_date), row_limit=row_limit):
        rows = _fetch_day(batch_date, row_limit)
        if not rows:
            raise ValueError(f"No 311 rows returned for {batch_date}; refusing to land an empty batch.")

        # Round-robin shard so each upstream system gets a comparable slice.
        shards: dict[str, list[dict]] = {d.name: [] for d in DIALECTS}
        for i, row in enumerate(rows):
            shards[DIALECTS[i % len(DIALECTS)].name].append(row)

        files = []
        for dialect in DIALECTS:
            path = _write_dialect_file(dialect, shards[dialect.name], out_dir)
            files.append({
                "source_system": dialect.name,
                "path": str(path),
                "rows": len(shards[dialect.name]),
                "columns": len(dialect.headers) + len(dialect.extra_columns),
            })
            log_event(log, "extract.file_written", source_system=dialect.name,
                      rows=len(shards[dialect.name]), path=str(path))

    manifest = {
        "batch_date": batch_date.isoformat(),
        "total_rows": len(rows),
        "files": files,
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2))
    log_event(log, "extract.complete", batch_date=str(batch_date), total_rows=len(rows),
              files=len(files))
    return manifest
