"""Transform: apply the resolved column map and attach lineage.

Deliberately thin. All business logic lives in dbt - this step only renames
columns to the canonical contract, drops what no tier would map, and stamps the
lineage columns. Values are carried as strings; casting happens in the dbt
staging layer where it is covered by tests.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from pipeline.ai_schema_mapper import MappingResult
from pipeline.canonical_schema import CANONICAL_NAMES
from pipeline.logging_utils import get_logger, log_event

log = get_logger("pipeline.transform")

SAMPLE_ROWS = 5


def read_source_file(path: Path) -> pd.DataFrame:
    """Read an upstream file with every column as a string, blanks as empty."""
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])


def sample_values(df: pd.DataFrame, n: int = SAMPLE_ROWS) -> dict[str, list[str]]:
    """First n non-empty values per column, used as context for the LLM mapper."""
    return {
        col: [v for v in df[col].head(n * 4).tolist() if str(v).strip()][:n]
        for col in df.columns
    }


def apply_mapping(
    df: pd.DataFrame,
    result: MappingResult,
    batch_date: date | str,
    batch_id: str,
) -> pd.DataFrame:
    """Rename to canonical columns, drop unmapped ones, add lineage columns."""
    column_map = result.column_map()
    dropped = [c for c in df.columns if c not in column_map]

    out = df[list(column_map)].rename(columns=column_map)
    # Guarantee a stable column set regardless of what this source provided,
    # so every batch lands with the same shape.
    for field in CANONICAL_NAMES:
        if field not in out.columns:
            out[field] = pd.NA
    out = out[list(CANONICAL_NAMES)]

    out["_source_system"] = result.source_system
    out["_batch_date"] = str(batch_date)
    out["_batch_id"] = batch_id
    out["_ingested_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    log_event(log, "transform.applied", source_system=result.source_system,
              rows=len(out), mapped_columns=len(column_map), dropped_columns=dropped)
    return out
