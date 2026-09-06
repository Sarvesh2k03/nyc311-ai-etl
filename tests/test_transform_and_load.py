"""Tests for the transform and load stages."""
from __future__ import annotations

import pandas as pd
import pytest

from pipeline.ai_schema_mapper import MappingDecision, MappingResult, METHOD_EXACT, METHOD_UNMAPPED
from pipeline.canonical_schema import CANONICAL_NAMES, LINEAGE_COLUMNS
from pipeline.load import DuckDBWarehouse
from pipeline.transform import apply_mapping, sample_values


def _result(decisions):
    return MappingResult(source_system="test_src", batch_date="2024-01-15",
                         decisions=decisions, llm_used=False)


def test_apply_mapping_renames_drops_and_stamps_lineage():
    df = pd.DataFrame({
        "SR_NUMBER": ["1", "2"],
        "AGCY": ["NYPD", "DSNY"],
        "ROW_CHECKSUM": ["abc", "def"],
    })
    result = _result([
        MappingDecision("SR_NUMBER", "request_id", METHOD_EXACT, 1.0),
        MappingDecision("AGCY", "agency_code", METHOD_EXACT, 1.0),
        MappingDecision("ROW_CHECKSUM", None, METHOD_UNMAPPED, 0.4),
    ])

    out = apply_mapping(df, result, "2024-01-15", "batch-1")

    assert list(out.columns) == list(CANONICAL_NAMES) + list(LINEAGE_COLUMNS)
    assert out["request_id"].tolist() == ["1", "2"]
    assert "ROW_CHECKSUM" not in out.columns          # unmapped columns are dropped
    assert out["_source_system"].unique().tolist() == ["test_src"]
    assert out["_batch_id"].unique().tolist() == ["batch-1"]


def test_unmapped_canonical_fields_are_present_but_null():
    """Every batch must land with the same column set, whatever the source had."""
    df = pd.DataFrame({"SR_NUMBER": ["1"]})
    result = _result([MappingDecision("SR_NUMBER", "request_id", METHOD_EXACT, 1.0)])

    out = apply_mapping(df, result, "2024-01-15", "batch-1")

    assert set(CANONICAL_NAMES).issubset(out.columns)
    assert out["borough"].isna().all()


def test_sample_values_skips_blanks():
    df = pd.DataFrame({"col": ["", "  ", "real", "also real"]})
    assert sample_values(df, n=2)["col"] == ["real", "also real"]


def test_duckdb_load_is_idempotent(tmp_path):
    """Re-running a batch must replace it, not duplicate it - task retries depend on this."""
    warehouse = DuckDBWarehouse(path=tmp_path / "test.duckdb", schema="raw")
    frame = pd.DataFrame({
        "request_id": ["1", "2"],
        "_source_system": ["a", "a"],
        "_batch_date": ["2024-01-15", "2024-01-15"],
        "_batch_id": ["b1", "b1"],
        "_ingested_at": ["2024-01-15T00:00:00", "2024-01-15T00:00:00"],
    })

    assert warehouse.replace_batch(frame, "2024-01-15") == 2
    assert warehouse.replace_batch(frame, "2024-01-15") == 2       # not 4
    assert warehouse.row_count("raw.service_requests_raw") == 2


def test_duckdb_load_keeps_other_batches(tmp_path):
    warehouse = DuckDBWarehouse(path=tmp_path / "test.duckdb", schema="raw")

    def frame(batch_date, ids):
        return pd.DataFrame({
            "request_id": ids,
            "_source_system": ["a"] * len(ids),
            "_batch_date": [batch_date] * len(ids),
            "_batch_id": [f"b-{batch_date}"] * len(ids),
            "_ingested_at": ["2024-01-15T00:00:00"] * len(ids),
        })

    warehouse.replace_batch(frame("2024-01-15", ["1", "2"]), "2024-01-15")
    warehouse.replace_batch(frame("2024-01-16", ["3"]), "2024-01-16")
    warehouse.replace_batch(frame("2024-01-15", ["1", "2"]), "2024-01-15")   # re-run day 1

    assert warehouse.row_count("raw.service_requests_raw") == 3


def test_unknown_warehouse_is_rejected():
    from pipeline.load import get_warehouse
    with pytest.raises(ValueError, match="Unknown WAREHOUSE"):
        get_warehouse("snowflake")
