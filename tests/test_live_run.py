"""Tests for the live pipeline run.

The network call is stubbed so these stay fast and deterministic; what is being
tested is the pipeline around it - sharding, mapping, landing, and the quality
checks - plus the two things most likely to break in front of a viewer: an API
outage, and a host with no warehouse installed.
"""
from __future__ import annotations

import pandas as pd
import pytest

from app import live_run as lr


def _fake_rows(n: int = 40) -> list[dict]:
    """Rows shaped exactly like the Socrata response."""
    return [
        {
            "unique_key": str(100000 + i),
            "created_date": f"2026-09-0{(i % 3) + 1}T09:0{i % 10}:00.000",
            "closed_date": f"2026-09-0{(i % 3) + 2}T11:00:00.000",
            "agency": "NYPD" if i % 2 else "HPD",
            "agency_name": "New York City Police Department" if i % 2 else "Housing",
            "complaint_type": "Noise - Residential" if i % 2 else "HEAT/HOT WATER",
            "descriptor": "Loud Music/Party",
            "location_type": "Residential Building",
            "incident_zip": "11211",
            "incident_address": "1 MAIN STREET",
            "city": "BROOKLYN",
            "borough": "BROOKLYN",
            "status": "Closed" if i % 2 else "Open",
            "resolution_description": "The Department responded.",
            "resolution_action_updated_date": "2026-09-03T11:00:00.000",
            "community_board": "01 BROOKLYN",
            "open_data_channel_type": "ONLINE",
            "latitude": "40.71884",
            "longitude": "-73.95448",
        }
        for i in range(n)
    ]


@pytest.fixture
def stub_api(monkeypatch):
    monkeypatch.setattr(lr, "fetch_newest", lambda limit: _fake_rows(40))


def test_rows_are_sharded_across_every_source_system():
    shards = lr.shard_into_dialects(_fake_rows(40))
    assert set(shards) == {d.name for d in lr.DIALECTS}
    assert sum(len(f) for f in shards.values()) == 40
    # Each shard must carry that system's own header names, not canonical ones.
    assert "SR_NUMBER" in shards["legacy_crm"].columns
    assert "srId" in shards["mobile_app_v2"].columns


def test_live_run_completes_without_a_warehouse(stub_api):
    """The path a hosting platform takes when dbt and duckdb are not installed."""
    result = lr.run_live_pipeline(limit=40, use_llm=False, force_pandas=True)

    assert result.error is None
    assert result.engine == "pandas"
    assert len(result.canonical) == 40
    assert result.checks, "the fallback must still assert something"
    assert result.checks_passed == len(result.checks)
    assert len(result.steps) == 5


def test_live_run_reports_mapping_stats(stub_api):
    result = lr.run_live_pipeline(limit=40, use_llm=False, force_pandas=True)
    stats = result.mapping_stats
    assert stats["columns"] == 83          # 4 source systems' headers, every run
    assert 0 < stats["auto_match_rate"] <= 1
    assert not result.decisions.empty


def test_api_failure_is_reported_not_raised(monkeypatch):
    """An outage must degrade to a message, never a stack trace in the page."""
    def boom(limit):
        raise ConnectionError("311 API unreachable")
    monkeypatch.setattr(lr, "fetch_newest", boom)

    result = lr.run_live_pipeline(limit=40, use_llm=False, force_pandas=True)
    assert result.error is not None
    assert "ConnectionError" in result.error
    assert result.steps and result.steps[-1].ok is False


def test_quality_checks_catch_bad_data():
    """The checks have to actually fail on data that violates them."""
    bad = pd.DataFrame([{
        "request_id": "1", "created_at": "2099-01-01T00:00:00", "closed_at": "",
        "agency_code": "NYPD", "complaint_type": "Noise", "borough": "ATLANTIS",
        "latitude": "88.0", "longitude": "-73.9",
    }])
    results = {c.name: c for c in lr.pandas_checks(bad)}

    assert not results["not_in_future: created_at"].passed
    assert not results["within_range: latitude"].passed
    assert not results["accepted_values: borough"].passed
    assert results["within_range: longitude"].passed
