"""Tests for the AI schema mapper.

The point of these is the failure paths. The happy path (LLM returns good
mappings) is the easy case; what has to be proven is that a broken, slow,
refusing or hallucinating model degrades to the deterministic fallback instead
of taking the pipeline down or - worse - writing data into the wrong column.
"""
from __future__ import annotations

import pytest

from pipeline import ai_schema_mapper as mapper
from pipeline.ai_schema_mapper import (
    METHOD_EXACT,
    METHOD_FUZZY,
    METHOD_LLM,
    METHOD_OVERRIDE,
    METHOD_UNMAPPED,
    map_headers,
    normalize,
)
from pipeline.canonical_schema import CANONICAL_NAMES
from pipeline.source_dialects import DIALECTS, expected_mapping


def decision_for(result, column):
    return next(d for d in result.decisions if d.source_column == column)


# --------------------------------------------------------------------------
# Tier 1 - exact
# --------------------------------------------------------------------------
@pytest.mark.parametrize("raw,expected", [
    ("Service Request ID", "service_request_id"),
    ("geoLat", "geo_lat"),
    ("SR_NUMBER", "sr_number"),
    ("Last Update Of Outcome", "last_update_of_outcome"),
    ("  spaced  out  ", "spaced_out"),
])
def test_normalize(raw, expected):
    assert normalize(raw) == expected


def test_exact_tier_matches_canonical_names_and_synonyms():
    result = map_headers("t", ["created_at", "Incident Zip"], use_llm=False, strict_required=False)
    assert decision_for(result, "created_at").canonical_field == "created_at"
    assert decision_for(result, "created_at").method == METHOD_EXACT
    # 'incident zip' is a declared synonym of postal_code.
    assert decision_for(result, "Incident Zip").canonical_field == "postal_code"


# --------------------------------------------------------------------------
# Tier 3 - fuzzy fallback
# --------------------------------------------------------------------------
def test_fuzzy_resolves_close_variants_when_llm_is_off():
    result = map_headers("t", ["ZIP_CD"], use_llm=False, strict_required=False)
    decision = decision_for(result, "ZIP_CD")
    assert decision.canonical_field == "postal_code"
    assert decision.method == METHOD_FUZZY


def test_fuzzy_refuses_to_guess_below_threshold():
    result = map_headers("t", ["ROW_CHECKSUM"], use_llm=False, strict_required=False)
    decision = decision_for(result, "ROW_CHECKSUM")
    assert decision.canonical_field is None
    assert decision.method == METHOD_UNMAPPED
    assert "threshold" in decision.reasoning


def test_operational_columns_are_never_mapped_by_the_fallback():
    """Audit/lineage columns must not be forced into a canonical field."""
    distractors = [c for d in DIALECTS for c in d.extra_columns]
    result = map_headers("t", distractors, use_llm=False, strict_required=False)
    assert all(not d.mapped for d in result.decisions), \
        [(d.source_column, d.canonical_field) for d in result.decisions if d.mapped]


# --------------------------------------------------------------------------
# Tier 2 - LLM, and every way it can fail
# --------------------------------------------------------------------------
def _stub_llm(monkeypatch, proposals, meta=None):
    def fake(source_system, columns, samples):
        return proposals, meta or {"latency_ms": 12, "input_tokens": 100, "output_tokens": 50}
    monkeypatch.setattr(mapper, "_call_llm", fake)


def test_llm_proposal_is_accepted_above_the_confidence_floor(monkeypatch):
    _stub_llm(monkeypatch, [{
        "source_column": "INTK_SRC", "canonical_field": "intake_channel",
        "confidence": 0.93, "reasoning": "abbreviation of intake source",
    }])
    result = map_headers("t", ["INTK_SRC"], use_llm=True, strict_required=False)
    decision = decision_for(result, "INTK_SRC")
    assert (decision.canonical_field, decision.method) == ("intake_channel", METHOD_LLM)
    assert result.llm_used is True


def test_llm_exception_falls_back_and_does_not_raise(monkeypatch):
    def boom(*_a, **_k):
        raise ConnectionError("API unreachable")
    monkeypatch.setattr(mapper, "_call_llm", boom)

    result = map_headers("t", ["ZIP_CD"], use_llm=True, strict_required=False)
    assert result.llm_used is False
    assert "ConnectionError" in result.llm_error
    # The column still resolved - via the fallback.
    assert decision_for(result, "ZIP_CD").method == METHOD_FUZZY


def test_llm_low_confidence_is_rejected_and_handed_to_fallback(monkeypatch):
    _stub_llm(monkeypatch, [{
        "source_column": "ZIP_CD", "canonical_field": "borough",
        "confidence": 0.11, "reasoning": "not sure at all",
    }])
    result = map_headers("t", ["ZIP_CD"], use_llm=True, strict_required=False)
    decision = decision_for(result, "ZIP_CD")
    assert result.llm_low_confidence == 1
    assert decision.method == METHOD_FUZZY
    assert decision.canonical_field == "postal_code"   # fallback got it right


def test_hallucinated_canonical_field_is_rejected(monkeypatch):
    _stub_llm(monkeypatch, [{
        "source_column": "ZIP_CD", "canonical_field": "zip_code_field_that_does_not_exist",
        "confidence": 0.99, "reasoning": "confidently wrong",
    }])
    result = map_headers("t", ["ZIP_CD"], use_llm=True, strict_required=False)
    assert result.llm_invalid_proposals == 1
    decision = decision_for(result, "ZIP_CD")
    assert decision.canonical_field in CANONICAL_NAMES
    assert decision.method == METHOD_FUZZY


def test_llm_null_mapping_is_respected(monkeypatch):
    _stub_llm(monkeypatch, [{
        "source_column": "ROW_CHECKSUM", "canonical_field": None,
        "confidence": 0.97, "reasoning": "audit column, no canonical equivalent",
    }])
    result = map_headers("t", ["ROW_CHECKSUM"], use_llm=True, strict_required=False)
    decision = decision_for(result, "ROW_CHECKSUM")
    assert decision.canonical_field is None
    assert decision.method == METHOD_LLM


def test_columns_the_model_omits_still_get_resolved(monkeypatch):
    """A short or truncated LLM response must not leave columns undecided."""
    _stub_llm(monkeypatch, [])
    result = map_headers("t", ["ZIP_CD", "ROW_CHECKSUM"], use_llm=True, strict_required=False)
    assert decision_for(result, "ZIP_CD").method == METHOD_FUZZY
    assert decision_for(result, "ROW_CHECKSUM").method == METHOD_UNMAPPED


# --------------------------------------------------------------------------
# Collision handling and guardrails
# --------------------------------------------------------------------------
def test_two_columns_cannot_claim_the_same_canonical_field(monkeypatch):
    _stub_llm(monkeypatch, [
        {"source_column": "col_a", "canonical_field": "borough", "confidence": 0.80, "reasoning": "a"},
        {"source_column": "col_b", "canonical_field": "borough", "confidence": 0.95, "reasoning": "b"},
    ])
    result = map_headers("t", ["col_a", "col_b"], use_llm=True, strict_required=False)
    winners = [d for d in result.decisions if d.canonical_field == "borough"]
    assert len(winners) == 1
    assert winners[0].source_column == "col_b"          # higher confidence wins
    assert decision_for(result, "col_a").canonical_field is None


def test_missing_required_field_fails_the_run():
    with pytest.raises(ValueError, match="required canonical fields left unmapped"):
        map_headers("t", ["ROW_CHECKSUM"], use_llm=False, strict_required=True)


def test_human_override_wins_over_every_other_tier(monkeypatch):
    _stub_llm(monkeypatch, [{
        "source_column": "Issue Category", "canonical_field": "descriptor",
        "confidence": 0.99, "reasoning": "model disagrees with the human",
    }])
    result = map_headers(
        "t", ["Issue Category"], use_llm=True, strict_required=False,
        overrides={"Issue Category": "complaint_type"},
    )
    decision = decision_for(result, "Issue Category")
    assert (decision.canonical_field, decision.method) == ("complaint_type", METHOD_OVERRIDE)
    assert result.human_overrides == 1


def test_overrides_reject_unknown_canonical_fields(tmp_path):
    bad = tmp_path / "overrides.yml"
    bad.write_text("some_system:\n  Some Column: not_a_real_field\n")
    with pytest.raises(ValueError, match="unknown"):
        mapper.load_overrides(bad)


# --------------------------------------------------------------------------
# Accuracy regression - the fallback must never mis-map, only decline
# --------------------------------------------------------------------------
def test_fallback_precision_is_perfect_across_every_dialect():
    """The fallback may miss a column, but it must never map one to the wrong field.

    A miss costs a null column; a mis-map silently writes wrong values into a
    real field, which is far more expensive. This test pins that property.
    """
    wrong = []
    for dialect in DIALECTS:
        truth = expected_mapping(dialect)
        result = map_headers(dialect.name, list(truth), use_llm=False, strict_required=False)
        for decision in result.decisions:
            if decision.mapped and decision.canonical_field != truth[decision.source_column]:
                wrong.append((dialect.name, decision.source_column,
                              decision.canonical_field, truth[decision.source_column]))
    assert wrong == [], f"fallback mis-mapped columns: {wrong}"
