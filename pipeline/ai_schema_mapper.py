"""AI-assisted schema mapping with a deterministic fallback.

Problem: each upstream system names its columns differently, so before anything
can be loaded we must decide, for every incoming header, which canonical field
it means - or that it means none of them.

Four tiers, cheapest first. A column is resolved by the first tier that will
commit to it, and every decision is logged with the tier that made it:

  0. override - a mapping a human recorded in config/mapping_overrides.yml after
              a previous run could not resolve it. Highest priority, and counted
              separately from the auto-match rate: a column a person mapped is
              not a column the pipeline mapped.
  1. exact  - normalized string equality against a canonical name or synonym.
              Free, deterministic, runs first so the LLM is never asked about
              columns that are already unambiguous.
  2. llm    - one Claude call for all remaining columns at once, with sample
              values for context. Proposals are validated against the canonical
              field list and dropped below a confidence floor.
  3. fuzzy  - rapidfuzz similarity against canonical names and synonyms. This is
              the fallback: it runs whenever the LLM is disabled, errors, times
              out, refuses, returns malformed output, hallucinates a field name,
              or is not confident enough. The pipeline therefore has no hard
              dependency on the LLM being reachable.

Anything no tier will commit to is left `unmapped` and dropped at load time,
which is the correct outcome for audit/checksum columns. If a *required*
canonical field ends up unmapped, the run fails loudly rather than silently
loading a table with a missing key.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path

import yaml
from rapidfuzz import fuzz, process

from pipeline import config
from pipeline.canonical_schema import (
    CANONICAL_FIELDS,
    CANONICAL_NAMES,
    REQUIRED_FIELDS,
    schema_for_prompt,
)
from pipeline.logging_utils import get_logger, log_event

log = get_logger("pipeline.ai_schema_mapper")

METHOD_OVERRIDE = "override"
METHOD_EXACT = "exact"
METHOD_LLM = "llm"
METHOD_FUZZY = "fuzzy"
METHOD_UNMAPPED = "unmapped"


OVERRIDES_PATH = config.PROJECT_ROOT / "config" / "mapping_overrides.yml"


def load_overrides(path: Path | None = None) -> dict[str, dict[str, str | None]]:
    """Load human-recorded mappings, keyed by source system then source column."""
    path = path or OVERRIDES_PATH
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text()) or {}
    cleaned: dict[str, dict[str, str | None]] = {}
    for system, columns in loaded.items():
        cleaned[system] = {}
        for column, target in (columns or {}).items():
            if target is not None and target not in CANONICAL_NAMES:
                raise ValueError(
                    f"mapping_overrides.yml: '{system}.{column}' targets unknown "
                    f"canonical field '{target}'"
                )
            cleaned[system][column] = target
    return cleaned


# --------------------------------------------------------------------------
# Result types
# --------------------------------------------------------------------------
@dataclass
class MappingDecision:
    source_column: str
    canonical_field: str | None
    method: str
    confidence: float
    reasoning: str = ""

    @property
    def mapped(self) -> bool:
        return self.canonical_field is not None


@dataclass
class MappingResult:
    source_system: str
    batch_date: str
    decisions: list[MappingDecision]
    llm_used: bool
    llm_error: str | None = None
    llm_latency_ms: int = 0
    llm_input_tokens: int = 0
    llm_output_tokens: int = 0
    llm_invalid_proposals: int = 0      # hallucinated / non-existent canonical fields
    llm_low_confidence: int = 0         # proposals rejected by the confidence floor
    human_overrides: int = 0            # columns resolved from the override file
    model: str = ""
    stats: dict = field(default_factory=dict)

    def column_map(self) -> dict[str, str]:
        """source column -> canonical field, for the columns that resolved."""
        return {d.source_column: d.canonical_field for d in self.decisions if d.mapped}

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["decisions"] = [asdict(d) for d in self.decisions]
        return payload


# --------------------------------------------------------------------------
# Tier 1 - exact
# --------------------------------------------------------------------------
def normalize(name: str) -> str:
    """'Service Request ID' -> 'service_request_id'; 'geoLat' -> 'geo_lat'."""
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)          # camelCase split
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", spaced.lower())).strip("_")


_EXACT_INDEX: dict[str, str] = {}
for _f in CANONICAL_FIELDS:
    _EXACT_INDEX[normalize(_f.name)] = _f.name
    for _syn in _f.synonyms:
        _EXACT_INDEX.setdefault(normalize(_syn), _f.name)


def exact_match(column: str) -> str | None:
    return _EXACT_INDEX.get(normalize(column))


# --------------------------------------------------------------------------
# Tier 3 - fuzzy fallback (deterministic, always available)
# --------------------------------------------------------------------------
_FUZZY_CHOICES: dict[str, str] = {}   # candidate phrase -> canonical field
for _f in CANONICAL_FIELDS:
    _FUZZY_CHOICES[_f.name.replace("_", " ")] = _f.name
    for _syn in _f.synonyms:
        _FUZZY_CHOICES[_syn] = _f.name


def fuzzy_score(probe: str, phrase: str) -> float:
    """Similarity of a normalized header to a candidate phrase, 0-1.

    The score is the *weaker* of two measures, which is what makes it safe:

      WRatio           - good at abbreviations and partial overlap ('zip cd' vs 'zip'),
                         but it happily rewards a long operational name for sharing
                         a substring with a short field name.
      token_set_ratio  - measures how much of the two token sets actually overlap,
                         which is exactly what a spurious substring match lacks.

    Taking the minimum means a candidate has to be plausible under both.

    Measured on this project's four dialects, the combined score cleanly rejects
    operational/audit columns - every one of them scores <=0.69, well under the
    0.82 floor - which is the failure mode that actually corrupts data.

    What it cannot do is separate *semantic* near-misses: 'assignedAgencyLabel'
    scores 0.90 against agency_code (wrong) and 'boroughName' scores 0.90 against
    borough (right). No threshold splits those, because the difference is meaning,
    not spelling. That gap is the whole reason the LLM tier exists, and why the
    one-column-per-canonical-field collision guard is not optional.
    """
    return min(fuzz.WRatio(probe, phrase), fuzz.token_set_ratio(probe, phrase)) / 100.0


def fuzzy_candidates(column: str, top_n: int = 3) -> list[tuple[str, str, float]]:
    """Top-N (canonical field, matched phrase, score) candidates for a column.

    Exposes the ranking the fallback tier used, so a decision can be inspected
    rather than taken on trust - which is what the dashboard's mapper demo and
    any manual triage of a bad mapping both need.
    """
    probe = normalize(column).replace("_", " ")
    scored = sorted(
        ((_FUZZY_CHOICES[phrase], phrase, round(fuzzy_score(probe, phrase), 4))
         for phrase in _FUZZY_CHOICES),
        key=lambda row: row[2], reverse=True,
    )

    seen: set[str] = set()
    best: list[tuple[str, str, float]] = []
    for canonical, phrase, score in scored:
        if canonical in seen:
            continue
        seen.add(canonical)
        best.append((canonical, phrase, score))
        if len(best) == top_n:
            break
    return best


def fuzzy_match(column: str, min_score: float | None = None) -> tuple[str | None, float, str]:
    """Best canonical field for a column by string similarity, or (None, score, why)."""
    min_score = config.FUZZY_MIN_SCORE if min_score is None else min_score
    probe = normalize(column).replace("_", " ")

    best_phrase, best_score = None, 0.0
    for phrase in _FUZZY_CHOICES:
        score = fuzzy_score(probe, phrase)
        if score > best_score:
            best_phrase, best_score = phrase, score

    if best_phrase is None:
        return None, 0.0, "no fuzzy candidates"

    confidence = round(best_score, 4)
    canonical = _FUZZY_CHOICES[best_phrase]
    if confidence < min_score:
        return None, confidence, (
            f"best fuzzy candidate '{best_phrase}' ({canonical}) scored {confidence:.2f} "
            f"< threshold {min_score:.2f}"
        )
    return canonical, confidence, f"fuzzy match on '{best_phrase}' (score {confidence:.2f})"


# --------------------------------------------------------------------------
# Tier 2 - LLM
# --------------------------------------------------------------------------
_SYSTEM_PROMPT = """You map column headers from an upstream data source onto a fixed canonical schema for a data warehouse.

Rules:
- Map a column ONLY to a canonical field name from the provided list. Never invent a field name.
- If a column does not correspond to any canonical field, return null for canonical_field. Operational and audit columns (checksums, batch refs, load sequences, app versions, export timestamps, device metadata) map to null - it is much better to leave a column unmapped than to map it to a field it does not mean.
- Never map two different columns to the same canonical field.
- confidence is your genuine probability that the mapping is correct, from 0.0 to 1.0. Use low confidence when the header is ambiguous rather than guessing high.
- reasoning is one short sentence citing the header text and/or the sample values that decided it."""


def _build_user_prompt(source_system: str, columns: list[str], samples: dict[str, list[str]]) -> str:
    lines = [
        f"Upstream source system: {source_system}",
        "",
        "Canonical schema (the only valid targets):",
        schema_for_prompt(),
        "",
        "Columns to map, with sample values from the incoming file:",
    ]
    for col in columns:
        vals = [v for v in samples.get(col, []) if v][:3]
        rendered = " | ".join(str(v)[:60] for v in vals) if vals else "(all empty in sample)"
        lines.append(f"- {col}  ->  samples: {rendered}")
    return "\n".join(lines)


_LLM_SCHEMA = {
    "type": "object",
    "properties": {
        "mappings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "source_column": {"type": "string"},
                    "canonical_field": {"type": ["string", "null"], "enum": list(CANONICAL_NAMES) + [None]},
                    "confidence": {"type": "number"},
                    "reasoning": {"type": "string"},
                },
                "required": ["source_column", "canonical_field", "confidence", "reasoning"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["mappings"],
    "additionalProperties": False,
}


def _call_llm(source_system: str, columns: list[str], samples: dict[str, list[str]]) -> tuple[list[dict], dict]:
    """One structured Claude call for all unresolved columns. Raises on any failure."""
    import anthropic  # imported lazily so the pipeline runs without the SDK installed

    client = anthropic.Anthropic(timeout=config.AI_MAPPER_TIMEOUT_S)
    started = time.perf_counter()
    response = client.messages.create(
        model=config.AI_MAPPER_MODEL,
        max_tokens=config.AI_MAPPER_MAX_TOKENS,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": _build_user_prompt(source_system, columns, samples)}],
        output_config={"format": {"type": "json_schema", "schema": _LLM_SCHEMA}},
    )
    latency_ms = int((time.perf_counter() - started) * 1000)

    # A refusal is a 200 with no usable content - treat it as an LLM failure so
    # the fallback tier takes over instead of the run dying.
    if getattr(response, "stop_reason", None) == "refusal":
        raise RuntimeError(f"model refused the request: {getattr(response, 'stop_details', None)}")

    text = next((b.text for b in response.content if b.type == "text"), None)
    if not text:
        raise RuntimeError("model returned no text block")

    payload = json.loads(text)
    meta = {
        "latency_ms": latency_ms,
        "input_tokens": getattr(response.usage, "input_tokens", 0),
        "output_tokens": getattr(response.usage, "output_tokens", 0),
    }
    return payload.get("mappings", []), meta


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def map_headers(
    source_system: str,
    columns: list[str],
    samples: dict[str, list[str]] | None = None,
    batch_date: str | date = "",
    use_llm: bool | None = None,
    strict_required: bool = True,
    overrides: dict[str, str | None] | None = None,
) -> MappingResult:
    """Resolve every incoming column to a canonical field, or to nothing."""
    samples = samples or {}
    overrides = load_overrides().get(source_system, {}) if overrides is None else overrides
    use_llm = config.AI_MAPPER_ENABLED if use_llm is None else use_llm
    batch_date = batch_date.isoformat() if isinstance(batch_date, date) else str(batch_date)

    decisions: dict[str, MappingDecision] = {}
    claimed: dict[str, MappingDecision] = {}   # canonical field -> winning decision

    def commit(column: str, canonical: str | None, method: str, confidence: float, why: str) -> None:
        """Record a decision, resolving collisions in favour of higher confidence."""
        if canonical and (incumbent := claimed.get(canonical)):
            if confidence <= incumbent.confidence:
                decisions[column] = MappingDecision(
                    column, None, METHOD_UNMAPPED, confidence,
                    f"{canonical} already claimed by '{incumbent.source_column}' "
                    f"at higher confidence ({incumbent.confidence:.2f})",
                )
                return
            # New claim wins; demote the incumbent.
            decisions[incumbent.source_column] = MappingDecision(
                incumbent.source_column, None, METHOD_UNMAPPED, incumbent.confidence,
                f"{canonical} reassigned to '{column}' at higher confidence ({confidence:.2f})",
            )
        decision = MappingDecision(column, canonical, method, round(confidence, 4), why)
        decisions[column] = decision
        if canonical:
            claimed[canonical] = decision

    # Tier 0 - human-recorded overrides
    unresolved: list[str] = []
    for col in columns:
        if col in overrides:
            commit(col, overrides[col], METHOD_OVERRIDE, 1.0,
                   "mapping recorded by a human in config/mapping_overrides.yml")
        else:
            unresolved.append(col)

    # Tier 1 - exact
    remaining_after_exact: list[str] = []
    for col in unresolved:
        if hit := exact_match(col):
            commit(col, hit, METHOD_EXACT, 1.0, f"normalized '{normalize(col)}' matches canonical '{hit}'")
        else:
            remaining_after_exact.append(col)
    unresolved = remaining_after_exact

    # Tier 2 - LLM
    llm_used = False
    llm_error: str | None = None
    meta = {"latency_ms": 0, "input_tokens": 0, "output_tokens": 0}
    invalid_proposals = 0
    low_confidence = 0

    if unresolved and use_llm:
        try:
            proposals, meta = _call_llm(source_system, unresolved, samples)
            llm_used = True
            by_column = {p.get("source_column"): p for p in proposals}
            still_unresolved: list[str] = []
            for col in unresolved:
                proposal = by_column.get(col)
                if not proposal:
                    still_unresolved.append(col)
                    continue
                canonical = proposal.get("canonical_field")
                confidence = float(proposal.get("confidence") or 0.0)
                why = str(proposal.get("reasoning", ""))[:300]
                if canonical is not None and canonical not in CANONICAL_NAMES:
                    # Hallucinated target: refuse it and let the fallback decide.
                    invalid_proposals += 1
                    log_event(log, "mapper.llm_invalid_field", source_system=source_system,
                              source_column=col, proposed=canonical)
                    still_unresolved.append(col)
                elif canonical is None:
                    commit(col, None, METHOD_LLM, confidence,
                           why or "model found no corresponding canonical field")
                elif confidence < config.LLM_MIN_CONFIDENCE:
                    low_confidence += 1
                    log_event(log, "mapper.llm_low_confidence", source_system=source_system,
                              source_column=col, proposed=canonical, confidence=confidence)
                    still_unresolved.append(col)
                else:
                    commit(col, canonical, METHOD_LLM, confidence, why)
            unresolved = still_unresolved
        except Exception as exc:   # network, auth, timeout, refusal, bad JSON - all fall back
            llm_error = f"{type(exc).__name__}: {exc}"
            log_event(log, "mapper.llm_failed", source_system=source_system, error=llm_error)

    # Tier 3 - fuzzy fallback, then give up
    for col in unresolved:
        canonical, score, why = fuzzy_match(col)
        if canonical:
            commit(col, canonical, METHOD_FUZZY, score, why)
        else:
            commit(col, None, METHOD_UNMAPPED, score, why)

    ordered = [decisions[c] for c in columns]
    mapped = [d for d in ordered if d.mapped]
    by_method = {m: sum(1 for d in ordered if d.method == m)
                 for m in (METHOD_OVERRIDE, METHOD_EXACT, METHOD_LLM, METHOD_FUZZY, METHOD_UNMAPPED)}
    auto_columns = [d for d in ordered if d.method != METHOD_OVERRIDE]
    auto_mapped = [d for d in auto_columns if d.mapped]
    missing_required = [f for f in REQUIRED_FIELDS if f not in claimed]

    result = MappingResult(
        source_system=source_system,
        batch_date=batch_date,
        decisions=ordered,
        llm_used=llm_used,
        llm_error=llm_error,
        llm_latency_ms=meta["latency_ms"],
        llm_input_tokens=meta["input_tokens"],
        llm_output_tokens=meta["output_tokens"],
        llm_invalid_proposals=invalid_proposals,
        llm_low_confidence=low_confidence,
        human_overrides=by_method[METHOD_OVERRIDE],
        model=config.AI_MAPPER_MODEL if llm_used else "",
        stats={
            "columns_total": len(ordered),
            "columns_mapped": len(mapped),
            "by_method": by_method,
            # Auto-match rate excludes human overrides from both numerator and
            # denominator: it measures what the pipeline resolved on its own.
            "auto_match_rate": round(len(auto_mapped) / len(auto_columns), 4) if auto_columns else 0.0,
            # Fallback rate = share of *mapped* columns the LLM did not resolve.
            "fallback_rate": round(by_method[METHOD_FUZZY] / len(mapped), 4) if mapped else 0.0,
            "mean_confidence": round(sum(d.confidence for d in mapped) / len(mapped), 4) if mapped else 0.0,
            "missing_required": missing_required,
        },
    )

    log_event(log, "mapper.complete", source_system=source_system, batch_date=batch_date,
              llm_used=llm_used, llm_error=llm_error, **result.stats)

    if missing_required and strict_required:
        raise ValueError(
            f"[{source_system}] required canonical fields left unmapped: {missing_required}. "
            "Refusing to load - fix the mapping or the source file."
        )
    return result


def write_decision_log(results: list[MappingResult], path: Path) -> Path:
    """Append every decision to a JSONL audit log (one row per column decision)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for result in results:
            for decision in result.decisions:
                fh.write(json.dumps({
                    "batch_date": result.batch_date,
                    "source_system": result.source_system,
                    "model": result.model,
                    "llm_used": result.llm_used,
                    "llm_error": result.llm_error,
                    **asdict(decision),
                }) + "\n")
    return path
