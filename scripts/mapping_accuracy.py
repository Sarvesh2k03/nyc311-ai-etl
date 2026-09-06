"""Measure schema-mapping accuracy against the known ground truth.

The dialect definitions record what each header *should* map to, so mapping
quality is measurable rather than eyeballed. Run this to compare the fallback-only
baseline against a run with the LLM tier enabled:

    .venv/bin/python scripts/mapping_accuracy.py            # fallback only
    .venv/bin/python scripts/mapping_accuracy.py --use-llm  # needs ANTHROPIC_API_KEY
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.ai_schema_mapper import METHOD_OVERRIDE, map_headers
from pipeline.source_dialects import DIALECTS, expected_mapping


def main(use_llm: bool) -> int:
    totals = {"columns": 0, "should_map": 0, "correct_mapped": 0, "mis_mapped": 0,
              "missed": 0, "distractors": 0, "distractors_declined": 0, "overrides": 0}

    print(f"{'source system':16s} {'cols':>5s} {'correct':>8s} {'mis-mapped':>11s} {'missed':>7s}")
    for dialect in DIALECTS:
        truth = expected_mapping(dialect)
        result = map_headers(dialect.name, list(truth), use_llm=use_llm, strict_required=False)

        correct = mis = missed = declined = 0
        for decision in result.decisions:
            want = truth[decision.source_column]
            if decision.method == METHOD_OVERRIDE:
                totals["overrides"] += 1
            if want is None:
                totals["distractors"] += 1
                if not decision.mapped:
                    declined += 1
            else:
                totals["should_map"] += 1
                if decision.canonical_field == want:
                    correct += 1
                elif decision.mapped:
                    mis += 1
                else:
                    missed += 1

        totals["columns"] += len(truth)
        totals["correct_mapped"] += correct
        totals["mis_mapped"] += mis
        totals["missed"] += missed
        totals["distractors_declined"] += declined
        print(f"{dialect.name:16s} {len(truth):5d} {correct:8d} {mis:11d} {missed:7d}")

    should = totals["should_map"]
    mapped = totals["correct_mapped"] + totals["mis_mapped"]
    print("\n--- totals ---")
    print(f"columns evaluated          : {totals['columns']}")
    print(f"columns that should map    : {should}")
    print(f"operational/audit columns  : {totals['distractors']} "
          f"(correctly declined: {totals['distractors_declined']})")
    print(f"human overrides applied    : {totals['overrides']}")
    print(f"recall  (correct / should) : {totals['correct_mapped']}/{should} = "
          f"{totals['correct_mapped'] / should:.1%}")
    print(f"precision (correct/mapped) : {totals['correct_mapped']}/{mapped} = "
          f"{totals['correct_mapped'] / mapped:.1%}" if mapped else "precision: n/a")
    print(f"mis-mapped columns         : {totals['mis_mapped']}")
    print(f"LLM tier enabled           : {use_llm}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--use-llm", action="store_true")
    sys.exit(main(parser.parse_args().use_llm))
