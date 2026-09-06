"""Smoke tests for the dashboard.

Streamlit executes the whole script on every run, so a single AppTest pass
exercises every tab - including the chart builders, which fail loudly on a bad
column reference or an empty frame. This is what catches a dashboard broken by a
schema change in the pipeline, without anyone having to click through it.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from streamlit.testing.v1 import AppTest

APP = Path(__file__).resolve().parents[1] / "app" / "streamlit_app.py"
HOW_IT_WORKS = Path(__file__).resolve().parents[1] / "app" / "pages" / "1_How_it_works.py"
DEMO_DIR = Path(__file__).resolve().parents[1] / "data" / "demo"

pytestmark = pytest.mark.skipif(
    not (DEMO_DIR / "manifest.json").exists(),
    reason="demo snapshot missing - run scripts/export_demo_data.py",
)


@pytest.fixture(scope="module")
def app() -> AppTest:
    at = AppTest.from_file(str(APP), default_timeout=90)
    at.run()
    return at


def test_app_runs_without_exceptions(app: AppTest):
    assert not app.exception, [str(e) for e in app.exception]


def test_every_tab_renders(app: AppTest):
    labels = {label for tab in app.tabs for label in ([tab.label] if tab.label else [])}
    for expected in ["Data", "Quality", "Mapping"]:
        assert expected in labels, f"missing tab: {expected} (found {labels})"


def test_landing_page_leads_with_the_demo_not_an_explainer(app: AppTest):
    """The architecture and the design rationale belong on the second page.

    This is a product decision worth pinning: the landing page is the working
    demo, so a future edit that moves the diagram back onto it should fail here.
    """
    body = " ".join(block.value for block in app.markdown)
    assert "A data pipeline you can run right now" in body
    assert "<svg" not in body, "architecture diagram must live on the How it works page"
    assert "Why it is built this way" not in body


def test_landing_page_offers_a_live_run(app: AppTest):
    """The button that runs the real pipeline is the point of the landing page."""
    labels = [b.label for b in app.button]
    assert any("Run the pipeline now" in label for label in labels), labels


def test_landing_page_stays_uncluttered(app: AppTest):
    """A recruiter should meet a page, not a dashboard.

    The landing page earns its density by being interactive; caps keep a future
    edit from turning it back into a wall. Detail belongs in expanders."""
    assert len(app.tabs) <= 3, f"{len(app.tabs)} tabs on the landing page"
    assert len(app.get("vega_lite_chart")) <= 5, "too many charts competing at once"


def test_how_it_works_page_runs(app: AppTest):
    page = AppTest.from_file(str(HOW_IT_WORKS), default_timeout=90)
    page.run()
    assert not page.exception, [str(e) for e in page.exception]
    body = " ".join(block.value for block in page.markdown)
    assert "<svg" in body, "the architecture diagram should render here"


def test_headline_numbers_are_present(app: AppTest):
    """The hero must show real loaded numbers, never placeholders."""
    body = " ".join(block.value for block in app.markdown)
    assert "133,993" in body, "row count should come from the snapshot, not a placeholder"


def test_charts_are_built(app: AppTest):
    """Every chart builder ran; a broken spec raises before this point."""
    charts = app.get("vega_lite_chart")
    assert len(charts) >= 3, f"expected the reference-run charts, rendered {len(charts)}"


def test_mapper_demo_resolves_a_known_header(app: AppTest):
    """The live mapper widget answers with the fallback tier, no API key needed."""
    app.text_input(key="mapper_input").set_value("ZIP_CD").run()
    body = " ".join(block.value for block in app.markdown)
    assert "postal_code" in body
    assert not app.exception


def test_mapper_demo_declines_an_audit_column(app: AppTest):
    app.text_input(key="mapper_input").set_value("ROW_CHECKSUM").run()
    body = " ".join(block.value for block in app.markdown)
    assert "declined" in body
    assert not app.exception
