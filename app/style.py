"""Shared page styling and small presentation helpers.

Kept in one module so the demo page and the explanation page look like the same
product rather than two apps that happen to share a repository.
"""
from __future__ import annotations

import streamlit as st

PAGE_STYLE = """
<style>
  .block-container { padding-top: 3.2rem; max-width: 1280px; }
  #MainMenu, footer { visibility: hidden; }

  .hero-eyebrow {
    font-size: .74rem; font-weight: 700; letter-spacing: .12em;
    text-transform: uppercase; color: #8a8880; margin-bottom: .4rem;
  }
  .hero-title {
    font-size: 2.1rem; font-weight: 700; line-height: 1.15;
    color: #0b0b0b; margin: 0 0 .55rem 0; letter-spacing: -.02em;
  }
  .hero-sub { font-size: .98rem; line-height: 1.55; color: #52514e; max-width: 56rem; }

  .chips { display: flex; flex-wrap: wrap; gap: .4rem; margin: .2rem 0 .4rem 0; }
  .chip {
    font-size: .76rem; font-weight: 600; padding: .22rem .6rem; border-radius: 999px;
    background: #f1f1ee; color: #52514e; border: 1px solid #e2e1db;
  }
  .chip-accent { background: #eaf2fd; color: #1c5ba8; border-color: #cfe1f8; }

  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(168px, 1fr)); gap: .7rem; }
  /* Result cards sit inside a half-width column, so they need a smaller floor
     to stay on one row instead of wrapping a lone card onto its own line. */
  .cards-tight { grid-template-columns: repeat(auto-fit, minmax(112px, 1fr)); gap: .55rem; }
  .card { background: #ffffff; border: 1px solid #e7e7e2; border-radius: 10px; padding: .85rem .95rem; }
  .card-label {
    font-size: .7rem; font-weight: 700; letter-spacing: .07em; text-transform: uppercase;
    color: #8a8880; margin-bottom: .3rem;
  }
  .card-value { font-size: 1.7rem; font-weight: 700; color: #0b0b0b; line-height: 1.05; }
  .card-value-sm { font-size: 1.02rem; font-weight: 700; color: #0b0b0b; line-height: 1.3; }
  .card-sub { font-size: .76rem; color: #8a8880; margin-top: .25rem; }
  .card-accent { border-left: 3px solid #2a78d6; }
  .card-good { border-left: 3px solid #0ca30c; }
  .card-warn { border-left: 3px solid #fab219; }

  .note {
    border-left: 3px solid #2a78d6; background: #f7fafe; border-radius: 0 8px 8px 0;
    padding: .75rem .95rem; font-size: .89rem; color: #33322f; line-height: 1.55;
  }
  .note-warn { border-left-color: #fab219; background: #fefaf0; }
  .note-good { border-left-color: #0ca30c; background: #f4fbf4; }

  /* One-line verdict strip, used instead of a row of cards when the result is a
     single fact rather than a set of measurements. */
  .verdict {
    display: flex; align-items: baseline; gap: .55rem; flex-wrap: wrap;
    border-left: 3px solid #0ca30c; background: #f4fbf4; border-radius: 0 8px 8px 0;
    padding: .8rem 1rem; font-size: 1.15rem; font-weight: 700; color: #0b0b0b;
  }
  .verdict-warn { border-left-color: #fab219; background: #fefaf0; }
  .verdict-arrow { font-size: 1rem; color: #8a8880; font-weight: 400; }
  .verdict code { font-size: 1.1rem; background: transparent; padding: 0; color: #1c5ba8; }
  .verdict-meta { font-size: .8rem; font-weight: 500; color: #8a8880; }

  .rule { border: 0; border-top: 1px solid #e7e7e2; margin: 2.2rem 0 1.6rem 0; }

  .svg-wrap {
    border: 1px solid #e7e7e2; border-radius: 10px; background: #fcfcfb;
    padding: .5rem; overflow-x: auto;
  }
  h3 { font-size: 1.06rem !important; font-weight: 700 !important; margin-top: .2rem !important; }
  .stTabs [data-baseweb="tab"] { font-size: .93rem; font-weight: 600; }
</style>
"""


def cards(items: list[tuple[str, str, str, str]], tight: bool = False) -> None:
    """Render a responsive row of metric cards: (label, value, sub, modifier).

    A modifier containing "small" renders the value at text size instead of
    display size, for cards whose value is a phrase rather than a number.
    `tight` narrows the grid for cards placed inside a column.
    """
    html = "".join(
        f'<div class="card {mod}"><div class="card-label">{label}</div>'
        f'<div class="{"card-value-sm" if "small" in mod else "card-value"}">{value}</div>'
        f'<div class="card-sub">{sub}</div></div>'
        for label, value, sub, mod in items
    )
    css = "cards cards-tight" if tight else "cards"
    st.markdown(f'<div class="{css}">{html}</div>', unsafe_allow_html=True)


def note(text: str, kind: str = "") -> None:
    st.markdown(f'<div class="note {kind}">{text}</div>', unsafe_allow_html=True)


def page_link(target: str, label: str, icon: str = "") -> None:
    """Render a link to a sibling page, tolerating a standalone run.

    st.page_link resolves paths relative to the app's entrypoint, so it raises
    when a page in pages/ is launched directly (`streamlit run
    app/pages/1_How_it_works.py`). The page itself is perfectly usable that way,
    so a missing sibling link should not take it down.
    """
    try:
        st.page_link(target, label=label, icon=icon or None)
    except Exception:
        st.caption(f"{label} — run `streamlit run app/streamlit_app.py` for full navigation.")


def rule() -> None:
    """A hairline section divider - quieter than a heading, enough to breathe."""
    st.markdown('<hr class="rule"/>', unsafe_allow_html=True)


def gap(rem: float = 1.0) -> None:
    st.markdown(f'<div style="height:{rem}rem"></div>', unsafe_allow_html=True)
