"""Hand-built architecture SVG.

Inline SVG rather than a rendered image or a Mermaid block: it stays sharp at any
width, needs no diagramming runtime, and the labels are real text - so they are
selectable and searchable on the page.
"""
from __future__ import annotations

INK = "#0b0b0b"
INK_2 = "#52514e"
INK_3 = "#8a8880"
BLUE = "#2a78d6"
BLUE_TINT = "#eaf2fd"
AQUA = "#1baf7a"
AQUA_TINT = "#e7f7f1"
ORANGE = "#eb6834"
ORANGE_TINT = "#fdeee8"
LINE = "#d6d5ce"
SURFACE = "#fcfcfb"
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"

TASKS = [
    ("extract", "Socrata API &#8594; 4 raw files"),
    ("ai_schema_map_and_load", "resolve headers &#8594; warehouse"),
    ("dbt_run", "staging &#8594; marts"),
    ("dbt_test", "44 data tests"),
    ("quality_report", "metrics + gate"),
]

TIERS = [
    ("0 &#160;override", "human-recorded mapping", "#f4f4f1", INK_2),
    ("1 &#160;exact", "normalized name or synonym", BLUE_TINT, BLUE),
    ("2 &#160;LLM", "Claude, structured JSON", AQUA_TINT, AQUA),
    ("3 &#160;fuzzy", "rapidfuzz &#8212; always available", ORANGE_TINT, ORANGE),
]

LAYERS = [
    ("raw", "service_requests_raw", "landing, all strings"),
    ("staging", "stg_ &#183; int_", "typed, deduped"),
    ("analytics", "fct_ &#183; dim_ &#183; agg_", "tested marts"),
]


def _task_box(x: float, y: float, w: float, h: float, name: str, sub: str, index: int) -> str:
    accent = AQUA if index == 1 else BLUE
    fill = AQUA_TINT if index == 1 else "#ffffff"
    return f"""
  <g>
    <rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8"
          fill="{fill}" stroke="{accent}" stroke-width="{1.5 if index == 1 else 1}"/>
    <rect x="{x}" y="{y}" width="3.5" height="{h}" rx="1.75" fill="{accent}"/>
    <text x="{x + 14}" y="{y + 24}" font-family="{FONT}" font-size="12.5"
          font-weight="600" fill="{INK}">{name}</text>
    <text x="{x + 14}" y="{y + 42}" font-family="{FONT}" font-size="11"
          fill="{INK_3}">{sub}</text>
  </g>"""


def architecture_svg() -> str:
    """Three bands: the DAG, the mapper's tiers, the warehouse layers."""
    parts: list[str] = [
        f'<svg viewBox="0 0 1180 470" width="100%" role="img" '
        f'aria-label="Pipeline architecture: Airflow DAG, schema mapper tiers, warehouse layers" '
        f'xmlns="http://www.w3.org/2000/svg" style="display:block">',
        f'<rect width="1180" height="470" fill="{SURFACE}"/>',
        '<defs>'
        f'<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" '
        f'markerHeight="6" orient="auto"><path d="M0,1 L9,5 L0,9" fill="none" '
        f'stroke="{LINE}" stroke-width="1.6"/></marker>'
        f'<marker id="arrow-orange" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" '
        f'markerHeight="6" orient="auto"><path d="M0,1 L9,5 L0,9" fill="none" '
        f'stroke="{ORANGE}" stroke-width="1.6"/></marker>'
        '</defs>',
    ]

    # ---- Band 1: the Airflow DAG -----------------------------------------
    parts.append(
        f'<text x="24" y="30" font-family="{FONT}" font-size="11.5" font-weight="600" '
        f'letter-spacing="0.6" fill="{INK_3}">AIRFLOW DAG &#183; nyc311_ai_etl &#183; '
        f'@daily &#183; catchup &#183; retries with exponential backoff</text>'
    )

    src_x, src_y, src_w, src_h = 24, 48, 150, 62
    parts.append(f"""
  <g>
    <rect x="{src_x}" y="{src_y}" width="{src_w}" height="{src_h}" rx="8"
          fill="#ffffff" stroke="{LINE}" stroke-width="1" stroke-dasharray="4 3"/>
    <text x="{src_x + 14}" y="{src_y + 25}" font-family="{FONT}" font-size="12.5"
          font-weight="600" fill="{INK}">NYC 311 Open Data</text>
    <text x="{src_x + 14}" y="{src_y + 43}" font-family="{FONT}" font-size="11"
          fill="{INK_3}">Socrata API &#183; ~9.6k rows/day</text>
  </g>""")

    box_w, box_h, gap = 178, 62, 20
    start_x = src_x + src_w + 34
    for i, (name, sub) in enumerate(TASKS):
        x = start_x + i * (box_w + gap)
        parts.append(_task_box(x, src_y, box_w, box_h, name, sub, i))
        prev_right = src_x + src_w if i == 0 else x - gap
        parts.append(
            f'<line x1="{prev_right + 4}" y1="{src_y + box_h / 2}" x2="{x - 5}" '
            f'y2="{src_y + box_h / 2}" stroke="{LINE}" stroke-width="1.4" '
            f'marker-end="url(#arrow)"/>'
        )

    # ---- Band 2: the mapper's tiers --------------------------------------
    panel_x, panel_y, panel_w, panel_h = start_x + box_w + gap, 150, 430, 196
    parts.append(
        f'<line x1="{panel_x + 90}" y1="{src_y + box_h}" x2="{panel_x + 90}" y2="{panel_y}" '
        f'stroke="{AQUA}" stroke-width="1.2" stroke-dasharray="3 3"/>'
    )
    parts.append(
        f'<rect x="{panel_x}" y="{panel_y}" width="{panel_w}" height="{panel_h}" rx="10" '
        f'fill="#ffffff" stroke="{LINE}" stroke-width="1"/>'
    )
    parts.append(
        f'<text x="{panel_x + 18}" y="{panel_y + 26}" font-family="{FONT}" font-size="11.5" '
        f'font-weight="600" letter-spacing="0.5" fill="{INK_3}">'
        f'INSIDE ai_schema_map_and_load &#8212; first tier to commit, wins</text>'
    )

    row_y = panel_y + 42
    for label, sub, fill, accent in TIERS:
        parts.append(f"""
    <g>
      <rect x="{panel_x + 18}" y="{row_y}" width="{panel_w - 36}" height="30" rx="6"
            fill="{fill}" stroke="{accent}" stroke-width="0.8"/>
      <text x="{panel_x + 30}" y="{row_y + 20}" font-family="{FONT}" font-size="11.5"
            font-weight="600" fill="{INK}">{label}</text>
      <text x="{panel_x + 148}" y="{row_y + 20}" font-family="{FONT}" font-size="11"
            fill="{INK_2}">{sub}</text>
    </g>""")
        row_y += 36

    llm_y = panel_y + 42 + 2 * 36 + 15
    fuzzy_y = panel_y + 42 + 3 * 36 + 15
    parts.append(
        f'<path d="M {panel_x + panel_w - 8} {llm_y} C {panel_x + panel_w + 26} {llm_y}, '
        f'{panel_x + panel_w + 26} {fuzzy_y}, {panel_x + panel_w - 8} {fuzzy_y}" '
        f'fill="none" stroke="{ORANGE}" stroke-width="1.4" stroke-dasharray="4 3" '
        f'marker-end="url(#arrow-orange)"/>'
    )
    parts.append(
        f'<text x="{panel_x + panel_w + 34}" y="{(llm_y + fuzzy_y) / 2 - 6}" '
        f'font-family="{FONT}" font-size="10.5" font-weight="600" fill="{ORANGE}">fallback on</text>'
        f'<text x="{panel_x + panel_w + 34}" y="{(llm_y + fuzzy_y) / 2 + 8}" '
        f'font-family="{FONT}" font-size="10.5" fill="{INK_2}">error &#183; timeout &#183; refusal</text>'
        f'<text x="{panel_x + panel_w + 34}" y="{(llm_y + fuzzy_y) / 2 + 22}" '
        f'font-family="{FONT}" font-size="10.5" fill="{INK_2}">bad JSON &#183; low confidence</text>'
    )

    # ---- Band 3: warehouse layers ----------------------------------------
    wh_y = 380
    parts.append(
        f'<text x="24" y="{wh_y - 14}" font-family="{FONT}" font-size="11.5" font-weight="600" '
        f'letter-spacing="0.6" fill="{INK_3}">WAREHOUSE &#183; DuckDB locally, '
        f'BigQuery sandbox in the cloud &#183; one env var switches both loader and dbt</text>'
    )
    lw, lgap = 246, 22
    for i, (schema, models, sub) in enumerate(LAYERS):
        x = 24 + i * (lw + lgap)
        parts.append(f"""
    <g>
      <rect x="{x}" y="{wh_y}" width="{lw}" height="62" rx="8" fill="#ffffff"
            stroke="{BLUE}" stroke-width="1"/>
      <text x="{x + 14}" y="{wh_y + 24}" font-family="{FONT}" font-size="12.5"
            font-weight="600" fill="{BLUE}">{schema}</text>
      <text x="{x + 14}" y="{wh_y + 42}" font-family="{FONT}" font-size="11"
            fill="{INK_2}">{models} &#183; {sub}</text>
    </g>""")
        if i:
            parts.append(
                f'<line x1="{x - lgap + 2}" y1="{wh_y + 31}" x2="{x - 5}" y2="{wh_y + 31}" '
                f'stroke="{LINE}" stroke-width="1.4" marker-end="url(#arrow)"/>'
            )

    # Tail arrow from the analytics layer out to the dashboard itself.
    tail_x = 24 + 2 * (lw + lgap) + lw + 12
    parts.append(
        f'<line x1="{tail_x}" y1="{wh_y + 31}" x2="{tail_x + 78}" y2="{wh_y + 31}" '
        f'stroke="{LINE}" stroke-width="1.4" marker-end="url(#arrow)"/>'
        f'<text x="{tail_x + 90}" y="{wh_y + 28}" font-family="{FONT}" font-size="11.5" '
        f'font-weight="600" fill="{INK_2}">this dashboard</text>'
    )

    parts.append("</svg>")
    return "".join(parts)
