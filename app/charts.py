"""Altair chart builders.

One place for every chart so the visual language stays consistent: the same
palette, the same recessive chrome, the same hover behaviour.

Palette is a validated categorical set (slots assigned in fixed order, never
cycled) plus a reserved status palette that is never reused for a series. Three
of the light-mode slots sit below 3:1 contrast on the chart surface, so every
chart here ships either direct value labels or an accompanying table view.
"""
from __future__ import annotations

import altair as alt
import pandas as pd

# Categorical slots, assigned in fixed order.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
PRIMARY = SERIES[0]

# Status palette - reserved, never used for a data series.
STATUS = {"good": "#0ca30c", "warning": "#fab219", "serious": "#ec835a", "critical": "#d03b3b"}

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#8a8880"
GRID = "#e7e7e2"

FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"

# Format quantitative tick labels explicitly. Neither a per-axis `format` nor a
# global `numberFormat` survived the Vega runtime here - counts still rendered as
# "1e+4" - and an axis a reader has to decode in scientific notation is a broken
# axis. A label expression is evaluated by the renderer itself, so it holds.
THOUSANDS = "format(datum.value, ',')"


def _base(chart: alt.Chart, height: int) -> alt.Chart:
    """Recessive chrome: no chart border, hairline grid, quiet axes.

    `numberFormat` is set globally because a per-axis `format` alone still let
    the renderer fall back to exponent notation ("1e+4") on larger counts, which
    is unreadable in a chart a non-engineer is meant to skim.
    """
    return (
        chart.properties(height=height, background=SURFACE)
        .configure(numberFormat=",")
        .configure_view(stroke=None)
        .configure_axis(
            labelFont=FONT, titleFont=FONT, labelColor=INK_SECONDARY, titleColor=INK_MUTED,
            labelFontSize=11, titleFontSize=11, titleFontWeight="normal",
            gridColor=GRID, gridWidth=1, domain=False, tickSize=0, labelPadding=6,
        )
        .configure_legend(
            labelFont=FONT, titleFont=FONT, labelColor=INK, titleColor=INK_MUTED,
            labelFontSize=11, titleFontSize=11, titleFontWeight="normal",
            symbolType="circle", symbolSize=90, orient="top", direction="horizontal",
            columns=6, offset=6,
        )
        .configure_text(font=FONT)
    )


def daily_volume(frame: pd.DataFrame, boroughs: list[str]) -> alt.Chart:
    """Requests opened per day, one line per borough."""
    data = frame[frame["borough"].isin(boroughs)]
    hover = alt.selection_point(on="mouseover", nearest=True, fields=["created_date"], empty=False)

    line = alt.Chart(data).mark_line(strokeWidth=2, interpolate="monotone").encode(
        x=alt.X("created_date:T", title=None, axis=alt.Axis(format="%b %d", grid=False)),
        y=alt.Y("requests_opened:Q", title="requests opened",
                scale=alt.Scale(nice=True), axis=alt.Axis(grid=True, labelExpr=THOUSANDS)),
        # The legend has to be declared on this layer explicitly: the transparent
        # hover layer below shares the color scale, and a bare shared resolution
        # lets its legend=None win, which drops the legend from the whole chart.
        color=alt.Color(
            "borough:N", title=None,
            scale=alt.Scale(domain=sorted(boroughs), range=SERIES[:len(boroughs)]),
            legend=alt.Legend(orient="top", direction="horizontal", title=None,
                              symbolType="stroke", symbolStrokeWidth=3, columns=6),
        ),
    )
    points = alt.Chart(data).mark_circle(size=90, opacity=0).encode(
        x="created_date:T", y="requests_opened:Q", color=alt.Color("borough:N", legend=None),
        tooltip=[
            alt.Tooltip("created_date:T", title="date", format="%a %b %d, %Y"),
            alt.Tooltip("borough:N", title="borough"),
            alt.Tooltip("requests_opened:Q", title="opened", format=","),
            alt.Tooltip("requests_closed:Q", title="closed", format=","),
        ],
    ).add_params(hover)
    rule = alt.Chart(data).mark_rule(color=INK_MUTED, strokeWidth=1).encode(
        x="created_date:T"
    ).transform_filter(hover)

    return _base((rule + line + points).resolve_scale(color="shared"), 320)


def ranked_bars(
    frame: pd.DataFrame, value: str, label: str, value_title: str,
    height: int = 320, color: str = PRIMARY, fmt: str = ",",
) -> alt.Chart:
    """Horizontal bars, single hue, with a direct value label on every bar.

    Both layers are built from one shared base encoding. Declaring the
    positional channels twice lets the layers disagree about the axis - the text
    layer wins the merge and silently drops the number format - and declaring
    `axis=None` on it removes the axis from the whole chart.
    """
    base = alt.Chart(frame).encode(
        y=alt.Y(f"{label}:N", sort="-x", title=None,
                axis=alt.Axis(grid=False, labelLimit=220)),
        x=alt.X(f"{value}:Q", title=value_title,
                axis=alt.Axis(grid=True, format=fmt, labelExpr=THOUSANDS)),
    )
    bars = base.mark_bar(cornerRadiusEnd=4, height=14, color=color).encode(
        tooltip=[alt.Tooltip(f"{label}:N", title=label),
                 alt.Tooltip(f"{value}:Q", title=value_title, format=fmt)],
    )
    labels = base.mark_text(
        align="left", dx=6, fontSize=11, color=INK_SECONDARY,
    ).encode(text=alt.Text(f"{value}:Q", format=fmt))
    return _base(bars + labels, height)


def tier_bars(counts: dict[str, int]) -> alt.Chart:
    """Mapping decisions by resolution tier.

    'unmapped' is drawn in muted ink rather than the series hue: it is the
    absence of a decision, not another category of one.
    """
    order = ["exact", "llm", "fuzzy", "override", "unmapped"]
    frame = pd.DataFrame([
        {"tier": tier, "columns": counts.get(tier, 0),
         "kind": "declined" if tier == "unmapped" else "resolved"}
        for tier in order
    ])
    base = alt.Chart(frame).encode(
        y=alt.Y("tier:N", sort=order, title=None, axis=alt.Axis(grid=False)),
        x=alt.X("columns:Q", title="column decisions",
                axis=alt.Axis(grid=True, labelExpr=THOUSANDS)),
    )
    bars = base.mark_bar(cornerRadiusEnd=4, height=22).encode(
        color=alt.Color("kind:N", title=None,
                        scale=alt.Scale(domain=["resolved", "declined"],
                                        range=[PRIMARY, "#c9c8c1"])),
        tooltip=[alt.Tooltip("tier:N", title="tier"),
                 alt.Tooltip("columns:Q", title="columns")],
    )
    labels = base.mark_text(
        align="left", dx=6, fontSize=11, color=INK_SECONDARY,
    ).encode(text="columns:Q")
    return _base(bars + labels, 220)


def confidence_bars(frame: pd.DataFrame) -> alt.Chart:
    """Distribution of mapping confidence for columns that resolved."""
    base = alt.Chart(frame).encode(
        x=alt.X("bucket:N", sort=list(frame["bucket"]), title=None,
                axis=alt.Axis(grid=False, labelAngle=0)),
        y=alt.Y("columns:Q", title="columns", axis=alt.Axis(grid=True, labelExpr=THOUSANDS)),
    )
    bars = base.mark_bar(cornerRadiusEnd=4, size=34, color=PRIMARY).encode(
        tooltip=[alt.Tooltip("bucket:N", title="confidence"),
                 alt.Tooltip("columns:Q", title="columns")],
    )
    labels = base.mark_text(dy=-8, fontSize=11, color=INK_SECONDARY).encode(text="columns:Q")
    return _base(bars + labels, 260)


def dbt_test_bars(frame: pd.DataFrame) -> alt.Chart:
    """dbt tests by type, split by outcome using the reserved status palette."""
    grouped = (frame.groupby(["type", "status"]).size()
               .reset_index(name="tests").sort_values("tests", ascending=False))
    bars = alt.Chart(grouped).mark_bar(cornerRadiusEnd=4, height=16, stroke=SURFACE,
                                       strokeWidth=2).encode(
        y=alt.Y("type:N", sort="-x", title=None, axis=alt.Axis(grid=False)),
        x=alt.X("tests:Q", title="tests", stack=True,
                axis=alt.Axis(grid=True, labelExpr=THOUSANDS)),
        color=alt.Color("status:N", title=None,
                        scale=alt.Scale(domain=["pass", "warn", "fail", "error"],
                                        range=[STATUS["good"], STATUS["warning"],
                                               STATUS["critical"], STATUS["critical"]])),
        tooltip=[alt.Tooltip("type:N", title="test type"),
                 alt.Tooltip("status:N", title="outcome"),
                 alt.Tooltip("tests:Q", title="tests")],
    )
    return _base(bars, 260)


def stage_funnel(frame: pd.DataFrame) -> alt.Chart:
    """Row counts at each pipeline stage - equal bars mean nothing was lost."""
    base = alt.Chart(frame).encode(
        y=alt.Y("stage:N", sort=list(frame["stage"]), title=None, axis=alt.Axis(grid=False)),
        x=alt.X("rows:Q", title="rows",
                axis=alt.Axis(grid=True, format=",", labelExpr=THOUSANDS)),
    )
    bars = base.mark_bar(cornerRadiusEnd=4, height=26, color=PRIMARY).encode(
        tooltip=[alt.Tooltip("stage:N", title="stage"),
                 alt.Tooltip("relation:N", title="relation"),
                 alt.Tooltip("rows:Q", title="rows", format=",")],
    )
    labels = base.mark_text(
        align="left", dx=6, fontSize=11, color=INK_SECONDARY,
    ).encode(text=alt.Text("rows:Q", format=","))
    return _base(bars + labels, 220)
