{#-
  Portability shims. The pipeline runs on DuckDB locally and BigQuery in the
  cloud; these are the only three places the dialects differ, so every model
  below is written once and runs on both.
-#}

{% macro safe_cast_timestamp(column) -%}
    {%- if target.type == 'bigquery' -%}
        safe_cast(nullif(trim({{ column }}), '') as timestamp)
    {%- else -%}
        try_cast(nullif(trim({{ column }}), '') as timestamp)
    {%- endif -%}
{%- endmacro %}

{% macro safe_cast_float(column) -%}
    {%- if target.type == 'bigquery' -%}
        safe_cast(nullif(trim({{ column }}), '') as float64)
    {%- else -%}
        try_cast(nullif(trim({{ column }}), '') as double)
    {%- endif -%}
{%- endmacro %}

{% macro hours_between(start_ts, end_ts) -%}
    {%- if target.type == 'bigquery' -%}
        timestamp_diff({{ end_ts }}, {{ start_ts }}, second) / 3600.0
    {%- else -%}
        date_diff('second', {{ start_ts }}, {{ end_ts }}) / 3600.0
    {%- endif -%}
{%- endmacro %}

{% macro median_of(column) -%}
    {%- if target.type == 'bigquery' -%}
        approx_quantiles({{ column }}, 2)[offset(1)]
    {%- else -%}
        median({{ column }})
    {%- endif -%}
{%- endmacro %}
