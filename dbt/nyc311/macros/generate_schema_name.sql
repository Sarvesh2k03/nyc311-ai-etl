{#-
  Use the schema configured on the model verbatim (staging, analytics) instead
  of dbt's default <target_schema>_<custom_schema> concatenation. Keeps the
  warehouse laid out as raw / staging / analytics on both DuckDB and BigQuery.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
