{#-
  Custom generic test: a numeric column must sit inside a closed interval.
  Nulls pass - "unknown" is a legitimate value here, "impossible" is not.
-#}
{% test within_range(model, column_name, min_value, max_value) %}

select
    {{ column_name }} as offending_value
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})

{% endtest %}
