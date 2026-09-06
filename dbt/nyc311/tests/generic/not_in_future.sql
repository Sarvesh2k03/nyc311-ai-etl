{#-
  Custom generic test: a timestamp column must not be in the future.

  311 requests cannot be created or closed after now. A future timestamp means
  either an upstream data-entry error or - more interestingly for this pipeline
  - a column that the schema mapper pointed at the wrong field. This is the test
  that would catch a bad AI mapping before anyone trusted the numbers.

  `grace_hours` allows for clock skew between the source system and the
  warehouse without letting a genuinely wrong year through.
-#}
{% test not_in_future(model, column_name, grace_hours=24) %}

select
    {{ column_name }} as offending_value
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} > {{ dbt.current_timestamp() }} + interval '{{ grace_hours }} hour'

{% endtest %}
