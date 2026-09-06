{{ config(severity='warn', warn_if='>0') }}

-- Observability test, warn-only: surfaces how many requests carry impossible
-- timestamps. These are quarantined by fct_service_requests rather than dropped,
-- so this does not block the run - it keeps the rate visible in every dbt
-- invocation and in the quality report, and it would turn into a failure if the
-- rate ever became material.

select
    count(*) as anomalous_requests
from {{ ref('fct_service_requests') }}
where has_invalid_timestamps
having count(*) > 0
