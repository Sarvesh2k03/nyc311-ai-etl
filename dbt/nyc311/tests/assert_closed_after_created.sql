-- Singular test: every timestamp anomaly must be flagged AND have its derived
-- duration quarantined.
--
-- The upstream feed genuinely contains requests closed before they were created,
-- so asserting that none exist would just fail forever. What must hold is that
-- the pipeline never lets one through unlabelled or lets it produce a negative
-- resolution time - which is also what a swapped created_at / closed_at mapping
-- would look like, so this test guards the AI mapper as well as the source data.

select
    request_id,
    created_at,
    closed_at,
    resolution_hours,
    has_invalid_timestamps
from {{ ref('fct_service_requests') }}
where closed_at is not null
  and closed_at < created_at
  and (has_invalid_timestamps = false or resolution_hours is not null)
