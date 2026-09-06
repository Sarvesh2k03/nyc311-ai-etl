-- Singular test: the daily aggregate must account for every fact row.
-- Guards against a GROUP BY that silently drops rows on a null grain column.

with fact_total as (
    select count(*) as n from {{ ref('fct_service_requests') }}
),

agg_total as (
    select sum(requests_opened) as n from {{ ref('agg_daily_borough_sla') }}
)

select
    fact_total.n as fact_rows,
    agg_total.n  as agg_rows
from fact_total
cross join agg_total
where fact_total.n != agg_total.n
