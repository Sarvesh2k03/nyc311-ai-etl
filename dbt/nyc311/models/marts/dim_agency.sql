{{ config(materialized='table') }}

-- Agency dimension. Names vary across upstream systems for the same acronym,
-- so the most frequently seen spelling wins.

with requests as (

    select * from {{ ref('int_service_requests_deduped') }}

),

name_frequency as (

    select
        agency_code,
        agency_name,
        count(*) as name_count,
        row_number() over (
            partition by agency_code order by count(*) desc, agency_name
        ) as name_rank
    from requests
    where agency_name is not null
    group by agency_code, agency_name

),

totals as (

    select
        agency_code,
        count(*)                                        as total_requests,
        count(closed_at)                                as closed_requests,
        min(created_at)                                 as first_seen_at,
        max(created_at)                                 as last_seen_at
    from requests
    group by agency_code

)

select
    totals.agency_code,
    coalesce(name_frequency.agency_name, 'UNKNOWN')     as agency_name,
    totals.total_requests,
    totals.closed_requests,
    round(totals.closed_requests * 1.0 / nullif(totals.total_requests, 0), 4) as closure_rate,
    totals.first_seen_at,
    totals.last_seen_at
from totals
left join name_frequency
    on totals.agency_code = name_frequency.agency_code
    and name_frequency.name_rank = 1
