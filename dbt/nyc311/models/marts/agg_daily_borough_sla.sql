{{ config(materialized='table') }}

-- Analytics-ready mart: daily resolution performance by borough and agency.
-- This is the table a BI tool or an analyst would actually query.

with requests as (

    select * from {{ ref('fct_service_requests') }}

)

select
    created_date,
    coalesce(borough, 'UNSPECIFIED')                        as borough,
    agency_code,
    count(*)                                                as requests_opened,
    sum(case when is_closed then 1 else 0 end)              as requests_closed,
    round(
        sum(case when is_closed then 1 else 0 end) * 1.0 / nullif(count(*), 0), 4
    )                                                       as closure_rate,
    round(avg(resolution_hours), 3)                         as avg_resolution_hours,
    round({{ median_of('resolution_hours') }}, 3)            as median_resolution_hours,
    max(resolution_hours)                                   as max_resolution_hours,
    count(distinct complaint_type)                          as distinct_complaint_types
from requests
group by created_date, coalesce(borough, 'UNSPECIFIED'), agency_code
