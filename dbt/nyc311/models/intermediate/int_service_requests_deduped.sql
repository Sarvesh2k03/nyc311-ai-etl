{{ config(materialized='view') }}

-- Intermediate: collapse to one row per request_id.
-- The same request can be re-landed by a backfill or a retried task, and can in
-- principle arrive from more than one upstream system. The winner is the most
-- recently ingested row, which is also the most recently updated one.
-- Columns are listed explicitly rather than using a star-exclude, because that
-- syntax differs between DuckDB (EXCLUDE) and BigQuery (EXCEPT).

with typed as (

    select * from {{ ref('stg_service_requests') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by request_id
            order by ingested_at desc, batch_date desc, source_system
        ) as row_rank,
        count(*) over (partition by request_id) as times_landed
    from typed

)

select
    request_id,
    created_at,
    closed_at,
    agency_code,
    agency_name,
    complaint_type,
    descriptor,
    location_type,
    postal_code,
    street_address,
    city,
    borough,
    status,
    resolution_description,
    resolution_updated_at,
    community_board,
    intake_channel,
    latitude,
    longitude,
    source_system,
    batch_date,
    batch_id,
    ingested_at,
    times_landed > 1 as was_relanded
from ranked
where row_rank = 1
