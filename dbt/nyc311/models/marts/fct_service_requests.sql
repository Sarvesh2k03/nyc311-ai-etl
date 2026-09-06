{{ config(
    materialized='incremental',
    unique_key='request_id',
    incremental_strategy='delete+insert'
) }}

-- Request fact, grain = one service request.
-- Incremental so a daily run only processes the batches that arrived since the
-- last one. delete+insert on request_id means a re-landed request updates in
-- place rather than duplicating, which makes the whole DAG safe to re-run.

with requests as (

    select * from {{ ref('int_service_requests_deduped') }}

    {% if is_incremental() %}
    -- Only reprocess batches at or after the newest one already in the table.
    where batch_date >= (select coalesce(max(batch_date), date '1900-01-01') from {{ this }})
    {% endif %}

),

final as (

    select
        request_id,
        created_at,
        closed_at,
        cast(created_at as date)                            as created_date,
        agency_code,
        complaint_type,
        descriptor,
        location_type,
        borough,
        postal_code,
        city,
        street_address,
        latitude,
        longitude,
        intake_channel,
        community_board,
        status,
        resolution_description,
        resolution_updated_at,

        case when closed_at is not null then true else false end as is_closed,

        -- Real 311 data contains requests stamped closed *before* they were
        -- created (upstream backdating). Flag them rather than dropping them -
        -- the request itself is real and still counts as volume - but null the
        -- derived duration so a handful of negative values cannot drag down
        -- every SLA average downstream.
        case
            when closed_at is not null and closed_at < created_at then true
            else false
        end                                                 as has_invalid_timestamps,

        case
            when closed_at is null then null
            when closed_at < created_at then null
            else round({{ hours_between('created_at', 'closed_at') }}, 3)
        end                                                 as resolution_hours,

        source_system,
        batch_date,
        batch_id,
        ingested_at,
        was_relanded
    from requests

)

select * from final
