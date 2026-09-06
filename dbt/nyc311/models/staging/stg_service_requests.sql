{{ config(materialized='view') }}

-- Staging: type-cast and standardize. One row in, one row out - no filtering,
-- no dedupe, no business logic, so this model stays a faithful typed mirror of
-- the landing table and any row loss downstream is attributable to a named step.

with source as (

    select * from {{ source('raw', 'service_requests_raw') }}

),

typed as (

    select
        trim(request_id)                                        as request_id,
        {{ safe_cast_timestamp('created_at') }}                 as created_at,
        {{ safe_cast_timestamp('closed_at') }}                  as closed_at,
        upper(trim(agency_code))                                as agency_code,
        nullif(trim(agency_name), '')                           as agency_name,
        nullif(trim(complaint_type), '')                        as complaint_type,
        nullif(trim(descriptor), '')                            as descriptor,
        nullif(trim(location_type), '')                         as location_type,
        nullif(trim(postal_code), '')                           as postal_code,
        nullif(trim(street_address), '')                        as street_address,
        nullif(trim(city), '')                                  as city,
        upper(nullif(trim(borough), ''))                        as borough,
        nullif(trim(status), '')                                as status,
        nullif(trim(resolution_description), '')                as resolution_description,
        {{ safe_cast_timestamp('resolution_updated_at') }}      as resolution_updated_at,
        nullif(trim(community_board), '')                       as community_board,
        upper(nullif(trim(intake_channel), ''))                 as intake_channel,
        {{ safe_cast_float('latitude') }}                       as latitude,
        {{ safe_cast_float('longitude') }}                      as longitude,
        _source_system                                          as source_system,
        cast(_batch_date as date)                               as batch_date,
        _batch_id                                               as batch_id,
        {{ safe_cast_timestamp('_ingested_at') }}               as ingested_at
    from source

)

select * from typed
