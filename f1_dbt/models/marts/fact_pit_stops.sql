-- Fact table: Pit stops from OpenF1 API.
-- Each row represents one pit stop for a driver during a specific session.

with pit as (
    select * from {{ ref('stg_api_pit') }}
),

api_drivers as (
    select distinct
        driver_number,
        full_name,
        name_acronym,
        team_name
    from read_parquet('../data/silver/api_drivers.parquet')
)

select
    -- Identifiers
    p.session_key,
    p.meeting_key,
    p.driver_number,
    
    -- Context
    p.date as pit_stop_date,
    p.lap_number,
    
    -- Driver enrichment
    ad.full_name as driver_name,
    ad.name_acronym as driver_code,
    ad.team_name,
    
    -- Metrics
    p.pit_duration,
    p.lane_duration
from pit p
left join api_drivers ad
    on p.driver_number = ad.driver_number
