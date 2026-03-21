-- Fact table of lap times from OpenF1 API (Bahrain GP 2024).
-- Each row = one lap by one driver including sector times.

with laps as (
    select * from {{ ref('stg_api_laps') }}
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
    l.session_key,
    l.meeting_key,
    l.driver_number,

    -- Enrichment with driver name
    ad.full_name       as driver_name,
    ad.name_acronym    as driver_code,
    ad.team_name,

    -- Facts about the lap
    l.lap_number,
    l.lap_duration,
    l.duration_sector_1,
    l.duration_sector_2,
    l.duration_sector_3,

    -- Speeds
    l.i1_speed          as speed_trap_1,
    l.i2_speed          as speed_trap_2,
    l.st_speed          as speed_trap_finish,

    -- Flags
    l.is_pit_out_lap,

    -- Derived metrics
    l.duration_sector_1 + l.duration_sector_2 + l.duration_sector_3
        as total_sector_time,
    l.lap_duration - (l.duration_sector_1 + l.duration_sector_2 + l.duration_sector_3)
        as time_loss

from laps l
left join api_drivers ad
    on l.driver_number = ad.driver_number
where l.is_pit_out_lap = false
