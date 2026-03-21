-- Fact table of race results - joined to dimensions via surrogate keys.
-- Each row = one driver in one race.

with results as (
    select * from {{ ref('stg_csv_results') }}
),

races as (
    select * from {{ ref('stg_csv_races') }}
),

dim_drivers as (
    select * from {{ ref('dim_drivers') }}
),

dim_circuits as (
    select * from {{ ref('dim_circuits') }}
),

dim_teams as (
    select * from {{ ref('dim_teams') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
)

select
    -- Surrogate keys for joining dimensions
    dd.driver_key,
    dc.circuit_key,
    dt.team_key,
    ddt.date_key,

    -- Facts about the race
    r.race_id,
    ra.race_name,
    ra.season,
    ra.round,

    -- Facts about the result
    r.grid               as start_position,
    r.position_text      as finish_position,
    r.position_numeric   as finish_position_numeric,
    r.position_order,
    r.points,
    r.laps               as laps_completed,
    r.status,

    -- Derived metrics
    r.grid - r.position_numeric as positions_gained,
    case
        when r.position_numeric = 1 then true
        else false
    end as is_winner,
    case
        when r.position_numeric <= 3 then true
        else false
    end as is_podium

from results r
inner join races ra
    on r.race_id = ra.race_id
left join dim_drivers dd
    on r.driver_id = dd.driver_id
left join dim_circuits dc
    on ra.circuit_id = dc.circuit_id
left join dim_teams dt
    on r.constructor_id = dt.constructor_id
left join dim_date ddt
    on ra.race_date = ddt.race_date
