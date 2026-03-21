-- Drivers dimension - combining historical data (Kaggle) and API data (OpenF1).
-- Contains surrogate key, identifier, name, nationality, and date of birth.

with kaggle_drivers as (
    select * from {{ ref('stg_csv_drivers') }}
),

api_drivers as (
    select distinct
        driver_number,
        full_name,
        first_name   as given_name,
        last_name    as family_name,
        team_name,
        country_code as nationality,
        name_acronym
    from read_parquet('../data/silver/api_drivers.parquet')
)

select
    -- Surrogate key - hash of driver_id for stability
    md5(cast(d.driver_id as varchar)) as driver_key,
    d.driver_id,
    d.given_name,
    d.family_name,
    d.given_name || ' ' || d.family_name as full_name,
    d.nationality,
    d.date_of_birth,
    -- Enrichment from API data (if match on last name exists)
    a.driver_number,
    a.name_acronym,
    a.team_name    as current_team
from kaggle_drivers d
left join api_drivers a
    on lower(d.family_name) = lower(a.family_name)
