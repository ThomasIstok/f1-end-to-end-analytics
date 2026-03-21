-- Date dimension - generated from the races table.
-- Contains surrogate key and breakdown of date into sub-components
-- for easy filtering and aggregations in BI tools.

with races as (
    select * from {{ ref('stg_csv_races') }}
)

select distinct
    md5(cast(race_date as varchar)) as date_key,
    race_date,
    extract(year from race_date)    as year,
    extract(month from race_date)   as month,
    extract(day from race_date)     as day,
    dayname(race_date)              as day_of_week,
    monthname(race_date)            as month_name,
    extract(quarter from race_date) as quarter,
    season
from races
where race_date is not null
order by race_date
