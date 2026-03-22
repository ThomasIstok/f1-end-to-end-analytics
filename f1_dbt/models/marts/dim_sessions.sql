-- Dimension: sessions from OpenF1 API.
-- Contains details about each session (Race, Practice, Qualifying).

with sessions as (
    select * from {{ ref('stg_api_sessions') }}
)

select
    session_key,
    session_name,
    session_type,
    date_start,
    date_end,
    meeting_key,
    circuit_key,
    circuit_short_name,
    country_key,
    country_code,
    country_name,
    location,
    year
from sessions
