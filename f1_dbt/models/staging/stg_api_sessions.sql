-- Staging model: sessions from OpenF1 API (season 2024).

select
    session_key,
    session_type,
    session_name,
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
from read_parquet('../data/silver/api_sessions.parquet')
