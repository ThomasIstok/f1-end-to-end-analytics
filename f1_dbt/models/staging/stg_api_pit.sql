-- Staging model: pit stop data from OpenF1 API.

select
    date,
    session_key,
    meeting_key,
    driver_number,
    lap_number,
    pit_duration,
    lane_duration
from read_parquet('../data/silver/api_pit.parquet')
