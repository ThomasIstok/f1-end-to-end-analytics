-- Staging model: laps data from OpenF1 API (Bahrain GP 2024).

select
    session_key,
    meeting_key,
    driver_number,
    lap_number,
    date_start,
    lap_duration,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    i1_speed,
    i2_speed,
    st_speed,
    is_pit_out_lap
from read_parquet('../data/silver/api_laps.parquet')
