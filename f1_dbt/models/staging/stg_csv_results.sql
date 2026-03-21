-- Staging model: race results from Kaggle/Ergast dataset.

select
    race_id,
    driver_id,
    constructor_id,
    grid,
    position          as position_text,
    position_numeric,
    position_order,
    points,
    laps,
    status
from read_parquet('../data/silver/csv_results.parquet')
