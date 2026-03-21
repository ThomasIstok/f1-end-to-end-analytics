-- Staging model: historical races from Kaggle/Ergast dataset.

select
    race_id,
    season,
    round,
    race_name,
    date       as race_date,
    time       as race_time,
    circuit_id
from read_parquet('../data/silver/csv_races.parquet')
