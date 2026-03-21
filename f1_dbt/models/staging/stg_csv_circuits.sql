-- Staging model: historical circuits from Kaggle/Ergast dataset.

select
    circuit_id,
    name          as circuit_name,
    lat           as latitude,
    long          as longitude,
    locality,
    country
from read_parquet('../data/silver/csv_circuits.parquet')
