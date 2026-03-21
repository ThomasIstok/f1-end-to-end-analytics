-- Staging model: constructors (teams) from Kaggle/Ergast dataset.

select
    constructor_id,
    name         as team_name,
    nationality
from read_parquet('../data/silver/csv_constructors.parquet')
