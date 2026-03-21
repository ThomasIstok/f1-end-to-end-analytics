-- Staging model: historical drivers from Kaggle/Ergast dataset.
-- Thin wrapper over Silver Parquet file with column renaming.

select
    driver_id,
    "givenName"  as given_name,
    "familyName" as family_name,
    nationality,
    dob          as date_of_birth
from read_parquet('../data/silver/csv_drivers.parquet')
