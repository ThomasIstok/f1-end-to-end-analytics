-- Circuits dimension - historical data from Kaggle dataset.
-- Contains surrogate key, identifier, name, location, and GPS coordinates.

with circuits as (
    select * from {{ ref('stg_csv_circuits') }}
)

select
    md5(cast(circuit_id as varchar)) as circuit_key,
    circuit_id,
    circuit_name,
    locality,
    country,
    latitude,
    longitude
from circuits
