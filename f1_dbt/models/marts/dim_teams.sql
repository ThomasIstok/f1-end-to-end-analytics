-- Teams (constructors) dimension - historical data from Kaggle dataset.
-- Contains surrogate key, identifier, name, and nationality.

with constructors as (
    select * from {{ ref('stg_csv_constructors') }}
)

select
    md5(cast(constructor_id as varchar)) as team_key,
    constructor_id,
    team_name,
    nationality
from constructors
