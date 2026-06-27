with insurers as (
    select * from {{ source('bronze', 'seed_insurers') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['insurance_id']) }} as insurance_sk,
    insurance_id,
    insurer_name
from insurers