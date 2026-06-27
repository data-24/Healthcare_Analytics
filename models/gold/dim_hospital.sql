with hospitals as (
    select * from {{ source('bronze', 'seed_hospitals') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['hospital_id']) }} as hospital_sk,
    hospital_id,
    hospital_name,
    city
from hospitals