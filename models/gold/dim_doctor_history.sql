with snap as (
    select * from {{ ref('scd_doctors') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['doctor_id', 'dbt_valid_from']) }} as doctor_history_sk,
    doctor_id,
    doctor_name,
    specialty,
    years_experience,
    seniority_band,
    case
        when row_number() over (partition by doctor_id order by dbt_valid_from) = 1
        then '1900-01-01'::timestamp_ntz
        else dbt_valid_from
    end as valid_from,
    coalesce(dbt_valid_to, '9999-12-31'::timestamp_ntz) as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from snap