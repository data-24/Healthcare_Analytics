with adm as (
    select * from {{ ref('stg_admissions') }}
),
patient_rollup as (
    select
        patient_id,
        count(*)                              as total_admissions,
        min(admit_date)                       as first_admit_date,
        max(admit_date)                       as last_admit_date,
        sum(case when is_30day_readmission then 1 else 0 end) as readmission_count
    from adm
    group by patient_id
)
select
    {{ dbt_utils.generate_surrogate_key(['patient_id']) }} as patient_sk,
    patient_id,
    total_admissions,
    first_admit_date,
    last_admit_date,
    readmission_count,
    case
        when total_admissions >= 4 then 'High-frequency'
        when total_admissions >= 2 then 'Repeat'
        else 'One-time'
    end as patient_segment
from patient_rollup