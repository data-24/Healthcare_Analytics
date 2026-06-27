with t as (
    select * from {{ ref('stg_treatments') }}
)
select
    t.treatment_id,
    t.admission_id,
    {{ dbt_utils.generate_surrogate_key(['t.doctor_id']) }} as doctor_sk,
    {{ dbt_utils.generate_surrogate_key(['t.treatment_date']) }} as date_sk,

    t.procedure_code,
    t.treatment_date,
    t.cost,
    t.outcome,
    t.is_success,
    t.cost_zscore,
    t.is_cost_outlier,
    t.dq_status,

    t.file_name, t.load_dttm
from t