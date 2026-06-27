-- Fact: one row per admission, linked to all dimensions by surrogate key
with adm as (
    select * from {{ ref('stg_admissions') }}
)
select
    -- degenerate dimension (the admission's own ID)
    a.admission_id,

    -- foreign keys to dimensions (surrogate keys)
    {{ dbt_utils.generate_surrogate_key(['a.patient_id']) }}  as patient_sk,
    {{ dbt_utils.generate_surrogate_key(['a.doctor_id']) }}   as doctor_sk,
    {{ dbt_utils.generate_surrogate_key(['a.hospital_id']) }} as hospital_sk,
    {{ dbt_utils.generate_surrogate_key(['a.admit_date']) }}  as date_sk,

    -- measures / attributes
    a.admit_date,
    a.discharge_date,
    a.length_of_stay,
    a.admission_type,
    a.department,
    a.diagnosis_code,
    a.is_30day_readmission,
    a.patient_visit_seq,
    a.dq_status,
    a.is_valid,

    -- lineage
    a.file_name, a.load_dttm
from adm a