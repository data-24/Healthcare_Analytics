-- Fact: one row per treatment, linked to dimensions by surrogate key.
-- INCREMENTAL: deterministic surrogate keys, no cross-row logic here
-- (the cost stats are computed upstream in Silver), so we only process
-- treatments new since the last run.
{{
    config(
        materialized='incremental',
        unique_key='treatment_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with t as (
    select * from {{ ref('stg_treatments') }}
    {% if is_incremental() %}
    where load_dttm > (select max(load_dttm) from {{ this }})
    {% endif %}
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