-- Fact: one row per claim, linked to dimensions by surrogate key.
-- INCREMENTAL: deterministic surrogate keys, no cross-row logic,
-- so we only process claims new since the last run.
{{
    config(
        materialized='incremental',
        unique_key='claim_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with c as (
    select * from {{ ref('stg_claims') }}
    {% if is_incremental() %}
    where load_dttm > (select max(load_dttm) from {{ this }})
    {% endif %}
)
select
    c.claim_id,
    c.admission_id,
    {{ dbt_utils.generate_surrogate_key(['c.insurance_id']) }} as insurance_sk,
    {{ dbt_utils.generate_surrogate_key(['c.claim_date']) }} as date_sk,
    c.claim_amount,
    c.approved_amount,
    c.unapproved_amount,
    c.claim_status,
    c.approval_ratio,
    c.days_to_settle,
    c.is_approved,
    c.is_pending,
    c.dq_status,
    c.file_name, c.load_dttm
from c