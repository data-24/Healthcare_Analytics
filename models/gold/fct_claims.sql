with c as (
    select * from {{ ref('stg_claims') }}
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