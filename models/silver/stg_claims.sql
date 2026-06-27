-- ════════════════════════════════════════════════════════════════════
-- stg_claims (SILVER)
-- Complex logic: dedup, status decode, settlement-time calc, approval
-- ratio, NULL-safe financial handling, and DQ validation.
-- ════════════════════════════════════════════════════════════════════

with source as (

    select * from {{ source('bronze', 'insurance_claims') }}

),

deduplicated as (
    select *,
        row_number() over (
            partition by claim_id order by load_dttm desc
        ) as _row_num
    from source
),

cleaned as (
    select
        claim_id,
        admission_id,
        upper(trim(insurance_id)) as insurance_id,
        claim_amount,
        approved_amount,

        case upper(trim(claim_status))
            when 'A' then 'Approved'
            when 'R' then 'Rejected'
            when 'P' then 'Pending'
            else 'Unknown'
        end as claim_status,

        claim_date,
        settle_date,
        file_name, upload_dttm, load_dttm
    from deduplicated
    where _row_num = 1
),

final as (
    select
        claim_id,
        admission_id,
        insurance_id,
        claim_amount,
        approved_amount,
        claim_status,
        claim_date,
        settle_date,

        -- settlement time in days (NULL for pending = not yet settled)
        case
            when settle_date is not null
            then datediff(day, claim_date, settle_date)
        end as days_to_settle,

        -- approval ratio (NULL-safe): how much of the claim was approved
        case
            when claim_status = 'Approved' and claim_amount > 0
            then round(approved_amount / claim_amount, 4)
        end as approval_ratio,

        -- amount the insurer did NOT cover (patient/写off exposure)
        case
            when approved_amount is not null
            then claim_amount - approved_amount
        end as unapproved_amount,

        -- fast boolean flags for Gold rollups
        case when claim_status = 'Approved' then true else false end as is_approved,
        case when claim_status = 'Pending'  then true else false end as is_pending,

        -- ── DATA QUALITY ──
        case
            when approved_amount > claim_amount        then 'APPROVED_EXCEEDS_CLAIM'
            when claim_amount <= 0                     then 'INVALID_CLAIM_AMOUNT'
            when settle_date < claim_date              then 'SETTLE_BEFORE_CLAIM'
            when claim_status = 'Approved'
                 and approved_amount is null           then 'APPROVED_BUT_NO_AMOUNT'
            when claim_status = 'Unknown'              then 'INVALID_STATUS'
            else 'VALID'
        end as dq_status,

        file_name, upload_dttm, load_dttm
    from cleaned
)

select * from final