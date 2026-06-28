{{
    config(
        materialized='incremental',
        unique_key='treatment_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

-- ════════════════════════════════════════════════════════════════════
-- stg_treatments (SILVER)
-- Unions Snowpipe + Gatekeeper treatments. Parses text dates (Bronze
-- stores dates as raw text; Silver converts them - medallion principle).
-- Complex logic: dedup, outcome decode, per-admission cost context,
-- and STATISTICAL outlier detection (cost z-score per procedure).
-- ════════════════════════════════════════════════════════════════════

{% if is_incremental() %}
with affected_keys as (

    select procedure_code, admission_id
    from {{ source('bronze', 'treatment_records') }}
    where load_dttm > (select max(load_dttm) from {{ this }})

    union

    select procedure_code, admission_id
    from {{ source('bronze', 'gk_treatment_records') }}
    where load_dttm > (select max(load_dttm) from {{ this }})

),

snowpipe_treatments as (
{% else %}
with snowpipe_treatments as (
{% endif %}

    select
        treatment_id, admission_id, doctor_id, procedure_code,
        treatment_date::varchar as treatment_date, cost, outcome,
        file_name, upload_dttm, load_dttm,
        'SNOWPIPE' as source_system
    from {{ source('bronze', 'treatment_records') }}
    {% if is_incremental() %}
    where procedure_code in (select procedure_code from affected_keys)
       or admission_id in (select admission_id from affected_keys)
    {% endif %}

),

gatekeeper_treatments as (

    select
        treatment_id, admission_id, doctor_id, procedure_code,
        treatment_date::varchar as treatment_date, cost, outcome,
        file_name, upload_dttm, load_dttm,
        'GATEKEEPER' as source_system
    from {{ source('bronze', 'gk_treatment_records') }}
    {% if is_incremental() %}
    where procedure_code in (select procedure_code from affected_keys)
       or admission_id in (select admission_id from affected_keys)
    {% endif %}

),

source as (

    select * from snowpipe_treatments
    union all
    select * from gatekeeper_treatments

),

-- 1) DEDUP latest version per treatment_id
deduplicated as (
    select *,
        row_number() over (
            partition by treatment_id order by load_dttm desc
        ) as _row_num
    from source
),

cleaned as (
    select
        treatment_id,
        admission_id,
        upper(trim(doctor_id))      as doctor_id,
        upper(trim(procedure_code)) as procedure_code,

        -- parse text date (handles YYYY-MM-DD and DD/MM/YYYY)
        coalesce(
            try_to_date(treatment_date, 'YYYY-MM-DD'),
            try_to_date(treatment_date, 'DD/MM/YYYY')
        ) as treatment_date,

        cost,

        case upper(trim(outcome))
            when 'S' then 'Success'
            when 'P' then 'Partial'
            when 'F' then 'Failed'
            else 'Unknown'
        end as outcome,

        source_system,
        file_name, upload_dttm, load_dttm
    from deduplicated
    where _row_num = 1
),

-- 2) STATISTICAL CONTEXT: cost stats per procedure_code (window aggregates)
stats as (
    select
        *,
        avg(cost)    over (partition by procedure_code) as avg_cost_for_procedure,
        stddev(cost) over (partition by procedure_code) as stddev_cost_for_procedure,

        -- 99th percentile cost per procedure (robust for skewed data)
        percentile_cont(0.99) within group (order by cost)
            over (partition by procedure_code) as p99_cost_for_procedure,

        row_number() over (
            partition by admission_id order by treatment_date, treatment_id
        ) as treatment_seq_in_admission,
        count(*) over (partition by admission_id) as treatments_in_admission
    from cleaned
),

final as (
    select
        treatment_id,
        admission_id,
        doctor_id,
        procedure_code,
        treatment_date,
        cost,
        outcome,
        treatment_seq_in_admission,
        treatments_in_admission,
        round(avg_cost_for_procedure, 2) as avg_cost_for_procedure,

        -- COST Z-SCORE: how many std-devs from the procedure's mean?
        case
            when stddev_cost_for_procedure is null
              or stddev_cost_for_procedure = 0 then 0
            else round((cost - avg_cost_for_procedure)
                       / stddev_cost_for_procedure, 2)
        end as cost_zscore,

        -- flag statistical outliers (cost > 3 std-devs from mean)
        -- OUTLIER: treatment costs above the 99th percentile for its procedure
        -- (IQR/percentile method — robust for right-skewed healthcare costs)
        case
            when cost > p99_cost_for_procedure then true
            else false
        end as is_cost_outlier,

        -- boolean success flag for easy downstream rollups
        case when outcome = 'Success' then true else false end as is_success,

        -- data quality
        case
            when cost < 0                 then 'INVALID_NEGATIVE_COST'
            when treatment_date is null   then 'MISSING_DATE'
            when outcome = 'Unknown'      then 'INVALID_OUTCOME'
            else 'VALID'
        end as dq_status,

        source_system,
        file_name, upload_dttm, load_dttm
    from stats
)

select * from final