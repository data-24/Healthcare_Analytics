-- ════════════════════════════════════════════════════════════════════
-- stg_treatments (SILVER)
-- Complex logic: dedup, outcome decode, per-admission cost context,
-- and STATISTICAL outlier detection (cost z-score per procedure).
-- ════════════════════════════════════════════════════════════════════

with source as (

    select * from {{ source('bronze', 'treatment_records') }}

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
        treatment_date,
        cost,

        case upper(trim(outcome))
            when 'S' then 'Success'
            when 'P' then 'Partial'
            when 'F' then 'Failed'
            else 'Unknown'
        end as outcome,

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

        file_name, upload_dttm, load_dttm
    from stats
)

select * from final