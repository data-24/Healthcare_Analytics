-- ════════════════════════════════════════════════════════════════════
-- dim_doctor (GOLD) — doctor dimension with surrogate key
-- ════════════════════════════════════════════════════════════════════
with doctors as (

    select * from {{ source('bronze', 'seed_doctors') }}

),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['doctor_id']) }} as doctor_sk,

        doctor_id,
        doctor_name,
        specialty,
        years_experience,

        case
            when years_experience >= 20 then 'Senior'
            when years_experience >= 10 then 'Mid-level'
            when years_experience >= 1  then 'Junior'
            else 'Unknown'
        end as seniority_band

    from doctors
)

select * from final