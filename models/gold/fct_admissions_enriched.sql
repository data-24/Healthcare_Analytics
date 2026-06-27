-- ════════════════════════════════════════════════════════════════════
-- fct_admissions_enriched (GOLD)
-- Each admission joined to the doctor's specialty AS IT WAS on the
-- admit_date (time-accurate, using SCD2 history) — NOT today's specialty.
-- ════════════════════════════════════════════════════════════════════
with adm as (
    select * from {{ ref('fct_admissions') }}
),

-- map admission's surrogate doctor_sk back to the natural doctor_id
adm_with_id as (
    select
        adm.*,
        d.doctor_id
    from adm
    join {{ ref('dim_doctor') }} d
      on adm.doctor_sk = d.doctor_sk
),

-- join to the HISTORICAL doctor version valid on the admit_date
final as (
    select
        a.admission_id,
        a.admit_date,
        a.discharge_date,
        a.length_of_stay,
        a.admission_type,
        a.department,
        a.is_30day_readmission,
        a.doctor_id,

        -- specialty as it was AT THE TIME of admission (time-accurate)
        h.specialty       as specialty_at_admission,
        h.seniority_band  as seniority_at_admission,

        -- for comparison: the doctor's CURRENT specialty
        cur.specialty     as current_specialty,

        -- flag where they differ (specialty changed since the admission)
        case when h.specialty != cur.specialty then true else false end as specialty_changed_since

    from adm_with_id a

    -- historical version valid on admit_date
    join {{ ref('dim_doctor_history') }} h
      on a.doctor_id = h.doctor_id
     and a.admit_date >= h.valid_from
     and a.admit_date <  h.valid_to

    -- current version (for comparison)
    join {{ ref('dim_doctor_history') }} cur
      on a.doctor_id = cur.doctor_id
     and cur.is_current = true
)

select * from final