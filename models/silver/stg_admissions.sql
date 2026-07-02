-- ════════════════════════════════════════════════════════════════════
-- stg_admissions  (SILVER) — INCREMENTAL
-- Why this layer exists: dedup, cross-row window logic, real readmission
-- detection, and row-level data-quality gating.
--
-- SINGLE INGESTION PATH: Snowpipe is retired. The gatekeeper validates
-- every file and loads it into RAW.PATIENT_ADMISSIONS, so this model now
-- reads ONE source table (no more Snowpipe + GK_ union).
--
-- INCREMENTAL STRATEGY (patient-scoped):
-- Readmission detection needs each patient's FULL history (LAG over
-- prior admissions). A naive "new rows only" incremental would miss
-- readmissions whose prior admission is an old row. So on incremental
-- runs we reprocess the COMPLETE history of any patient who received a
-- new/updated admission since the last run. This keeps window functions
-- correct while processing far less than a full rebuild.
-- ════════════════════════════════════════════════════════════════════

{{
    config(
        materialized='incremental',
        unique_key='admission_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

-- Patients touched since the last run (only used on incremental runs).
{% if is_incremental() %}
with affected_patients as (

    select patient_id from {{ source('bronze', 'patient_admissions') }}
    where load_dttm > (select max(load_dttm) from {{ this }})

),

source as (
{% else %}
with source as (
{% endif %}

    select
        admission_id, patient_id, doctor_id, hospital_id,
        admit_date::varchar as admit_date,
        department, admission_type, diagnosis_code, length_of_stay,
        readmission_flag, file_name, upload_dttm, load_dttm,
        'GATEKEEPER' as source_system
    from {{ source('bronze', 'patient_admissions') }}
    {% if is_incremental() %}
    where patient_id in (select patient_id from affected_patients)
    {% endif %}

),

-- ── 1) DEDUPLICATION: keep latest version of each admission_id by load_dttm.
deduplicated as (

    select *,
        row_number() over (
            partition by admission_id
            order by load_dttm desc
        ) as _row_num
    from source

),

-- ── 2) STANDARDIZE + DECODE + basic typing
cleaned as (

    select
        admission_id,
        patient_id,
        upper(trim(doctor_id))    as doctor_id,
        upper(trim(hospital_id))  as hospital_id,
        coalesce(
            try_to_date(admit_date, 'YYYY-MM-DD'),
            try_to_date(admit_date, 'DD/MM/YYYY')
        ) as admit_date,
        length_of_stay,

        case upper(trim(admission_type))
            when 'EMG' then 'Emergency'
            when 'URG' then 'Urgent'
            when 'ELC' then 'Elective'
            else 'Unknown'
        end as admission_type,

        upper(trim(department))     as department,
        upper(trim(diagnosis_code)) as diagnosis_code,
        cast(readmission_flag as boolean) as source_readmission_flag,

        -- lineage carried forward
        file_name, upload_dttm, load_dttm, source_system

    from deduplicated
    where _row_num = 1          -- keep only the latest row per admission

),

-- ── 3) CROSS-ROW WINDOW LOGIC: per-patient admission history
sequenced as (

    select
        *,
        -- derived discharge date
        dateadd(day, length_of_stay, admit_date) as discharge_date,

        -- which visit number is this for the patient? (1st, 2nd, 3rd...)
        row_number() over (
            partition by patient_id order by admit_date, admission_id
        ) as patient_visit_seq,

        -- total visits this patient ever has
        count(*) over (partition by patient_id) as patient_total_visits,

        -- the patient's PREVIOUS discharge date (for readmission calc)
        lag(dateadd(day, length_of_stay, admit_date)) over (
            partition by patient_id order by admit_date, admission_id
        ) as prev_discharge_date

    from cleaned

),

-- ── 4) REAL 30-DAY READMISSION + DATA QUALITY FLAGS
final as (

    select
        admission_id,
        patient_id,
        doctor_id,
        hospital_id,
        admit_date,
        discharge_date,
        length_of_stay,
        admission_type,
        department,
        diagnosis_code,
        source_readmission_flag,
        patient_visit_seq,
        patient_total_visits,

        -- COMPUTED 30-day readmission: was there a prior discharge
        -- within 30 days of this admission? (real clinical definition)
        case
            when prev_discharge_date is not null
             and datediff(day, prev_discharge_date, admit_date) <= 30
             and datediff(day, prev_discharge_date, admit_date) >= 0
            then true else false
        end as is_30day_readmission,

        -- days since the patient's last discharge (null for first visit)
        datediff(day, prev_discharge_date, admit_date) as days_since_last_visit,

        -- ── ROW-LEVEL DATA QUALITY ──
        case
            when datediff(day, prev_discharge_date, admit_date) < 0 then 'OVERLAPPING_ADMISSION'
            when length_of_stay < 0           then 'INVALID_NEGATIVE_LOS'
            when length_of_stay > 365         then 'INVALID_EXCESSIVE_LOS'
            when admit_date > current_date()  then 'INVALID_FUTURE_DATE'
            when doctor_id is null            then 'MISSING_DOCTOR'
            when hospital_id is null          then 'MISSING_HOSPITAL'
            else 'VALID'
        end as dq_status,

        -- single boolean gate downstream models can filter on
        case
            when length_of_stay between 0 and 365
             and admit_date <= current_date()
             and doctor_id is not null
             and hospital_id is not null
            then true else false
        end as is_valid,

        -- lineage
        file_name, upload_dttm, load_dttm, source_system

    from sequenced

)

select * from final