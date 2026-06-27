{% snapshot scd_doctors %}

{{
    config(
      target_schema='SNAPSHOTS',
      unique_key='doctor_id',
      strategy='check',
      check_cols=['specialty', 'years_experience', 'seniority_band'],
      invalidate_hard_deletes=True
    )
}}

select
    doctor_id,
    doctor_name,
    specialty,
    years_experience,
    seniority_band
from {{ ref('dim_doctor') }}

{% endsnapshot %}