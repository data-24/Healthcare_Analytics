with date_spine as (
    select dateadd(day, seq4(), '2023-01-01'::date) as date_day
    from table(generator(rowcount => 1095))   -- ~3 years
)
select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_sk,
    date_day,
    year(date_day)            as year,
    quarter(date_day)         as quarter,
    month(date_day)           as month,
    monthname(date_day)       as month_name,
    day(date_day)             as day_of_month,
    dayofweek(date_day)       as day_of_week,
    dayname(date_day)         as day_name,
    case when dayofweek(date_day) in (0,6) then true else false end as is_weekend
from date_spine