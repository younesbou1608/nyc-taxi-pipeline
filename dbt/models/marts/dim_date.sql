{{ config(materialized='table') }}

-- Dimension calendaire generee a partir des dates reellement presentes dans les faits.

with bounds as (

    select
        min(pickup_date) as date_from,
        max(pickup_date) as date_to
    from {{ ref('stg_trips') }}

),

spine as (

    select day as date_day
    from bounds,
         unnest(generate_date_array(bounds.date_from, bounds.date_to)) as day

)

select
    date_day,
    extract(year    from date_day)                    as year,
    extract(quarter from date_day)                    as quarter,
    extract(month   from date_day)                    as month,
    format_date('%B', date_day)                       as month_name,
    extract(day     from date_day)                    as day_of_month,
    extract(dayofweek from date_day)                  as day_of_week,
    format_date('%A', date_day)                       as day_name,
    extract(dayofweek from date_day) in (1, 7)        as is_weekend

from spine
