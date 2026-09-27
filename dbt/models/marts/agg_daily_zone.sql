{{ config(materialized='table') }}

-- Agregat de service : volume et revenu par jour et zone de prise en charge.

select
    f.pickup_date,
    f.pickup_zone_id,
    z.borough,
    z.zone_name,
    count(*)                                as trips,
    sum(f.total_amount)                     as revenue,
    round(avg(f.total_amount), 2)           as avg_ticket,
    round(avg(f.trip_distance_miles), 2)    as avg_distance_miles,
    round(avg(f.trip_duration_min), 2)      as avg_duration_min,
    round(avg(f.tip_rate), 4)               as avg_tip_rate,
    countif(f.is_weekend)                   as weekend_trips

from {{ ref('fct_trips') }} f
left join {{ ref('dim_zone') }} z
       on f.pickup_zone_id = z.zone_id

group by 1, 2, 3, 4
