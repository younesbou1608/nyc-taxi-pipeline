{{ config(materialized='table') }}

-- Profil de demande horaire : sert a identifier les heures de pointe.

select
    f.pickup_hour,
    f.is_weekend,
    count(*)                             as trips,
    round(avg(f.avg_speed_mph), 2)       as avg_speed_mph,
    round(avg(f.total_amount), 2)        as avg_ticket,
    round(sum(f.total_amount), 2)        as revenue

from {{ ref('fct_trips') }} f
group by 1, 2
