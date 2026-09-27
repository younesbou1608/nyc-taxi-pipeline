-- Test singulier : aucun agregat ne doit presenter de revenu negatif.
select
    pickup_date,
    pickup_zone_id,
    revenue
from {{ ref('agg_daily_zone') }}
where revenue < 0
