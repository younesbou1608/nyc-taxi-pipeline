-- Coherence metier : le pourboire ne peut pas depasser le montant total.
select trip_key, tip_amount, total_amount
from {{ ref('fct_trips') }}
where tip_amount > total_amount
