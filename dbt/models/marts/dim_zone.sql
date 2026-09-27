{{ config(materialized='table') }}

-- Dimension zone : 265 lignes, jointe depuis la table de faits.

select
    zone_id,
    borough,
    zone_name,
    service_zone,
    borough || ' - ' || zone_name as zone_full_name

from {{ ref('stg_zones') }}
