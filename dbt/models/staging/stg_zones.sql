{{ config(materialized='view') }}

select
    zone_id,
    coalesce(borough, 'Unknown')      as borough,
    coalesce(zone_name, 'Unknown')    as zone_name,
    coalesce(service_zone, 'Unknown') as service_zone

from {{ source('raw', 'taxi_zones') }}
