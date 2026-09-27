{{ config(
    materialized='incremental',
    unique_key='trip_key',
    partition_by={'field': 'pickup_date', 'data_type': 'date'},
    cluster_by=['pickup_zone_id', 'payment_type'],
    incremental_strategy='merge'
) }}

-- Table de faits : grain = un trajet. Cles vers dim_zone et dim_date.

with trips as (

    select * from {{ ref('stg_trips') }}

    {% if is_incremental() %}
      where pickup_date > (select coalesce(max(pickup_date), '1900-01-01') from {{ this }})
    {% endif %}

)

select
    trip_key,
    pickup_date,
    pickup_at,
    dropoff_at,
    pickup_hour,
    is_weekend,
    pickup_zone_id,
    dropoff_zone_id,
    vendor_id,
    payment_type,
    passenger_count,
    trip_distance_miles,
    trip_duration_min,
    avg_speed_mph,
    fare_amount,
    tip_amount,
    tip_rate,
    tolls_amount,
    total_amount,

    -- segmentation metier reutilisee par les agregats
    case
        when trip_distance_miles < 2  then 'short'
        when trip_distance_miles < 10 then 'medium'
        else 'long'
    end as trip_length_bucket

from trips
