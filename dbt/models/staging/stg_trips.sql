{{ config(materialized='view') }}

-- Couche staging : renommage homogene, filtres defensifs, aucune agregation.
-- Une ligne = un trajet.

with source as (

    select * from {{ source('raw', 'trips_clean') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'pickup_at', 'dropoff_at', 'pickup_zone_id', 'dropoff_zone_id', 'total_amount'
        ]) }}                                   as trip_key,
        vendor_id,
        pickup_at,
        dropoff_at,
        pickup_date,
        pickup_hour,
        pickup_dow,
        is_weekend,
        passenger_count,
        trip_distance_miles,
        trip_duration_min,
        avg_speed_mph,
        pickup_zone_id,
        pickup_borough,
        pickup_zone_name,
        dropoff_zone_id,
        dropoff_borough,
        dropoff_zone_name,
        payment_type_id,
        payment_type,
        fare_amount,
        tip_amount,
        tip_rate,
        tolls_amount,
        total_amount

    from source
    where total_amount > 0
      and trip_duration_min > 0

)

select * from renamed
