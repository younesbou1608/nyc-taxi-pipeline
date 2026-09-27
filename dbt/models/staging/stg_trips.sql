{{ config(materialized='view') }}

-- Couche staging : renommage homogene, filtres defensifs, aucune agregation.
-- Une ligne = un trajet.

with source as (

    select * from {{ source('raw', 'trips_clean') }}

),

renamed as (

    select
        -- Pas de cle naturelle dans les donnees TLC : hash des attributs du trajet.
        -- Des doublons exacts existent dans la source -> test unique en warn, pas en error.
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id', 'pickup_at', 'dropoff_at', 'pickup_zone_id', 'dropoff_zone_id',
            'passenger_count', 'trip_distance_miles', 'total_amount'
        ]) }}                                   as trip_key,
        year_month,
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
