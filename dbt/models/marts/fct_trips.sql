{{ config(
    materialized='table',
    partition_by={
        'field': 'year_month',
        'data_type': 'int64',
        'range': {'start': 200901, 'end': 203001, 'interval': 1}
    },
    cluster_by=['pickup_zone_id', 'payment_type']
) }}

-- Table de faits : grain = un trajet. Cles vers dim_zone et dim_date.
--
-- Materialisation `table` (CREATE OR REPLACE TABLE ... AS SELECT) et non `incremental` :
-- le sandbox BigQuery interdit le DML, donc MERGE / INSERT sont impossibles.
-- Reconstruire la table est idempotent par construction ; sur 3 a 6 mois, le cout
-- reste tres en dessous du quota gratuit.

select
    trip_key,
    year_month,
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

from {{ ref('stg_trips') }}
