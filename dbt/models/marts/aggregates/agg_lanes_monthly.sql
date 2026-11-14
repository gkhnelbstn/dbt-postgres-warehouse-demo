-- Monthly origin-destination lanes (pickup zone -> dropoff zone): volume, distance,
-- time and price per mile per lane. The logistics view of the data.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['service_type', 'source_month'],
        indexes=[
            {'columns': ['service_type', 'source_month']},
            {'columns': ['pickup_zone_id', 'dropoff_zone_id']},
            {'columns': ['loaded_at']},
        ],
    )
}}

select
    source_month                                    as month_start,
    service_type,
    pickup_zone_id,
    dropoff_zone_id,
    source_month,
    count(*)                                        as trips,
    sum(trip_distance_miles)                        as distance_miles,
    avg(trip_distance_miles)                        as avg_distance_miles,
    avg(duration_minutes)                           as avg_duration_minutes,
    sum(fare_amount)                                as fare_amount,
    sum(total_amount)                               as total_amount,
    -- Fare per mile over the lane, not the average of per-trip ratios.
    sum(fare_amount) / nullif(sum(trip_distance_miles), 0)   as fare_per_mile,
    max(loaded_at)                                  as loaded_at
from {{ ref('int_trips__analysis_ready') }}
where {{ new_file_months(ref('fct_trips')) }}
group by 1, 2, 3, 4, 5
