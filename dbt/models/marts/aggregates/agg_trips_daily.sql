-- Daily KPIs per service: volume, distance, time and revenue.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['service_type', 'source_month'],
        indexes=[{'columns': ['pickup_date']}, {'columns': ['loaded_at']}],
    )
}}

select
    pickup_date,
    service_type,
    source_month,
    count(*)                                        as trips,
    sum(passenger_count)                            as passengers,
    sum(trip_distance_miles)                        as distance_miles,
    sum(duration_minutes)                           as duration_minutes,
    avg(trip_distance_miles)                        as avg_distance_miles,
    avg(duration_minutes)                           as avg_duration_minutes,
    sum(fare_amount)                                as fare_amount,
    sum(tip_amount)                                 as tip_amount,
    sum(tolls_amount)                               as tolls_amount,
    sum(congestion_surcharge_amount)                as congestion_surcharge_amount,
    sum(cbd_congestion_fee_amount)                  as cbd_congestion_fee_amount,
    sum(total_amount)                               as total_amount,
    max(loaded_at)                                  as loaded_at
from {{ ref('int_trips__analysis_ready') }}
where {{ new_file_months(ref('fct_trips')) }}
group by 1, 2, 3
