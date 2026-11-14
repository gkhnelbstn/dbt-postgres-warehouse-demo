-- Hourly pickups per borough: the demand curve through the day.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['service_type', 'source_month'],
        indexes=[{'columns': ['service_type', 'source_month']}, {'columns': ['loaded_at']}],
    )
}}

select
    pickup_date,
    pickup_hour,
    service_type,
    coalesce(pickup_borough, 'Unknown')             as pickup_borough,
    source_month,
    count(*)                                        as trips,
    avg(duration_minutes)                           as avg_duration_minutes,
    avg(trip_distance_miles)                        as avg_distance_miles,
    sum(total_amount)                               as total_amount,
    max(loaded_at)                                  as loaded_at
from {{ ref('int_trips__analysis_ready') }}
where {{ new_file_months(ref('fct_trips')) }}
group by 1, 2, 3, 4, 5
