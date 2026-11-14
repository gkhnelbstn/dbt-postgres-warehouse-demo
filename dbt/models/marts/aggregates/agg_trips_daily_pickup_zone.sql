-- Daily pickups and revenue per pickup zone: where demand is, by day.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['service_type', 'source_month'],
        indexes=[
            {'columns': ['service_type', 'source_month']},
            {'columns': ['pickup_date', 'pickup_zone_id']},
            {'columns': ['loaded_at']},
        ],
    )
}}

select
    pickup_date,
    service_type,
    pickup_zone_id,
    source_month,
    count(*)                                        as trips,
    sum(trip_distance_miles)                        as distance_miles,
    avg(duration_minutes)                           as avg_duration_minutes,
    sum(tip_amount)                                 as tip_amount,
    sum(total_amount)                               as total_amount,
    max(loaded_at)                                  as loaded_at
from {{ ref('int_trips__analysis_ready') }}
where {{ new_file_months(ref('fct_trips')) }}
group by 1, 2, 3, 4
