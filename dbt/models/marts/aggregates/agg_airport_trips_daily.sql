-- Daily trips to and from the three airports (JFK, LaGuardia, Newark).
-- Zones are matched by TLC zone id; dim_airport holds the mapping.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['service_type', 'source_month'],
        indexes=[{'columns': ['service_type', 'source_month']}, {'columns': ['loaded_at']}],
    )
}}

with airport_trips as (
    select t.*, a.airport_code, 'from_airport' as direction
    from {{ ref('int_trips__analysis_ready') }} as t
    join {{ ref('dim_airport') }} as a on a.zone_id = t.pickup_zone_id
    where {{ new_file_months(ref('fct_trips')) }}

    union all

    select t.*, a.airport_code, 'to_airport' as direction
    from {{ ref('int_trips__analysis_ready') }} as t
    join {{ ref('dim_airport') }} as a on a.zone_id = t.dropoff_zone_id
    where {{ new_file_months(ref('fct_trips')) }}
)

select
    pickup_date,
    service_type,
    airport_code,
    direction,
    source_month,
    count(*)                                        as trips,
    avg(trip_distance_miles)                        as avg_distance_miles,
    avg(duration_minutes)                           as avg_duration_minutes,
    sum(airport_fee_amount)                         as airport_fee_amount,
    sum(total_amount)                               as total_amount,
    max(loaded_at)                                  as loaded_at
from airport_trips
group by 1, 2, 3, 4, 5
