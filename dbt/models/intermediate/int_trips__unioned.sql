-- Yellow and green trips in one shape, with a stable key and quality flags.
-- Kept as a view: fct_trips reads it month by month, and the month filter is
-- pushed down to the raw partitions.
with trips as (
    select * from {{ ref('stg_tlc__yellow_trips') }}
    union all
    select * from {{ ref('stg_tlc__green_trips') }}
)

select
    -- service (1 digit) | file month yyyymm (6) | row within the file (8).
    -- Deterministic for a given file version; files have well under 10^8 rows.
    (case service_type when 'yellow' then 1 when 'green' then 2 end)::bigint * 100000000000000
        + (extract(year from source_month) * 100 + extract(month from source_month))::bigint
          * 100000000
        + source_row                                            as trip_id,
    trips.*,
    pickup_at::date                                             as pickup_date,
    extract(epoch from dropoff_at - pickup_at) / 60.0           as duration_minutes,
    -- Rows TLC itself would call suspect. Kept, flagged, and excluded by the marts' metrics.
    (
        dropoff_at > pickup_at
        and dropoff_at - pickup_at < interval '1 day'
        and pickup_at >= source_month - interval '1 day'
        and pickup_at < source_month + interval '1 month 1 day'
        and trip_distance_miles >= 0
        and total_amount >= 0
    )                                                           as is_valid
from trips
