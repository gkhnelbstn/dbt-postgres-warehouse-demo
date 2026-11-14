-- The trips every metric in the aggregates is computed from: valid trips whose
-- pickup falls in the month of the file they came from (all but a few hundred of
-- the valid ones). That makes each pickup date belong to exactly one file, so
-- aggregates can be replaced file month by file month.
-- Pickup and dropoff zones are joined to their borough here once.
select
    t.trip_id,
    t.service_type,
    t.source_month,
    t.pickup_at,
    t.pickup_date,
    extract(hour from t.pickup_at)::int                          as pickup_hour,
    t.pickup_zone_id,
    t.dropoff_zone_id,
    pz.borough                                                   as pickup_borough,
    dz.borough                                                   as dropoff_borough,
    t.payment_type_id,
    t.passenger_count,
    t.trip_distance_miles,
    t.duration_minutes,
    t.fare_amount,
    t.tip_amount,
    t.tolls_amount,
    t.congestion_surcharge_amount,
    t.airport_fee_amount,
    t.cbd_congestion_fee_amount,
    t.total_amount,
    t.loaded_at
from {{ ref('fct_trips') }} as t
left join {{ ref('dim_zone') }} as pz on pz.zone_id = t.pickup_zone_id
left join {{ ref('dim_zone') }} as dz on dz.zone_id = t.dropoff_zone_id
where t.is_valid
  and date_trunc('month', t.pickup_at) = t.source_month
