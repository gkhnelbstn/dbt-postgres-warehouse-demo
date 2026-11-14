-- Monthly KPIs per service, rolled up from agg_trips_daily (small, rebuilt each run).
select
    source_month                                    as month_start,
    service_type,
    sum(trips)                                      as trips,
    count(*)                                        as active_days,
    sum(trips)::numeric / count(*)                  as avg_trips_per_day,
    sum(distance_miles) / nullif(sum(trips), 0)     as avg_distance_miles,
    sum(duration_minutes) / nullif(sum(trips), 0)   as avg_duration_minutes,
    sum(total_amount)                               as total_amount,
    sum(total_amount) / nullif(sum(trips), 0)       as avg_total_amount,
    sum(tip_amount) / nullif(sum(fare_amount), 0)   as tip_rate,
    sum(cbd_congestion_fee_amount)                  as cbd_congestion_fee_amount
from {{ ref('agg_trips_daily') }}
group by 1, 2
