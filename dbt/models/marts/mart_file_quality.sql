-- One row per loaded TLC file: does the warehouse hold every row of the file,
-- and how many rows fail each validity rule? The reasons overlap; a trip can
-- fail several. `is_valid` in fct_trips is the conjunction of all of them.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['service_type', 'source_month'],
        indexes=[{'columns': ['loaded_at']}],
    )
}}

with per_file as (
    select
        service_type,
        source_month,
        count(*)                                                        as fct_rows,
        count(*) filter (where is_valid)                                as valid_rows,
        count(*) filter (where dropoff_at <= pickup_at)                 as non_positive_duration_rows,
        count(*) filter (where dropoff_at - pickup_at >= interval '1 day') as over_one_day_rows,
        count(*) filter (where pickup_at < source_month - interval '1 day'
                            or pickup_at >= source_month + interval '1 month 1 day')
                                                                        as pickup_outside_file_month_rows,
        count(*) filter (where trip_distance_miles < 0)                 as negative_distance_rows,
        count(*) filter (where total_amount < 0)                        as negative_total_rows,
        count(*) filter (where pickup_zone_id in (264, 265)
                            or dropoff_zone_id in (264, 265))           as unknown_zone_rows,
        max(loaded_at)                                                  as loaded_at
    from {{ ref('fct_trips') }}
    where {{ new_file_months(ref('fct_trips')) }}
    group by 1, 2
),

latest_load as (
    select distinct on (dataset, source_month)
        dataset, source_month, source_rows, loaded_rows, source_bytes, source_version
    from {{ ref('stg_tlc__load_log') }}
    where status = 'succeeded'
    order by dataset, source_month, load_id desc
)

select
    f.service_type,
    f.source_month,
    l.source_rows                                                      as file_rows,
    l.loaded_rows                                                      as raw_rows,
    l.source_bytes                                                     as file_bytes,
    l.source_version                                                   as file_version,
    l.loaded_rows < l.source_rows                                      as is_sample_load,
    f.fct_rows,
    -- Every row the loader put into raw reached the fact table.
    coalesce(f.fct_rows = l.loaded_rows, false)                        as is_reconciled,
    f.valid_rows,
    round(100.0 * f.valid_rows / nullif(f.fct_rows, 0), 2)             as valid_pct,
    f.non_positive_duration_rows,
    f.over_one_day_rows,
    f.pickup_outside_file_month_rows,
    f.negative_distance_rows,
    f.negative_total_rows,
    f.unknown_zone_rows,
    f.loaded_at
from per_file as f
left join latest_load as l
    on l.dataset = f.service_type and l.source_month = f.source_month
