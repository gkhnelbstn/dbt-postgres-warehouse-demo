{#-
    One row per taxi trip. Incremental by source file: on each run, only the
    (service, month) files that raw.load_log shows as loaded since the last
    build are processed, and each such month is replaced as a whole
    (delete+insert). A month TLC republishes is therefore picked up and
    replaced, and nothing else is rescanned.

    The month list is read at compile time and inlined as constants, so
    PostgreSQL prunes the raw partitions instead of scanning all of them.
-#}
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['service_type', 'source_month'],
        on_schema_change='append_new_columns',
        indexes=[
            {'columns': ['service_type', 'source_month']},
            {'columns': ['loaded_at']},
            {'columns': ['pickup_at'], 'type': 'brin'},
        ],
    )
}}

{%- set months = [] -%}
{%- if execute and is_incremental() -%}
    {%- set new_loads -%}
        select dataset, period::text
        from {{ source('tlc', 'load_log') }}
        where status = 'succeeded'
          and dataset in ('yellow', 'green')
          and loaded_at > (select coalesce(max(loaded_at), '-infinity') from {{ this }})
        group by 1, 2
        order by 1, 2
    {%- endset -%}
    {%- set months = run_query(new_loads).rows -%}
{%- endif %}

select
    trip_id,
    service_type,
    source_month,
    source_row,
    vendor_id,
    pickup_at,
    dropoff_at,
    case when pickup_date between date '2009-01-01' and date '2030-12-31'
        then pickup_date end                                   as pickup_date,
    pickup_zone_id,
    dropoff_zone_id,
    coalesce(rate_code_id, 99)                                 as rate_code_id,
    payment_type_id,
    trip_type_id,
    passenger_count,
    trip_distance_miles,
    duration_minutes,
    is_store_and_forward,
    is_valid,
    fare_amount,
    extra_amount,
    mta_tax_amount,
    tip_amount,
    tolls_amount,
    improvement_surcharge_amount,
    congestion_surcharge_amount,
    airport_fee_amount,
    cbd_congestion_fee_amount,
    ehail_fee_amount,
    total_amount,
    request_source,
    source_file,
    loaded_at
from {{ ref('int_trips__unioned') }}
{% if is_incremental() %}
where
    {%- if months | length == 0 %} false
    {%- else %} (service_type, source_month) in (
        {%- for m in months %}
        ('{{ m[0] }}', date '{{ m[1] }}'){{ ',' if not loop.last }}
        {%- endfor %}
    )
    {%- endif %}
{% endif %}
