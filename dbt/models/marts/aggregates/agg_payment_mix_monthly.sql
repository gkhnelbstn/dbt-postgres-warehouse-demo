-- Monthly trips and revenue per payment type, with tip rate.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['service_type', 'source_month'],
        indexes=[{'columns': ['loaded_at']}],
    )
}}

select
    source_month                                    as month_start,
    service_type,
    payment_type_id,
    source_month,
    count(*)                                        as trips,
    sum(fare_amount)                                as fare_amount,
    sum(tip_amount)                                 as tip_amount,
    sum(total_amount)                               as total_amount,
    sum(tip_amount) / nullif(sum(fare_amount), 0)   as tip_rate,
    max(loaded_at)                                  as loaded_at
from {{ ref('int_trips__analysis_ready') }}
where {{ new_file_months(ref('fct_trips')) }}
group by 1, 2, 3, 4
