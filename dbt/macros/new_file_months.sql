{#-
    Incremental filter shared by the trip aggregates.

    Every aggregate keeps `service_type`, `source_month` (the TLC file month)
    and `loaded_at` (the newest load among its input rows). On an incremental
    run this returns a predicate selecting only the file months whose rows in
    `relation` were loaded after the aggregate's newest `loaded_at`; those
    months are then replaced as a whole by delete+insert.

    The month list is queried at compile time and inlined as constants, so the
    planner can use the (service_type, source_month) index on fct_trips.
-#}
{% macro new_file_months(relation) -%}
    {%- if not (execute and is_incremental()) -%}
        true
    {%- else -%}
        {%- set q -%}
            select distinct service_type, source_month::text
            from {{ relation }}
            where loaded_at > (select coalesce(max(loaded_at), '-infinity') from {{ this }})
            order by 1, 2
        {%- endset -%}
        {%- set rows = run_query(q).rows -%}
        {%- if rows | length == 0 -%}
            false
        {%- else -%}
            (service_type, source_month) in (
            {%- for r in rows %}('{{ r[0] }}', date '{{ r[1] }}'){{ ', ' if not loop.last }}{% endfor -%}
            )
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
