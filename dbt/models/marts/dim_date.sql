-- Calendar from the first TLC trip year to well past the loaded range.
-- fct_trips nulls pickup dates outside this range (bad device clocks).
select
    d::date                                   as date_day,
    extract(year from d)::int                 as year,
    extract(quarter from d)::int              as quarter,
    extract(month from d)::int                as month,
    to_char(d, 'Mon')                         as month_name,
    date_trunc('month', d)::date              as month_start,
    extract(isodow from d)::int               as iso_day_of_week,
    to_char(d, 'Dy')                          as day_name,
    extract(isodow from d) in (6, 7)          as is_weekend
from generate_series(date '2009-01-01', date '2030-12-31', interval '1 day') as d
