-- The three airport zones in the TLC zone map.
select
    zone_id,
    case zone_id when 1 then 'EWR' when 132 then 'JFK' when 138 then 'LGA' end as airport_code,
    case zone_id
        when 1 then 'Newark Liberty International'
        when 132 then 'John F. Kennedy International'
        when 138 then 'LaGuardia'
    end                                                                       as airport_name,
    zone_name,
    borough
from {{ ref('dim_zone') }}
where zone_id in (1, 132, 138)
