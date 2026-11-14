select
    zone_id,
    zone_name,
    borough,
    service_zone,
    zone_id in (264, 265) as is_unknown_zone
from {{ ref('stg_tlc__taxi_zones') }}
