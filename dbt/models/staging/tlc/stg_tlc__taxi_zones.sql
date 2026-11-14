select
    locationid      as zone_id,
    zone            as zone_name,
    borough,
    service_zone
from {{ source('tlc', 'taxi_zones') }}
