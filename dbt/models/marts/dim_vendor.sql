select vendor_id, vendor_name
from {{ ref('tlc_vendors') }}
