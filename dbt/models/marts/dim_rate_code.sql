select rate_code_id, rate_code_name
from {{ ref('tlc_rate_codes') }}
