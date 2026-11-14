select payment_type_id, payment_type_name
from {{ ref('tlc_payment_types') }}
