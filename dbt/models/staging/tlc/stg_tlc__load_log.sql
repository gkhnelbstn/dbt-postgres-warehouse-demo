select
    load_id,
    dataset,
    period          as source_month,
    source_url,
    source_version,
    source_bytes,
    source_rows,
    loaded_rows,
    loaded_at,
    status,
    started_at,
    finished_at,
    finished_at - started_at as duration,
    error
from {{ source('tlc', 'load_log') }}
