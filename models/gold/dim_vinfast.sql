
select
    raw_id as id,
    title,
    description,
    version,
    color,
    vehicle_type,
    price,
    item_type,
    created_at
from {{ ref('stg_vinfast') }}
