
select
    id as raw_id,
    product_name,
    version,
    color,
    type,
    base_price,
    description,
    created_at
from {{ ref('raw_vinfast') }}
