
select
    raw_id as id,
    title,
    description,
    version,
    color,
    vehicle_type,
    price,
    item_type,
    created_at,
    icon_url,
    {{ get_weighted_text({'title': 3, 'version': 2, 'description': 1}) }} as vector_text_raw
from {{ ref('stg_vinfast') }}
