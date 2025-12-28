
select
    raw_id as id,
    title,
    description,
    total_area,
    num_rooms,
    direction,
    price,
    item_type,
    created_at,
    icon_url,
    {{ get_weighted_text({'title': 3, 'description': 1}) }} as vector_text_raw
from {{ ref('stg_vinhome') }}
