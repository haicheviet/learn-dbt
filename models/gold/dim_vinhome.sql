
select
    raw_id as id,
    title,
    description,
    total_area,
    num_rooms,
    direction,
    price,
    item_type,
    created_at
from {{ ref('stg_vinhome') }}
