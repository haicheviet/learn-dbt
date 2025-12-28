
select
    id as raw_id,
    project_name,
    total_area,
    num_rooms,
    direction,
    price,
    description,
    created_at
from {{ ref('raw_vinhome') }}
