
with source as (
    select * from {{ ref('bronze_vinhome') }}
),

deduplicated as (
    select
        raw_id,
        project_name as title,
        description,
        total_area,
        num_rooms,
        direction,
        price,
        created_at
    from source
    qualify row_number() over (partition by raw_id order by created_at desc) = 1
)

select
    *,
    'property' as item_type
from deduplicated
where title is not null and price is not null -- Basic data quality filter
