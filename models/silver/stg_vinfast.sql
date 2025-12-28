
with source as (
    select * from {{ ref('bronze_vinfast') }}
),

deduplicated as (
    select
        raw_id,
        product_name as title,
        description,
        version,
        color,
        type as vehicle_type,
        base_price as price,
        created_at
    from source
    qualify row_number() over (partition by raw_id order by created_at desc) = 1
)

select
    *,
    'vehicle' as item_type
from deduplicated
where title is not null and price is not null
