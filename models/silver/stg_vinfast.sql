
with source as (
    select * from {{ ref('vinfast_snapshot') }}
    where dbt_valid_to is null
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
        created_at,
        icon_url
    from source
)

select
    *,
    'vehicle' as item_type
from deduplicated
where title is not null and price is not null
