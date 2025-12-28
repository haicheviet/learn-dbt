
with source as (
    select * from {{ ref('vinhome_snapshot') }}
    where dbt_valid_to is null -- Get current records only
),

deduplicated as (
    -- Snapshots already handle uniqueness via dbt_scd_id, but here we want latest logical record
    -- Since we filtered dbt_valid_to is null, we have the latest state.
    select
        raw_id,
        project_name as title,
        description,
        total_area,
        num_rooms,
        direction,
        price,
        created_at,
        icon_url
    from source
)

select
    *,
    'property' as item_type
from deduplicated
where title is not null and price is not null -- Basic data quality filter
