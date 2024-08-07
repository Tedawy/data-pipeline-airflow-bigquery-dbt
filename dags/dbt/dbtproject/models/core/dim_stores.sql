with src_stores as (
    select
        *
    from
        {{ ref ('src_stores')}}
)
select
    store_id,
    storetype_id,
    store_size,
    city_id
from src_stores
