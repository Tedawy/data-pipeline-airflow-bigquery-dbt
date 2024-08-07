with raw_stores as (
    select
        *
    from
        {{source ('retail_data', 'stores')}}
)
select
    store_id,
    storetype_id,
    store_size,
    city_id
from
    raw_stores