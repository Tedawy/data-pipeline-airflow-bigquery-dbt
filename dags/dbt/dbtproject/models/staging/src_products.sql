with raw_products as (
    select
        *
    from
        {{source ('retail_data', 'products')}}
)
select
    product_id,
    product_length,
    product_depth,
    product_width,
    cluster_id
from
    raw_products