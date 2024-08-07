with src_products as (
    select
        *
    from
       {{ ref('src_products') }}
)
select
    product_id,
    product_length,
    product_depth,
    product_width,
    cluster_id
from
    src_products