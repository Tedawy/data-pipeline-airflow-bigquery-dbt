with sales as (
    select
        product_id,
        store_id,
        sales_date,
        day,
        month,
        year,
        day_name,
        sales,
        revenue,
        stock,
        price
    from
        {{ ref('src_sales') }}
),

dim_product as (
    select
        product_id,
        product_length,
        product_depth,
        product_width,
        cluster_id
    from
        {{ ref('dim_products') }}
),

dim_store as (
    select
        store_id,
        storetype_id,
        store_size,
        city_id
    from
        {{ ref('dim_stores') }}
)

select
    s.sales_date,
    s.day,
    s.month,
    s.year,
    s.day_name,
    s.sales,
    s.revenue,
    s.stock,
    s.price,
    p.product_id,
    p.product_length,
    p.product_depth,
    p.product_width,
    p.cluster_id,
    st.storetype_id,
    st.store_size,
    st.city_id
from
    sales s
left join
    dim_product p on s.product_id = p.product_id
left join
    dim_store st on s.store_id = st.store_id