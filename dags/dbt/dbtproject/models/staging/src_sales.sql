with raw_sales as (
    select
        *
    from
        {{ source('retail_data', 'sales')}}
)
select
    product_id,
    store_id,
    date as sales_date,
    EXTRACT(DAY FROM date ) AS day,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR FROM date) AS year,
    FORMAT_TIMESTAMP('%A', date) AS day_name,
    sales,
    revenue,
    stock,
    price
from
    raw_sales