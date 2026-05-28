{{ config(materialized = 'view') }}

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    toDate(shipping_limit_date) as shipping_limit_date_day,
    price,
    freight_value,
    toDecimal64(1, 0) as item_quantity,
    price as item_revenue,
    price + freight_value as item_total_value
from {{ source('olist', 'order_items') }}
