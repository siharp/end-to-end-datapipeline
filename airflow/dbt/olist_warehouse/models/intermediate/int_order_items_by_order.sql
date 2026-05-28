{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['order_id']
) }}

select
    order_id,
    count() as item_count,
    uniqExact(product_id) as unique_product_count,
    uniqExact(seller_id) as unique_seller_count,
    sum(price) as order_item_value,
    sum(freight_value) as order_freight_value,
    sum(item_total_value) as order_total_value,
    min(shipping_limit_date) as first_shipping_limit_date,
    max(shipping_limit_date) as last_shipping_limit_date
from {{ ref('stg_order_items') }}
group by order_id
