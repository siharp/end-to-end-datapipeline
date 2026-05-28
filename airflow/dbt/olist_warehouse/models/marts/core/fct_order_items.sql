{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['order_purchase_date', 'order_id', 'order_item_id'],
    partition_by = ['toYYYYMM(order_purchase_date)']
) }}

with base as (
    select
        oi.order_id as order_id,
        oi.order_item_id as order_item_id,
        oi.product_id as product_id,
        oi.seller_id as seller_id,
        o.customer_id as customer_id,
        o.order_status as order_status,
        o.order_purchase_timestamp as order_purchase_timestamp,
        o.order_purchase_date as order_purchase_date,
        o.order_purchase_month as order_purchase_month,
        oi.shipping_limit_date as shipping_limit_date,
        oi.shipping_limit_date_day as shipping_limit_date_day,
        oi.price as price,
        oi.freight_value as freight_value,
        oi.item_quantity as item_quantity,
        oi.item_revenue as item_revenue,
        oi.item_total_value as item_total_value,
        p.product_category_name as product_category_name,
        p.product_category_name_english as product_category_name_english,
        p.product_weight_class as product_weight_class,
        s.seller_city as seller_city,
        s.seller_state as seller_state,
        s.seller_region as seller_region,
        o.customer_city as customer_city,
        o.customer_state as customer_state,
        o.customer_region as customer_region,
        o.is_delivered as is_delivered,
        o.is_late_delivery as is_late_delivery,
        o.actual_delivery_days as actual_delivery_days,
        o.delivery_delay_days as delivery_delay_days,
        o.latest_review_score as latest_review_score
    from {{ ref('stg_order_items') }} as oi
    left join {{ ref('fct_orders') }} as o
        on oi.order_id = o.order_id
    left join {{ ref('dim_products') }} as p
        on oi.product_id = p.product_id
    left join {{ ref('dim_sellers') }} as s
        on oi.seller_id = s.seller_id
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_purchase_date,
    order_purchase_month,
    shipping_limit_date,
    shipping_limit_date_day,
    price,
    freight_value,
    item_quantity,
    item_revenue,
    item_total_value,
    product_category_name,
    product_category_name_english,
    product_weight_class,
    seller_city,
    seller_state,
    seller_region,
    customer_city,
    customer_state,
    customer_region,
    is_delivered,
    is_late_delivery,
    actual_delivery_days,
    delivery_delay_days,
    latest_review_score
from base