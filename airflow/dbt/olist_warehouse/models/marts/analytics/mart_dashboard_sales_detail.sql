{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['order_purchase_date', 'order_id', 'order_item_id'],
    partition_by = ['toYYYYMM(order_purchase_date)']
) }}

select
    i.order_id,
    i.order_item_id,
    i.order_purchase_timestamp,
    i.order_purchase_date,
    i.order_purchase_month,
    i.order_status,
    i.customer_id,
    i.customer_city,
    i.customer_state,
    i.customer_region,
    i.product_id,
    i.product_category_name,
    i.product_category_name_english,
    i.product_weight_class,
    i.seller_id,
    i.seller_city,
    i.seller_state,
    i.seller_region,
    i.price,
    i.freight_value,
    i.item_revenue,
    i.item_total_value,
    i.shipping_limit_date_day,
    i.is_delivered,
    i.is_late_delivery,
    i.actual_delivery_days,
    i.delivery_delay_days,
    i.latest_review_score,
    o.payment_types,
    o.max_payment_installments,
    o.payment_value
from {{ ref('fct_order_items') }} as i
left join {{ ref('fct_orders') }} as o
    on i.order_id = o.order_id
