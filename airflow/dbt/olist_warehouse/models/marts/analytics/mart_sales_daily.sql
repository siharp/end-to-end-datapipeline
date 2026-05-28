{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['sales_date']
) }}

select
    order_purchase_date as sales_date,
    count() as orders_count,
    uniqExact(customer_unique_id) as customers_count,
    sum(item_count) as items_count,
    sum(order_item_value) as revenue,
    sum(order_freight_value) as freight_revenue,
    sum(order_total_value) as gross_revenue,
    sum(payment_value) as payment_value,
    countIf(order_status = 'delivered') as delivered_orders_count,
    countIf(order_status = 'canceled') as canceled_orders_count,
    countIf(is_late_delivery = 1) as late_orders_count,
    avg(order_item_value) as avg_order_value,
    avgIf(actual_delivery_days, actual_delivery_days is not null) as avg_delivery_days,
    avgIf(latest_review_score, latest_review_score is not null) as avg_review_score
from {{ ref('fct_orders') }}
group by order_purchase_date
