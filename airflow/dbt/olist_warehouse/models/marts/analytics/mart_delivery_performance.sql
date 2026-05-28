{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['sales_month', 'customer_state']
) }}

select
    order_purchase_month as sales_month,
    customer_state,
    customer_region,
    count() as orders_count,
    countIf(order_status = 'delivered') as delivered_orders_count,
    countIf(order_status = 'canceled') as canceled_orders_count,
    countIf(is_late_delivery = 1) as late_orders_count,
    avgIf(actual_delivery_days, actual_delivery_days is not null) as avg_delivery_days,
    avgIf(estimated_delivery_days, estimated_delivery_days is not null) as avg_estimated_delivery_days,
    avgIf(delivery_delay_days, delivery_delay_days is not null) as avg_delivery_delay_days,
    {{ safe_divide("countIf(order_status = 'delivered')", "count()") }} as delivery_rate,
    {{ safe_divide("countIf(is_late_delivery = 1)", "countIf(order_status = 'delivered')") }} as late_delivery_r
from {{ ref('fct_orders') }}
group by
    order_purchase_month,
    customer_state,
    customer_region
