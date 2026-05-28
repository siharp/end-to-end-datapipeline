{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['seller_state', 'seller_id']
) }}

select
    seller_id,
    any(seller_city) as seller_city,
    any(seller_state) as seller_state,
    any(seller_region) as seller_region,
    count() as item_lines_count,
    uniqExact(order_id) as orders_count,
    uniqExact(product_id) as products_count,
    sum(item_revenue) as revenue,
    sum(freight_value) as freight_revenue,
    sum(item_total_value) as gross_revenue,
    avg(price) as avg_item_price,
    avgIf(actual_delivery_days, actual_delivery_days is not null) as avg_delivery_days,
    countIf(is_late_delivery = 1) as late_item_lines_count,
    avgIf(latest_review_score, latest_review_score is not null) as avg_review_score
from {{ ref('fct_order_items') }}
group by seller_id
