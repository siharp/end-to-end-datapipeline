{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['product_category_name_english', 'product_id']
) }}

select
    product_id,
    any(product_category_name) as product_category_name,
    any(product_category_name_english) as product_category_name_english,
    any(product_weight_class) as product_weight_class,
    count() as item_lines_count,
    uniqExact(order_id) as orders_count,
    uniqExact(seller_id) as sellers_count,
    sum(item_revenue) as revenue,
    sum(freight_value) as freight_revenue,
    sum(item_total_value) as gross_revenue,
    avg(price) as avg_item_price,
    avgIf(latest_review_score, latest_review_score is not null) as avg_review_score,
    countIf(is_late_delivery = 1) as late_item_lines_count
from {{ ref('fct_order_items') }}
group by product_id
