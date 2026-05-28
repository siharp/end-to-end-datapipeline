{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['customer_unique_id']
) }}

with
    (select max(order_purchase_date) from {{ ref('fct_orders') }}) as max_order_date,

    customer_metrics as (
        select
            customer_unique_id,
            any(customer_state) as customer_state,
            any(customer_region) as customer_region,
            min(order_purchase_date) as first_order_date,
            max(order_purchase_date) as last_order_date,
            dateDiff('day', max(order_purchase_date), max_order_date) as recency_days,
            count() as total_orders,
            sum(order_item_value) as monetary_value,
            avg(order_item_value) as avg_order_value,
            sum(item_count) as total_items,
            avgIf(latest_review_score, latest_review_score is not null) as avg_review_score
        from {{ ref('fct_orders') }}
        where customer_unique_id is not null
        group by customer_unique_id
    )

select
    customer_unique_id,
    customer_state,
    customer_region,
    first_order_date,
    last_order_date,
    recency_days,
    total_orders,
    monetary_value,
    avg_order_value,
    total_items,
    avg_review_score,
    multiIf(
        total_orders >= 5 and monetary_value >= 1000 and recency_days <= 90, 'champion',
        total_orders >= 3 and recency_days <= 180, 'loyal_customer',
        recency_days > 365, 'at_risk',
        total_orders = 1, 'one_time_buyer',
        'regular_customer'
    ) as customer_segment
from customer_metrics
