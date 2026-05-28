{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['order_purchase_date', 'order_id', 'payment_sequential'],
    partition_by = ['toYYYYMM(order_purchase_date)']
) }}

select
    p.order_id,
    p.payment_sequential,
    o.customer_id,
    o.customer_unique_id,
    o.customer_state,
    o.customer_region,
    o.order_status,
    o.order_purchase_date,
    o.order_purchase_month,
    p.payment_type,
    p.payment_installments,
    p.payment_value
from {{ ref('stg_order_payments') }} as p
left join {{ ref('fct_orders') }} as o
    on p.order_id = o.order_id
