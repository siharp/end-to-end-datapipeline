{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['sales_month', 'payment_type']
) }}

with payment_events as (

    select
        order_purchase_month as sales_month,
        payment_type,
        order_id,
        payment_installments,
        payment_value as payment_amount
    from {{ ref('fct_payments') }}

),

payment_performance as (

    select
        sales_month,
        payment_type,
        count() as payment_rows_count,
        uniqExact(order_id) as orders_count,
        sum(payment_amount) as total_payment_value,
        avg(payment_amount) as avg_payment_value,
        avg(payment_installments) as avg_payment_installments,
        max(payment_installments) as max_payment_installments
    from payment_events
    group by
        sales_month,
        payment_type

)

select
    sales_month,
    payment_type,
    payment_rows_count,
    orders_count,
    total_payment_value as payment_value,
    avg_payment_value,
    avg_payment_installments,
    max_payment_installments
from payment_performance