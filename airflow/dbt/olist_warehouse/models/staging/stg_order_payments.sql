{{ config(materialized = 'view') }}

select
    order_id,
    payment_sequential,
    lowerUTF8(trimBoth(payment_type)) as payment_type,
    payment_installments,
    payment_value
from {{ source('olist', 'order_payments') }}
