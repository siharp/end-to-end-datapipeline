{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['order_id']
) }}

with payments as (
    select
        order_id,
        payment_type,
        payment_installments,
        payment_value as payment_amount
    from {{ ref('stg_order_payments') }}
)

select
    order_id,
    count() as payment_count,
    arrayStringConcat(arraySort(groupUniqArray(payment_type)), ', ') as payment_types,
    max(payment_installments) as max_payment_installments,
    sum(payment_amount) as payment_value,
    sumIf(payment_amount, payment_type = 'credit_card') as credit_card_value,
    sumIf(payment_amount, payment_type = 'boleto') as boleto_value,
    sumIf(payment_amount, payment_type = 'voucher') as voucher_value,
    sumIf(payment_amount, payment_type = 'debit_card') as debit_card_value,
    countIf(payment_type = 'credit_card') as credit_card_payment_count,
    countIf(payment_type = 'boleto') as boleto_payment_count,
    countIf(payment_type = 'voucher') as voucher_payment_count,
    countIf(payment_type = 'debit_card') as debit_card_payment_count
from payments
group by order_id