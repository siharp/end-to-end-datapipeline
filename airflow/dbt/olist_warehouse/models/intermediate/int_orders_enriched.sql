{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['order_purchase_date', 'order_id'],
    partition_by = ['toYYYYMM(order_purchase_date)']
) }}

select
    o.order_id as order_id,
    o.customer_id as customer_id,
    o.order_status as order_status,
    o.order_purchase_timestamp as order_purchase_timestamp,
    o.order_purchase_date as order_purchase_date,
    toStartOfMonth(o.order_purchase_date) as order_purchase_month,
    o.order_approved_at as order_approved_at,
    o.order_approved_date as order_approved_date,
    o.order_delivered_carrier_date as order_delivered_carrier_date,
    o.order_delivered_carrier_date_day as order_delivered_carrier_date_day,
    o.order_delivered_customer_date as order_delivered_customer_date,
    o.order_delivered_customer_date_day as order_delivered_customer_date_day,
    o.order_estimated_delivery_date as order_estimated_delivery_date,
    o.order_estimated_delivery_date_day as order_estimated_delivery_date_day,
    o.is_delivered as is_delivered,
    o.is_canceled as is_canceled,
    o.is_unavailable as is_unavailable,

    if(
        isNotNull(o.order_delivered_customer_date),
        dateDiff('day', o.order_purchase_timestamp, o.order_delivered_customer_date),
        null
    ) as actual_delivery_days,

    if(
        isNotNull(o.order_estimated_delivery_date),
        dateDiff('day', o.order_purchase_timestamp, o.order_estimated_delivery_date),
        null
    ) as estimated_delivery_days,

    if(
        isNotNull(o.order_delivered_customer_date)
        and isNotNull(o.order_estimated_delivery_date),
        dateDiff('day', o.order_estimated_delivery_date, o.order_delivered_customer_date),
        null
    ) as delivery_delay_days,

    if(
        isNotNull(o.order_delivered_customer_date)
        and isNotNull(o.order_estimated_delivery_date)
        and o.order_delivered_customer_date > o.order_estimated_delivery_date,
        1,
        0
    ) as is_late_delivery,

    ifNull(i.item_count, 0) as item_count,
    ifNull(i.unique_product_count, 0) as unique_product_count,
    ifNull(i.unique_seller_count, 0) as unique_seller_count,
    ifNull(i.order_item_value, toDecimal64(0, 2)) as order_item_value,
    ifNull(i.order_freight_value, toDecimal64(0, 2)) as order_freight_value,
    ifNull(i.order_total_value, toDecimal64(0, 2)) as order_total_value,

    ifNull(p.payment_count, 0) as payment_count,
    ifNull(p.payment_types, 'unknown') as payment_types,
    ifNull(p.max_payment_installments, 0) as max_payment_installments,
    ifNull(p.payment_value, toDecimal64(0, 2)) as payment_value,
    ifNull(p.credit_card_value, toDecimal64(0, 2)) as credit_card_value,
    ifNull(p.boleto_value, toDecimal64(0, 2)) as boleto_value,
    ifNull(p.voucher_value, toDecimal64(0, 2)) as voucher_value,
    ifNull(p.debit_card_value, toDecimal64(0, 2)) as debit_card_value,

    ifNull(r.review_count, 0) as review_count,
    r.avg_review_score as avg_review_score,
    r.latest_review_id as latest_review_id,
    r.latest_review_score as latest_review_score,
    r.latest_review_creation_date as latest_review_creation_date,
    r.latest_review_answer_timestamp as latest_review_answer_timestamp,
    ifNull(r.has_review_comment, 0) as has_review_comment
from {{ ref('stg_orders') }} as o
left join {{ ref('int_order_items_by_order') }} as i
    on o.order_id = i.order_id
left join {{ ref('int_payments_by_order') }} as p
    on o.order_id = p.order_id
left join {{ ref('int_reviews_by_order') }} as r
    on o.order_id = r.order_id