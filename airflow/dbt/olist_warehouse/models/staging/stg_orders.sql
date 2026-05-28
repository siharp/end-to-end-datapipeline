{{ config(materialized = 'view') }}

select
    order_id,
    customer_id,
    lowerUTF8(trimBoth(order_status)) as order_status,
    order_purchase_timestamp,
    toDate(order_purchase_timestamp) as order_purchase_date,
    order_approved_at,
    toDate(order_approved_at) as order_approved_date,
    order_delivered_carrier_date,
    toDate(order_delivered_carrier_date) as order_delivered_carrier_date_day,
    order_delivered_customer_date,
    toDate(order_delivered_customer_date) as order_delivered_customer_date_day,
    order_estimated_delivery_date,
    toDate(order_estimated_delivery_date) as order_estimated_delivery_date_day,
    if(order_status = 'delivered', 1, 0) as is_delivered,
    if(order_status = 'canceled', 1, 0) as is_canceled,
    if(order_status = 'unavailable', 1, 0) as is_unavailable
from {{ source('olist', 'orders') }}
