CREATE TABLE IF NOT EXISTS orders
(
    order_id                        String,
    customer_id                     String,
    order_status                    String,
    order_purchase_timestamp        DateTime,
    order_approved_at               Nullable(DateTime),
    order_delivered_carrier_date    Nullable(DateTime),
    order_delivered_customer_date   Nullable(DateTime),
    order_estimated_delivery_date   Date
)
ENGINE = MergeTree()
ORDER BY (order_id, customer_id);