CREATE TABLE IF NOT EXISTS order_items
(
    order_id                String,
    order_item_id           Int64,
    product_id              String,
    seller_id               String,
    shipping_limit_date     DateTime,
    price                   Float64,
    freight_value           Float64
)
ENGINE = MergeTree()
ORDER BY (order_id, order_item_id);