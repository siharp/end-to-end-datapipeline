CREATE TABLE IF NOT EXISTS order_payments
(
    order_id                String,
    payment_sequential      Int64,
    payment_type            String,
    payment_installments    Int64,
    payment_value           Float64
)
ENGINE = MergeTree()
ORDER BY (order_id, payment_sequential);