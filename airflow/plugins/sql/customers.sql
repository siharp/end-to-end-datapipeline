CREATE TABLE IF NOT EXISTS customers
(
    customer_id             String,
    customer_unique_id      String,
    customer_zip_code_prefix Int64,
    customer_city           String,
    customer_state          String
)
ENGINE = MergeTree()
ORDER BY customer_id;