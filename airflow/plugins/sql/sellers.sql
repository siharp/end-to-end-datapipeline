CREATE TABLE IF NOT EXISTS sellers
(
    seller_id               String,
    seller_zip_code_prefix  Int64,
    seller_city             String,
    seller_state            String
)
ENGINE = MergeTree()
ORDER BY seller_id;