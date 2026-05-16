CREATE TABLE IF NOT EXISTS product_category_name_translation
(
    product_category_name         String,
    product_category_name_english String
)
ENGINE = MergeTree()
ORDER BY product_category_name;