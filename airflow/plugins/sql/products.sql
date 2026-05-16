CREATE TABLE IF NOT EXISTS products
(
    product_id                  String,
    product_category_name       String,
    product_name_lenght         Int32,
    product_description_lenght  Float64,
    product_photos_qty          Int32,
    product_weight_g            Float64,
    product_length_cm           Float64,
    product_height_cm           Float64,
    product_width_cm            Float64
)
ENGINE = MergeTree()
ORDER BY product_id;