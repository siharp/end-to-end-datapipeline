{{ config(materialized = 'view') }}

select
    product_id,
    nullIf(lowerUTF8(trimBoth(ifNull(product_category_name, ''))), '') as product_category_name,
    product_name_lenght as product_name_length,
    product_description_lenght as product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_length_cm * product_height_cm * product_width_cm as product_volume_cm3
from {{ source('olist', 'products') }}
