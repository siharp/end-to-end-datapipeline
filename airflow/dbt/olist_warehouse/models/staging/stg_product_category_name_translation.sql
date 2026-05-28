{{ config(materialized = 'view') }}

select
    lowerUTF8(trimBoth(product_category_name)) as product_category_name,
    lowerUTF8(trimBoth(product_category_name_english)) as product_category_name_english
from {{ source('olist', 'product_category_name_translation') }}
