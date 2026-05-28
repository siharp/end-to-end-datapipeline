{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['product_id']
) }}

select
    p.product_id,
    ifNull(p.product_category_name, 'unknown') as product_category_name,
    ifNull(t.product_category_name_english, ifNull(p.product_category_name, 'unknown')) as product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_volume_cm3,
    multiIf(
        p.product_weight_g is null, 'unknown',
        p.product_weight_g < 500, 'light',
        p.product_weight_g < 5000, 'medium',
        'heavy'
    ) as product_weight_class
from {{ ref('stg_products') }} as p
left join {{ ref('stg_product_category_name_translation') }} as t
    on p.product_category_name = t.product_category_name
