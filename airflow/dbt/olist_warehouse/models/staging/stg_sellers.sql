{{ config(materialized = 'view') }}

select
    seller_id,
    seller_zip_code_prefix,
    lowerUTF8(trimBoth(seller_city)) as seller_city,
    upper(trimBoth(seller_state)) as seller_state
from {{ source('olist', 'sellers') }}
