{{ config(materialized = 'view') }}

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    lowerUTF8(trimBoth(customer_city)) as customer_city,
    upper(trimBoth(customer_state)) as customer_state
from {{ source('olist', 'customers') }}
