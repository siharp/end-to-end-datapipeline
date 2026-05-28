{{ config(materialized = 'view') }}

select
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    lowerUTF8(trimBoth(geolocation_city)) as geolocation_city,
    upper(trimBoth(geolocation_state)) as geolocation_state
from {{ source('olist', 'geolocation') }}
