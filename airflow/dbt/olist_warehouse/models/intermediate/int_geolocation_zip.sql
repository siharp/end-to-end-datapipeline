{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['geolocation_zip_code_prefix']
) }}

select
    geolocation_zip_code_prefix,
    any(geolocation_city) as geolocation_city,
    any(geolocation_state) as geolocation_state,
    avg(geolocation_lat) as avg_latitude,
    avg(geolocation_lng) as avg_longitude,
    count() as geolocation_records
from {{ ref('stg_geolocation') }}
where geolocation_zip_code_prefix is not null
group by geolocation_zip_code_prefix
