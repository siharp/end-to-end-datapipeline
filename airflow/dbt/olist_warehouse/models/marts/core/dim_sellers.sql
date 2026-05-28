{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['seller_id']
) }}

select
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    multiIf(
        s.seller_state in ('SP', 'RJ', 'MG', 'ES'), 'southeast',
        s.seller_state in ('PR', 'SC', 'RS'), 'south',
        s.seller_state in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA'), 'northeast',
        s.seller_state in ('DF', 'GO', 'MT', 'MS'), 'center_west',
        s.seller_state in ('AC', 'RO', 'AM', 'RR', 'PA', 'AP', 'TO'), 'north',
        'unknown'
    ) as seller_region,
    g.avg_latitude as seller_latitude,
    g.avg_longitude as seller_longitude
from {{ ref('stg_sellers') }} as s
left join {{ ref('int_geolocation_zip') }} as g
    on s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
