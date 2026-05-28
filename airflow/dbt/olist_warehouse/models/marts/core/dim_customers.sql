{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['customer_id']
) }}

select
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    multiIf(
        c.customer_state in ('SP', 'RJ', 'MG', 'ES'), 'southeast',
        c.customer_state in ('PR', 'SC', 'RS'), 'south',
        c.customer_state in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA'), 'northeast',
        c.customer_state in ('DF', 'GO', 'MT', 'MS'), 'center_west',
        c.customer_state in ('AC', 'RO', 'AM', 'RR', 'PA', 'AP', 'TO'), 'north',
        'unknown'
    ) as customer_region,
    g.avg_latitude as customer_latitude,
    g.avg_longitude as customer_longitude
from {{ ref('stg_customers') }} as c
left join {{ ref('int_geolocation_zip') }} as g
    on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
