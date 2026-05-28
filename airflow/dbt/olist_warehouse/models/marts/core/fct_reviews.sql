{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['review_creation_date_day', 'order_id', 'review_id'],
    partition_by = ['toYYYYMM(review_creation_date_day)']
) }}

select
    r.review_id,
    r.order_id,
    o.customer_id,
    o.customer_unique_id,
    o.customer_state,
    o.customer_region,
    o.order_status,
    o.order_purchase_date,
    o.order_purchase_month,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_creation_date_day,
    r.review_answer_timestamp,
    r.review_answer_date_day,
    r.has_review_comment
from {{ ref('stg_order_reviews') }} as r
left join {{ ref('fct_orders') }} as o
    on r.order_id = o.order_id
