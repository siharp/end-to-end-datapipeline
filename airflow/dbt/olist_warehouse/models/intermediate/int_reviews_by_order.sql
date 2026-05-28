{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['order_id']
) }}

select
    order_id,
    count() as review_count,
    avg(review_score) as avg_review_score,
    argMax(review_id, ifNull(review_answer_timestamp, review_creation_date)) as latest_review_id,
    argMax(review_score, ifNull(review_answer_timestamp, review_creation_date)) as latest_review_score,
    max(review_creation_date) as latest_review_creation_date,
    max(review_answer_timestamp) as latest_review_answer_timestamp,
    max(has_review_comment) as has_review_comment
from {{ ref('stg_order_reviews') }}
group by order_id
