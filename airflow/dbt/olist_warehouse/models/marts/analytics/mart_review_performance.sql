{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['review_month', 'review_score']
) }}

select
    toStartOfMonth(review_creation_date_day) as review_month,
    review_score,
    count() as reviews_count,
    countIf(has_review_comment = 1) as reviews_with_comment_count,
    uniqExact(order_id) as reviewed_orders_count
from {{ ref('fct_reviews') }}
group by
    review_month,
    review_score
