{{ config(materialized = 'view') }}

select
    review_id,
    order_id,
    review_score,
    nullIf(trimBoth(ifNull(review_comment_title, '')), '') as review_comment_title,
    nullIf(trimBoth(ifNull(review_comment_message, '')), '') as review_comment_message,
    review_creation_date,
    toDate(review_creation_date) as review_creation_date_day,
    review_answer_timestamp,
    toDate(review_answer_timestamp) as review_answer_date_day,
    if(lengthUTF8(trimBoth(ifNull(review_comment_message, ''))) > 0, 1, 0) as has_review_comment
from {{ source('olist', 'order_reviews') }}
