CREATE TABLE IF NOT EXISTS order_reviews
(
    review_id               String,
    order_id                String,
    review_score            Int64,
    review_comment_title    String,
    review_comment_message  String,
    review_creation_date    Date,
    review_answer_timestamp DateTime
)
ENGINE = MergeTree()
ORDER BY (order_id, review_id);