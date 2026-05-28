{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['date_day']
) }}

with
    toDate('2016-01-01') as fallback_date,
    ifNull(
        (select min(order_purchase_date) from {{ ref('stg_orders') }} where order_purchase_date is not null),
        fallback_date
    ) as min_date,
    greatest(
        ifNull(
            (select max(ifNull(order_estimated_delivery_date_day, order_purchase_date)) from {{ ref('stg_orders') }}),
            min_date
        ),
        ifNull(
            (select max(review_creation_date_day) from {{ ref('stg_order_reviews') }}),
            min_date
        )
    ) as max_date,
    toUInt64(greatest(toInt64(dateDiff('day', min_date, max_date) + 1), 1)) as day_count

select
    date_day,
    toYear(date_day) as year,
    toQuarter(date_day) as quarter,
    toMonth(date_day) as month,
    toDayOfMonth(date_day) as day_of_month,
    toDayOfWeek(date_day) as day_of_week,
    toISOWeek(date_day) as iso_week,
    toStartOfMonth(date_day) as month_start_date,
    toStartOfQuarter(date_day) as quarter_start_date,
    toStartOfYear(date_day) as year_start_date,
    formatDateTime(date_day, '%Y-%m') as year_month,
    formatDateTime(date_day, '%W') as day_name,
    if(toDayOfWeek(date_day) in (6, 7), 1, 0) as is_weekend
from (
    select addDays(min_date, toInt64(number)) as date_day
    from numbers(day_count)
)
