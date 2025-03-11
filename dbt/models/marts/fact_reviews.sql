{{ config(
    materialized='table',
    schema='marts'
) }}

WITH reviews_with_topics AS (
    SELECT
        review_id,
        bank_name,
        agency_name,
        city,
        review_text,
        rating,
        review_date,
        language,
        sentiment_category,
        sentiment_score,
        primary_topic,
        topics
    FROM {{ ref('reviews_with_topics') }}
),

-- Get bank ID from dimension table
banks AS (
    SELECT
        bank_id,
        bank_name
    FROM {{ ref('dim_bank') }}
),

-- Get branch ID from dimension table
branches AS (
    SELECT
        branch_id,
        bank_id,
        branch_name
    FROM {{ ref('dim_branch') }}
),

-- Get location ID from dimension table
locations AS (
    SELECT
        location_id,
        city
    FROM {{ ref('dim_location') }}
),

-- Get date ID from dimension table
dates AS (
    SELECT
        date_id,
        full_date
    FROM {{ ref('dim_date') }}
),

-- Get sentiment ID from dimension table
sentiments AS (
    SELECT
        sentiment_id,
        sentiment_category,
        sentiment_score
    FROM {{ ref('dim_sentiment') }}
)

SELECT
    r.review_id,
    b.bank_id,
    br.branch_id,
    l.location_id,
    d.date_id,
    s.sentiment_id,
    r.rating,
    r.review_text,
    r.language,
    r.topics,
    NOW() AS created_at
FROM reviews_with_topics r
JOIN banks b ON r.bank_name = b.bank_name
JOIN branches br ON r.agency_name = br.branch_name AND br.bank_id = b.bank_id
JOIN locations l ON r.city = l.city
JOIN dates d ON r.review_date = d.full_date
JOIN sentiments s ON r.sentiment_category = s.sentiment_category AND 
                    ABS(r.sentiment_score - s.sentiment_score) < 0.001