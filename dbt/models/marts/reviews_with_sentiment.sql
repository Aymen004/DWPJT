{{ config(
    materialized='table',
    schema='marts'
) }}

WITH stg_reviews AS (
    SELECT
        review_id,
        bank_name,
        agency_name,
        city,
        location,
        review_text,
        rating,
        review_date,
        language,
        raw_data,
        created_at
    FROM {{ ref('stg_reviews') }}
),

-- Derived sentiment based on rating
-- In a real implementation, we would use NLP models for sentiment analysis
sentiment_classification AS (
    SELECT
        review_id,
        bank_name,
        agency_name,
        city,
        location,
        review_text,
        rating,
        review_date,
        language,
        raw_data,
        created_at,
        CASE
            WHEN rating >= 4 THEN 'Positive'
            WHEN rating <= 2 THEN 'Negative'
            ELSE 'Neutral'
        END AS sentiment_category,
        CASE
            WHEN rating = 5 THEN 1.0
            WHEN rating = 4 THEN 0.5
            WHEN rating = 3 THEN 0.0
            WHEN rating = 2 THEN -0.5
            WHEN rating = 1 THEN -1.0
        END AS sentiment_score
    FROM stg_reviews
)

SELECT
    review_id,
    bank_name,
    agency_name,
    city,
    location,
    review_text,
    rating,
    review_date,
    language,
    sentiment_category,
    sentiment_score,
    raw_data,
    created_at
FROM sentiment_classification