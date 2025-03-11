{{ config(
    materialized='table',
    schema='marts'
) }}

WITH reviews_with_sentiment AS (
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
        sentiment_score
    FROM {{ ref('reviews_with_sentiment') }}
),

-- In a real implementation, we would use actual NLP topic modeling here
-- This is a simplified example using keyword matching
topic_extraction AS (
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
        CASE
            WHEN review_text ILIKE '%wait%' OR review_text ILIKE '%queue%' OR review_text ILIKE '%long%' THEN 'waiting_times'
            WHEN review_text ILIKE '%staff%' OR review_text ILIKE '%service%' OR review_text ILIKE '%employee%' THEN 'customer_service'
            WHEN review_text ILIKE '%online%' OR review_text ILIKE '%app%' OR review_text ILIKE '%website%' OR review_text ILIKE '%digital%' THEN 'digital_services'
            WHEN review_text ILIKE '%clean%' OR review_text ILIKE '%facility%' OR review_text ILIKE '%building%' OR review_text ILIKE '%branch%' THEN 'facilities'
            WHEN review_text ILIKE '%fee%' OR review_text ILIKE '%cost%' OR review_text ILIKE '%price%' OR review_text ILIKE '%expensive%' THEN 'fees'
            ELSE 'other'
        END AS primary_topic
    FROM reviews_with_sentiment
)

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
    -- Simulate a JSON structure with topics and confidence scores
    -- In a real implementation, this would come from a proper NLP model
    CASE primary_topic
        WHEN 'waiting_times' THEN '{"waiting_times": 0.8, "customer_service": 0.2}'
        WHEN 'customer_service' THEN '{"customer_service": 0.7, "staff_attitude": 0.3}'
        WHEN 'digital_services' THEN '{"digital_services": 0.85, "user_experience": 0.15}'
        WHEN 'facilities' THEN '{"facilities": 0.9, "cleanliness": 0.1}'
        WHEN 'fees' THEN '{"fees": 0.75, "value_for_money": 0.25}'
        ELSE '{"other": 1.0}'
    END::jsonb AS topics
FROM topic_extraction