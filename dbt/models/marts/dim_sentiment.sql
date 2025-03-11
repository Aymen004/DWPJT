{{ config(
    materialized='table',
    schema='marts'
) }}

-- Define the sentiment categories with their respective scores
WITH sentiment_values AS (
    SELECT *
    FROM (VALUES
        ('Positive', 1.0),
        ('Positive', 0.5),
        ('Neutral', 0.0),
        ('Negative', -0.5),
        ('Negative', -1.0)
    ) AS t(sentiment_category, sentiment_score)
)

SELECT
    ROW_NUMBER() OVER (ORDER BY sentiment_category, sentiment_score) AS sentiment_id,
    sentiment_category,
    sentiment_score,
    NOW() AS created_at
FROM sentiment_values